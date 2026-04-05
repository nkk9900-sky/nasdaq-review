import streamlit as st
import traceback

st.set_page_config(page_title="나스닥 선물 복기 대시보드", page_icon="📈", layout="wide")

try:
    import pandas as pd
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import yfinance as yf
    from datetime import datetime, timedelta
    import pytz
    import tempfile
    import os
    from io import BytesIO
    from concurrent.futures import ThreadPoolExecutor
    import database as db
    import kis_api
    import trade_classifier
except Exception as _e:
    st.error("앱 로드 실패 (아래 내용 복사해서 알려주세요)")
    st.code(traceback.format_exc())
    st.stop()

try:
    from database import is_network_unreachable_error as network_unreachable_error
except ImportError:
    import socket

    def network_unreachable_error(e):
        if isinstance(e, socket.gaierror):
            return True
        if isinstance(e, (ConnectionError, TimeoutError, BrokenPipeError)):
            return True
        msg = str(e).lower()
        for needle in (
            "name or service not known",
            "temporary failure in name resolution",
            "failed to resolve",
            "nodename nor servname",
            "getaddrinfo failed",
            "could not resolve host",
        ):
            if needle in msg:
                return True
        err = getattr(e, "errno", None)
        if err is not None and err in (-2, 11001, 11002):
            return True
        return False

KST = pytz.timezone("Asia/Seoul")
CST = pytz.timezone("America/Chicago")


def kst_to_cst(dt_kst: datetime) -> datetime:
    """KST → CST/CDT 변환 (서머타임 자동 반영, naive 반환)."""
    if dt_kst is None:
        return None
    if dt_kst.tzinfo is None:
        dt_kst = KST.localize(dt_kst)
    return dt_kst.astimezone(CST).replace(tzinfo=None)


def cst_to_kst(dt_cst: datetime) -> datetime:
    """CST/CDT → KST 변환 (서머타임 자동 반영, naive 반환)."""
    if dt_cst is None:
        return None
    if dt_cst.tzinfo is None:
        dt_cst = CST.localize(dt_cst)
    return dt_cst.astimezone(KST).replace(tzinfo=None)


def load_trades_by_date(trade_date: str):
    db_trades = db.get_paired_trades_by_date(trade_date)
    trades = []
    for t in db_trades:
        trades.append({
            'entry_time_kst': datetime.fromisoformat(t['entry_time_kst']),
            'entry_time_cst': datetime.fromisoformat(t['entry_time_cst']),
            'exit_time_kst': datetime.fromisoformat(t['exit_time_kst']),
            'exit_time_cst': datetime.fromisoformat(t['exit_time_cst']),
            'entry_price': t['entry_price'],
            'exit_price': t['exit_price'],
            'quantity': t['quantity'],
            'profit': t['profit'],
            'type': t['type'],
            'symbol': t.get('symbol', '')
        })
    return trades

def save_trades_to_db(trades):
    for t in trades:
        symbol = t.get('symbol', 'MNQ')
        if 'MNQ' in symbol:
            symbol = 'MNQ'
        elif 'NQ' in symbol:
            symbol = 'NQ'
        else:
            symbol = 'MNQ'
        trade_data = {
            'entry_time_kst': t['entry_time_kst'],
            'entry_time_cst': t['entry_time_cst'],
            'exit_time_kst': t['exit_time_kst'],
            'exit_time_cst': t['exit_time_cst'],
            'entry_price': t['entry_price'],
            'exit_price': t['exit_price'],
            'quantity': t['quantity'],
            'profit': t['profit'],
            'type': t['type'],
            'symbol': symbol
        }
        db.save_paired_trade(trade_data)

if 'focused_idx' not in st.session_state:
    st.session_state.focused_idx = None
if 'selected_date' not in st.session_state:
    st.session_state.selected_date = None
if 'trades_cache' not in st.session_state:
    st.session_state.trades_cache = {}
if 'candle_cache' not in st.session_state:
    st.session_state.candle_cache = {}
_MAX_CACHE_DATES = 12

def parse_execution_file(file_path):
    df = pd.read_excel(file_path, header=1)
    print(f"[파싱 디버그] 체결내역 컬럼: {df.columns.tolist()}")
    print(f"[파싱 디버그] 체결내역 행 수: {len(df)}")
    if len(df) > 0:
        print(f"[파싱 디버그] 첫번째 행 체결시간: {df.iloc[0].get('체결시간', 'N/A')}")
        print(f"[파싱 디버그] 첫번째 행 체결가: {df.iloc[0].get('체결가', 'N/A')}")
    trades = []
    skipped_rows = []
    for idx, row in df.iterrows():
        try:
            # 컬럼명 호환성: '체결일시' 또는 '체결시간'
            time_str = ''
            if '체결시간' in df.columns:
                time_str = str(row.get('체결시간', '')).strip()
            elif '체결일시' in df.columns:
                time_str = str(row.get('체결일시', '')).strip()
            
            if pd.isna(time_str) or time_str == 'nan' or time_str == '':
                if idx < 3:
                    print(f"[파싱 디버그] 행 {idx}: 시간 비어있음 - {time_str}")
                continue
            if '/' in time_str:
                # 다양한 날짜 형식 지원
                if time_str[2] == '/':  # "26/02/05" 형식 (2자리 년도)
                    if len(time_str) > 14:  # "26/02/05 05:14:58" (초 포함)
                        dt = datetime.strptime(time_str, "%y/%m/%d %H:%M:%S")
                    else:  # "26/01/31 03:00" (초 미포함)
                        dt = datetime.strptime(time_str, "%y/%m/%d %H:%M")
                else:  # "2026/02/05" 형식 (4자리 년도)
                    if len(time_str) > 16:  # "2026/02/05 05:14:58"
                        dt = datetime.strptime(time_str, "%Y/%m/%d %H:%M:%S")
                    else:  # "2026/02/05 03:00"
                        dt = datetime.strptime(time_str, "%Y/%m/%d %H:%M")
            else:
                dt = pd.to_datetime(time_str)
            
            price = float(row['체결가'])
            
            # 컬럼명 호환성: '체결량' 또는 '체결수량'
            if '체결수량' in df.columns:
                quantity = int(row['체결수량'])
            else:
                quantity = int(row['체결량'])
            trade_type = str(row['구분']).strip()
            symbol = str(row.get('종목코드', '')).strip() if '종목코드' in row else ''
            trades.append({
                'datetime_kst': dt,
                'datetime_cst': kst_to_cst(dt),
                'price': price,
                'quantity': quantity,
                'type': trade_type,
                'symbol': symbol
            })
        except Exception as e:
            skipped_rows.append(f"행 {idx+2}: {e}")
            continue
    if skipped_rows:
        st.warning(f"체결내역에서 {len(skipped_rows)}건 파싱 오류:\n" + "\n".join(skipped_rows[:5]))
    return trades

def parse_closing_file(file_path):
    df = pd.read_excel(file_path, header=1)
    closings = []
    skipped_rows = []
    for idx, row in df.iterrows():
        try:
            time_str = str(row.get('청산체결시간', '')).strip()
            if pd.isna(time_str) or time_str == 'nan' or time_str == '':
                continue
            if '/' in time_str:
                dt = datetime.strptime(time_str, "%Y/%m/%d %H:%M:%S")
            else:
                dt = pd.to_datetime(time_str)
            
            trade_date = None
            if '청산일' in df.columns:
                trade_date_val = row['청산일']
                if pd.notna(trade_date_val):
                    trade_date = str(trade_date_val).strip()
            
            if not trade_date:
                trade_date = kst_to_cst(dt).strftime('%Y-%m-%d')
            
            symbol = ''
            if '종목' in df.columns:
                symbol = str(row.get('종목', '')).strip()
            elif '종목코드' in df.columns:
                symbol = str(row.get('종목코드', '')).strip()
            elif '상품명' in df.columns:
                symbol = str(row.get('상품명', '')).strip()
            
            closings.append({
                'closing_time_kst': dt,
                'closing_time_cst': kst_to_cst(dt),
                'entry_price': float(row['매입가격']),
                'exit_price': float(row['청산가격']),
                'quantity': int(row['수량']),
                'profit': float(row['순손익']),
                'type': str(row['구분']).strip(),
                'trade_date': trade_date,
                'symbol': symbol
            })
        except Exception as e:
            skipped_rows.append(f"행 {idx+2}: {e}")
            continue
    if skipped_rows:
        st.warning(f"청산내역에서 {len(skipped_rows)}건 파싱 오류:\n" + "\n".join(skipped_rows[:5]))
    return closings

def match_trades(executions, closings):
    """
    매칭 로직:
    - 매칭 키: [상품명 + 가격] (수량은 제외 - 부분체결 고려)
    - 조건: 체결가격 == 매입가격, 체결시간 < 청산시간
    - 선택: 청산시간과 가장 가까운(최근) 체결 선택
    - 동일 체결은 여러 청산에 재사용 가능 (부분청산 지원)
    """
    matched = []
    debug_info = []
    
    # 각 체결의 남은 수량 추적 (부분체결 지원)
    exec_remaining_qty = {idx: ex['quantity'] for idx, ex in enumerate(executions)}
    
    sorted_closings = sorted(closings, key=lambda x: x['closing_time_kst'])
    sorted_execs = sorted(executions, key=lambda x: x['datetime_kst'])
    
    for closing in sorted_closings:
        best_entry = None
        best_idx = None
        best_time_diff = float('inf')
        
        closing_symbol = closing.get('symbol', '').upper()
        closing_product = ''.join(c for c in closing_symbol if not c.isdigit())[:3]
        
        for idx, ex in enumerate(sorted_execs):
            # 남은 수량이 없으면 스킵
            if exec_remaining_qty.get(idx, 0) <= 0:
                continue
            
            # 1. 상품명 일치 확인
            exec_symbol = ex.get('symbol', '').upper()
            exec_product = ''.join(c for c in exec_symbol if not c.isdigit())[:3]
            
            if closing_product and exec_product:
                if closing_product != exec_product:
                    continue
            
            # 2. 가격 일치 (부동소수점 허용 0.01)
            price_diff = abs(ex['price'] - closing['entry_price'])
            if price_diff > 0.01:
                continue
            
            # 3. 체결시간이 청산시간보다 이전
            if ex['datetime_kst'] >= closing['closing_time_kst']:
                continue
            
            # 4. 청산시간과 가장 가까운 시간 선택
            time_diff = (closing['closing_time_kst'] - ex['datetime_kst']).total_seconds()
            if time_diff < best_time_diff:
                best_entry = ex
                best_idx = idx
                best_time_diff = time_diff
        
        trade_date = closing.get('trade_date', closing['closing_time_cst'].strftime('%Y-%m-%d'))
        
        # 디버그: 매칭 결과 로깅
        if not best_entry:
            debug_info.append(f"매칭실패: closing_symbol={closing_symbol}, entry_price={closing['entry_price']}, closing_time={closing['closing_time_kst']}")
        
        if best_entry:
            # 사용한 수량만큼 차감
            exec_remaining_qty[best_idx] -= closing['quantity']
            symbol = best_entry.get('symbol', closing.get('symbol', ''))
            matched.append({
                'entry_time_kst': best_entry['datetime_kst'],
                'entry_time_cst': best_entry['datetime_cst'],
                'exit_time_kst': closing['closing_time_kst'],
                'exit_time_cst': closing['closing_time_cst'],
                'entry_price': best_entry['price'],
                'exit_price': closing['exit_price'],
                'quantity': closing['quantity'],
                'profit': closing['profit'],
                'type': closing['type'],
                'symbol': symbol,
                'trade_date': trade_date
            })
        else:
            # 매칭 실패 시 청산 데이터만으로 표시 (진입시간 = 청산시간 - 2분)
            entry_kst = closing['closing_time_kst'] - timedelta(minutes=int(2))
            matched.append({
                'entry_time_kst': entry_kst,
                'entry_time_cst': kst_to_cst(entry_kst),
                'exit_time_kst': closing['closing_time_kst'],
                'exit_time_cst': closing['closing_time_cst'],
                'entry_price': closing['entry_price'],
                'exit_price': closing['exit_price'],
                'quantity': closing['quantity'],
                'profit': closing['profit'],
                'type': closing['type'],
                'symbol': closing.get('symbol', ''),
                'trade_date': trade_date
            })
    
    # 디버그 정보 출력
    if debug_info:
        print(f"[매칭 디버그] 실패 {len(debug_info)}건:")
        for info in debug_info[:5]:
            print(f"  {info}")
        
        # 체결내역의 가격 목록 출력
        exec_prices = set(ex['price'] for ex in executions)
        close_prices = set(c['entry_price'] for c in closings)
        print(f"[매칭 디버그] 체결내역 가격 수: {len(exec_prices)}, 청산내역 매입가격 수: {len(close_prices)}")
        matching = exec_prices.intersection(close_prices)
        print(f"[매칭 디버그] 일치하는 가격 수: {len(matching)}")
    
    return matched

def get_candle_data(date_str, symbol="NQ=F", data_source="yahoo", timeframe="1", force_refresh=False):
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        cst = pytz.timezone('America/Chicago')
        
        cache_key = f"{timeframe}m"
        
        # 캐시는 요청한 날짜 1일만 조회 (8일치 조회 시 Supabase 호출 많아져 느려짐)
        if not force_refresh and db.has_cached_candles(date_str, symbol, cache_key):
            cached = db.get_cached_candles(date_str, symbol, cache_key)
            if cached:
                df = pd.DataFrame(cached)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                df = df[~df.index.duplicated(keep='first')]
                df = df.sort_index()
                df.index = df.index.tz_localize(cst)
                df.columns = [c.capitalize() if c != 'volume' else 'Volume' for c in df.columns]
                df.rename(columns={'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close'}, inplace=True)
                return df
        
        if data_source == "kis":
            kis_period_map = {"1": "1", "3": "3", "5": "5", "15": "15"}
            kis_period = kis_period_map.get(timeframe, "1")
            target_date_str = date_str.replace("-", "")
            
            df = kis_api.get_futures_minute_data(period=kis_period, count=1000, target_date=target_date_str)
            if df is not None and not df.empty:
                target_date = date_obj.date()
                df = df[df.index.date == target_date]
                if not df.empty:
                    save_candles_to_cache(df, date_str, symbol, cache_key)
                return df
            else:
                st.warning("한국투자증권 API 데이터 조회 실패. Yahoo Finance로 전환합니다.")
                data_source = "yahoo"
        
        if data_source == "yahoo":
            start_date = (date_obj - timedelta(days=int(1))).strftime("%Y-%m-%d")
            end_date = (date_obj + timedelta(days=int(2))).strftime("%Y-%m-%d")
            
            df = yf.download(symbol, start=start_date, end=end_date, interval="1m", progress=False)
            
            if df.empty and symbol == "NQ=F":
                df = yf.download("^NDX", start=start_date, end=end_date, interval="1m", progress=False)
            
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            if not df.empty and hasattr(df.index, 'tzinfo') and df.index.tzinfo is not None:
                df.index = df.index.tz_convert(cst)
            elif not df.empty:
                df.index = df.index.tz_localize('UTC').tz_convert(cst)
            
            # NQ 세션: 전일 17:00 CST ~ 당일 16:00 CST. 캐시/반환은 이 구간만 사용 (날짜 기준 자르면 세션 앞부분이 빠짐 → 차트 왼쪽 빈 칸 원인)
            if not df.empty:
                session_start = cst.localize(datetime.combine(date_obj.date() - timedelta(days=1), datetime.strptime("17:00", "%H:%M").time()))
                session_end = cst.localize(datetime.combine(date_obj.date(), datetime.strptime("16:00", "%H:%M").time()))
                df_session = df[(df.index >= session_start) & (df.index <= session_end)]
                if not df_session.empty:
                    save_candles_to_cache(df_session, date_str, symbol, cache_key)
                return df_session if not df_session.empty else df
        
        return pd.DataFrame()
    except Exception as e:
        if network_unreachable_error(e):
            st.error("차트 데이터 서버에 연결할 수 없습니다. 인터넷·DNS를 확인해 주세요.")
        else:
            st.error(f"데이터 조회 오류: {e}")
        return pd.DataFrame()

def save_candles_to_cache(df, trade_date, symbol, timeframe):
    try:
        candles = []
        for idx, row in df.iterrows():
            ts = idx.strftime('%Y-%m-%d %H:%M:%S') if hasattr(idx, 'strftime') else str(idx)
            candles.append({
                'timestamp': ts,
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': float(row.get('Volume', 0)) if 'Volume' in row else 0
            })
        if candles:
            count = db.save_candle_data(trade_date, symbol, timeframe, candles)
            return count
    except Exception as e:
        print(f"Cache save error: {e}")
    return 0


def calculate_mae_mfe(trade, df_candles):
    try:
        if df_candles is None or df_candles.empty:
            return None, None

        cst = pytz.timezone('America/Chicago')
        entry_cst = trade['entry_time_cst']
        exit_cst = trade['exit_time_cst']
        entry_price = trade['entry_price']
        trade_type = trade.get('type', '')

        if entry_cst.tzinfo is None:
            entry_cst = cst.localize(entry_cst)
        if exit_cst.tzinfo is None:
            exit_cst = cst.localize(exit_cst)

        df_tz = df_candles.copy()
        if df_tz.index.tz is None:
            df_tz.index = df_tz.index.tz_localize(cst)

        mask = (df_tz.index >= entry_cst) & (df_tz.index <= exit_cst)
        df_between = df_tz[mask]

        if df_between.empty:
            mask2 = (df_tz.index >= entry_cst - timedelta(minutes=5)) & \
                    (df_tz.index <= exit_cst + timedelta(minutes=5))
            df_between = df_tz[mask2]

        if df_between.empty:
            return None, None

        highest = df_between['High'].max()
        lowest = df_between['Low'].min()

        if '매수' in trade_type:
            mae = round(entry_price - lowest, 2)
            mfe = round(highest - entry_price, 2)
        else:
            mae = round(highest - entry_price, 2)
            mfe = round(entry_price - lowest, 2)

        return mae, mfe
    except Exception as e:
        return None, None


st.title("나스닥 선물 복기 대시보드")

try:
    available_dates = db.get_available_dates()
    _total_trades = len(db.get_all_paired_trades())
    _db_init_exc = None
except Exception as e:
    available_dates = []
    _total_trades = 0
    _db_init_exc = e

if _db_init_exc is not None:
    if network_unreachable_error(_db_init_exc):
        st.error(
            "**네트워크/DNS 오류**로 저장된 날짜 목록을 불러오지 못했습니다. "
            "인터넷·VPN·Streamlit Secrets의 `SUPABASE_URL` 주소를 확인한 뒤 새로고침하세요."
        )
        st.caption(str(_db_init_exc))
    else:
        st.error(f"DB 초기화 오류: {_db_init_exc}")

with st.sidebar:
    try:
        st.subheader("DB 연결 상태")
        db._sb()
        if getattr(db, "USE_SUPABASE", False):
            if getattr(db, "SUPABASE_INIT_ERROR", None):
                st.error("Supabase 연결 실패")
                err = getattr(db, "SUPABASE_INIT_ERROR", "") or ""
                st.caption(str(err)[:120] + ("…" if len(err) > 120 else ""))
                st.caption("→ Settings → Secrets: SUPABASE_URL, SUPABASE_KEY 확인 후 재시작")
            else:
                st.success("Supabase 연결됨")
        else:
            st.info("로컬 DB (SQLite)")
    except Exception as e:
        st.warning("DB 상태 확인 중 오류")
        st.caption(str(e)[:100])
    st.divider()
    st.header("파일 업로드")
    
    exec_file = st.file_uploader("체결내역 파일", type=['xlsx', 'xls'], key="exec")
    closing_file = st.file_uploader("청산내역 파일", type=['xlsx', 'xls'], key="closing")
    
    if exec_file and closing_file:
        if st.button("매칭 분석 시작", type="primary"):
            try:
                # 임시 파일 없이 메모리에서 바로 읽기 (배포 환경 Errno 11 방지)
                executions = parse_execution_file(BytesIO(exec_file.getvalue()))
                closings = parse_closing_file(BytesIO(closing_file.getvalue()))
                matched = match_trades(executions, closings)
                
                if matched:
                    save_trades_to_db(matched)
                    st.session_state.focused_idx = None
                    st.success(f"{len(matched)}개 거래 매칭 완료! (SQLite에 영구 저장됨)")
                    st.rerun()
                else:
                    st.warning("매칭된 거래가 없습니다.")
            except Exception as e:
                st.error(f"오류: {e}")
    
    if available_dates:
        st.divider()
        st.info(f"저장된 거래: {_total_trades}건 ({len(available_dates)}일)")
        if st.button("전체 데이터 초기화"):
            db.clear_all_paired_trades()
            st.session_state.focused_idx = None
            st.rerun()
    
    with st.sidebar.expander("Replit 데이터 가져오기"):
        st.caption("Replit에서 받은 trades.db를 올리면 거래(+캔들)가 Supabase로 복사됩니다.")
        db_file = st.file_uploader("trades.db 파일", type=["db"], key="replit_db")
        skip_candles = st.checkbox("거래만 가져오기 (캔들은 제외, 먼저 시도해 보세요)", value=True, key="import_skip_candles")
        if db_file:
            if st.button("지금 가져오기", type="primary", key="do_import"):
                fd, tmp_path = tempfile.mkstemp(suffix=".db")
                try:
                    os.write(fd, db_file.getvalue())
                    os.close(fd)
                    fd = None
                    with st.spinner("가져오는 중… (거래 519건 배치 업로드)" if skip_candles else "가져오는 중… (거래+캔들, 1~2분 걸릴 수 있음)"):
                        try:
                            trades_count, candles_count = db.import_from_sqlite(tmp_path, skip_candles=skip_candles)
                        except TypeError:
                            trades_count, candles_count = db.import_from_sqlite(tmp_path)
                    st.success(f"가져오기 완료: 거래 {trades_count}건" + (f", 캔들 {candles_count}건" if candles_count else ""))
                    st.session_state.focused_idx = None
                    st.rerun()
                except Exception as e:
                    st.error("가져오기 실패 — 아래 내용 복사해서 알려주세요")
                    st.code(traceback.format_exc(), language="text")
                finally:
                    if fd is not None:
                        try:
                            os.close(fd)
                        except Exception:
                            pass
                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass

if available_dates:
    st.sidebar.divider()
    st.sidebar.subheader("날짜 선택")
    date_option = st.sidebar.radio("조회 방식", ["저장된 날짜", "날짜 검색"], horizontal=True)
    
    if date_option == "저장된 날짜":
        selected_date = st.sidebar.selectbox("날짜 선택", available_dates, index=0)
    else:
        search_date = st.sidebar.date_input("날짜 검색", value=datetime.strptime(available_dates[0], "%Y-%m-%d"))
        selected_date = search_date.strftime("%Y-%m-%d")
    
    st.sidebar.divider()
    st.sidebar.subheader("데이터 관리")
    
    with st.sidebar.expander("날짜별 데이터 삭제"):
        delete_date = st.selectbox("삭제할 날짜", available_dates, key="delete_date")
        try:
            trades_count = len(db.get_paired_trades_by_date(delete_date))
        except Exception:
            trades_count = 0
        st.caption(f"해당 날짜 거래: {trades_count}건")
        
        if st.button(f"{delete_date} 데이터 삭제", type="secondary"):
            st.session_state.confirm_delete = delete_date
        
        if 'confirm_delete' in st.session_state and st.session_state.confirm_delete == delete_date:
            st.warning(f"정말 {delete_date}의 {trades_count}건 데이터를 삭제하시겠습니까?")
            col_yes, col_no = st.columns(2)
            with col_yes:
                if st.button("예, 삭제", type="primary"):
                    db.clear_paired_trades_by_date(delete_date)
                    st.session_state.confirm_delete = None
                    st.session_state.focused_idx = None
                    st.success(f"{delete_date} 데이터 삭제 완료!")
                    st.rerun()
            with col_no:
                if st.button("취소"):
                    st.session_state.confirm_delete = None
                    st.rerun()
    
    st.sidebar.divider()
    st.sidebar.subheader("차트 설정")
    
    chart_type = st.sidebar.radio("차트 유형", ["캔들 차트", "틱 차트"], horizontal=True)
    
    st.sidebar.caption("타임프레임")
    timeframe = st.sidebar.radio("타임프레임", ["1분", "3분", "5분", "15분"], index=0, horizontal=True, label_visibility="collapsed")
    
    st.sidebar.caption("데이터 소스")
    data_source = st.sidebar.radio("데이터 소스", ["Yahoo Finance", "한국투자증권"], horizontal=True, label_visibility="collapsed")
    data_source_key = "yahoo" if data_source == "Yahoo Finance" else "kis"
    
    timeframe_period_map = {"1분": "1", "3분": "3", "5분": "5", "15분": "15"}
    timeframe_period = timeframe_period_map.get(timeframe, "1")
    
    st.sidebar.divider()
    st.sidebar.subheader("MACD 설정")
    
    show_macd = st.sidebar.checkbox("MACD 표시", value=True)
    
    macd_col1, macd_col2, macd_col3 = st.sidebar.columns(3)
    with macd_col1:
        macd_fast = st.number_input("단기", value=7, min_value=2, max_value=50, key="macd_fast")
    with macd_col2:
        macd_slow = st.number_input("장기", value=20, min_value=5, max_value=100, key="macd_slow")
    with macd_col3:
        macd_signal = st.number_input("시그널", value=7, min_value=2, max_value=50, key="macd_signal")
    
    st.sidebar.divider()
    st.sidebar.subheader("DMI 설정")
    show_dmi = True
    dmi_period = st.sidebar.number_input("DMI 기간", value=14, min_value=5, max_value=50, key="dmi_period")
    
    st.sidebar.divider()
    st.sidebar.subheader("차트 데이터 저장")
    cache_key = f"{timeframe_period}m"
    try:
        is_cached = db.has_cached_candles(selected_date, "NQ=F", cache_key)
    except Exception:
        is_cached = False
    if is_cached:
        st.sidebar.success(f"✅ {selected_date} {timeframe} 자동저장됨")
    else:
        st.sidebar.info(f"🔄 {selected_date} {timeframe} 조회 시 자동저장")
    st.sidebar.caption("차트가 일부만 보이면: 아래 버튼으로 이 날짜 캔들 캐시를 지운 뒤 다시 조회하세요.")
    if st.sidebar.button("🔄 차트(캔들) 캐시 초기화 — 이 날짜만", key="clear_candle_btn", help="선택한 날짜의 차트 데이터만 삭제. 다음 조회 시 Yahoo에서 다시 받습니다."):
        db.clear_candle_cache(selected_date)
        for k in list(st.session_state.candle_cache.keys()):
            if k[0] == selected_date:
                del st.session_state.candle_cache[k]
        st.sidebar.success(f"{selected_date} 차트 캐시 삭제됨. 새로고침 후 다시 조회하세요.")
        st.rerun()

    trade_date = selected_date
    ck = (trade_date, timeframe_period)
    try:
        if trade_date in st.session_state.trades_cache and ck in st.session_state.candle_cache:
            all_day_trades = st.session_state.trades_cache[trade_date]
            df = st.session_state.candle_cache[ck]
        elif trade_date not in st.session_state.trades_cache and ck not in st.session_state.candle_cache:
            with ThreadPoolExecutor(max_workers=2) as ex:
                ft = ex.submit(load_trades_by_date, trade_date)
                fc = ex.submit(get_candle_data, trade_date, "NQ=F", data_source_key, timeframe_period)
                all_day_trades = ft.result()
                df = fc.result()
            if all_day_trades:
                st.session_state.trades_cache[trade_date] = all_day_trades
                if len(st.session_state.trades_cache) > _MAX_CACHE_DATES:
                    del st.session_state.trades_cache[next(iter(st.session_state.trades_cache))]
            if not df.empty:
                st.session_state.candle_cache[ck] = df
                if len(st.session_state.candle_cache) > _MAX_CACHE_DATES:
                    del st.session_state.candle_cache[next(iter(st.session_state.candle_cache))]
        else:
            if trade_date not in st.session_state.trades_cache:
                all_day_trades = load_trades_by_date(trade_date)
                st.session_state.trades_cache[trade_date] = all_day_trades
                if len(st.session_state.trades_cache) > _MAX_CACHE_DATES:
                    del st.session_state.trades_cache[next(iter(st.session_state.trades_cache))]
            else:
                all_day_trades = st.session_state.trades_cache[trade_date]
            if ck not in st.session_state.candle_cache:
                df = get_candle_data(trade_date, "NQ=F", data_source_key, timeframe_period)
                if not df.empty:
                    st.session_state.candle_cache[ck] = df
                    if len(st.session_state.candle_cache) > _MAX_CACHE_DATES:
                        del st.session_state.candle_cache[next(iter(st.session_state.candle_cache))]
            else:
                df = st.session_state.candle_cache[ck]
    except Exception as e:
        if network_unreachable_error(e):
            st.error("**네트워크/DNS 오류**로 데이터를 불러오지 못했습니다. 인터넷·VPN·Supabase URL을 확인한 뒤 새로고침하세요.")
        else:
            st.error("DB 연결이 일시적으로 불안정합니다. 잠시 후 **새로고침** 해 주세요.")
        st.caption(str(e))
        st.stop()
    
    selected_symbol = "NQ=F"
    
    if not all_day_trades:
        st.warning(f"{selected_date}에 해당하는 거래가 없습니다.")
        st.info(f"저장된 날짜: {', '.join(available_dates)}")
        st.stop()
    
    first_entry_all = min(t['entry_time_cst'] for t in all_day_trades)
    last_exit_all = max(t['exit_time_cst'] for t in all_day_trades)
    trades = all_day_trades
    first_entry = min(t['entry_time_cst'] for t in trades)
    last_exit = max(t['exit_time_cst'] for t in trades)
    
    df_chart_data = None
    classifications = None
    
    if 'checked_trades' not in st.session_state:
        st.session_state.checked_trades = set()
    
    if not df.empty:
        cst = pytz.timezone('America/Chicago')
        
        df_filtered = df.copy()
        if df_filtered.index.tz is None:
            df_filtered.index = df_filtered.index.tz_localize(cst)
        
        first_entry_tz = first_entry if first_entry.tzinfo else cst.localize(first_entry)
        last_exit_tz = last_exit if last_exit.tzinfo else cst.localize(last_exit)
        
        trade_date_obj = datetime.strptime(trade_date, "%Y-%m-%d").date()
        chart_start = cst.localize(datetime.combine(trade_date_obj - timedelta(days=1), datetime.strptime("17:00", "%H:%M").time()))
        chart_end = cst.localize(datetime.combine(trade_date_obj, datetime.strptime("16:00", "%H:%M").time()))
        
        df_filtered = df_filtered[(df_filtered.index >= chart_start) & (df_filtered.index <= chart_end)]
        
        if len(df_filtered) == 0:
            df_filtered = df.copy()
            if df_filtered.index.tz is None:
                df_filtered.index = df_filtered.index.tz_localize(cst)
        
        timeframe_map = {"1분": "1min", "3분": "3min", "5분": "5min", "15분": "15min"}
        tf = timeframe_map.get(timeframe, "3min")
        
        if tf != "1min":
            df_resampled = df_filtered.resample(tf, origin='start').agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
                'Volume': 'sum'
            }).dropna()
            df_chart_data = df_resampled
        else:
            df_chart_data = df_filtered
        
        classifications = trade_classifier.classify_all_trades(
            trades, df_filtered, macd_fast, macd_slow, macd_signal, dmi_period
        )
        
        stats = trade_classifier.get_statistics(trades, classifications)
        
        stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
        
        total_profit = sum(t['profit'] for t in trades)
        
        with stat_col1:
            st.metric("원칙 준수율", f"{stats['principle_rate']:.1f}%")
        with stat_col2:
            st.metric("원칙매매 승률", f"{stats['principle_win_rate']:.1f}%")
        with stat_col3:
            st.metric("뇌동매매 손실", f"${abs(stats['impulse_loss']):.2f}")
        with stat_col4:
            st.metric("총 손익", f"${total_profit:.2f}")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader(f"매매 분석 차트 - {trade_date}")
        st.caption(f"⏰ 차트 범위: KST 08:00 ~ CST 16:00 (정산일 기준) | 빨강=수익 | 검정=손실")
        
        if not df.empty and df_chart_data is not None:
            cst = pytz.timezone('America/Chicago')
        
            trade_date_obj = datetime.strptime(trade_date, "%Y-%m-%d").date()
            chart_start = cst.localize(datetime.combine(trade_date_obj - timedelta(days=1), datetime.strptime("17:00", "%H:%M").time()))
            chart_end = cst.localize(datetime.combine(trade_date_obj, datetime.strptime("16:00", "%H:%M").time()))
        
            num_subplots = 1 + (1 if show_macd else 0) + (1 if show_dmi else 0)
        
            if num_subplots > 1:
                if show_macd and show_dmi:
                    row_heights = [0.64, 0.18, 0.18]
                    subplot_titles = ('', 'MACD', 'DMI')
                elif show_macd:
                    row_heights = [0.75, 0.25]
                    subplot_titles = ('', 'MACD')
                else:
                    row_heights = [0.75, 0.25]
                    subplot_titles = ('', 'DMI')
            
                fig = make_subplots(
                    rows=num_subplots, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.02,
                    row_heights=row_heights,
                    subplot_titles=subplot_titles
                )
            else:
                fig = go.Figure()
        
            has_subplots = num_subplots > 1
        
            fig.add_trace(go.Candlestick(
                x=df_chart_data.index,
                open=df_chart_data['Open'],
                high=df_chart_data['High'],
                low=df_chart_data['Low'],
                close=df_chart_data['Close'],
                name=f'캔들({timeframe})',
                increasing_line_color='#FF0000',
                increasing_fillcolor='#FFFFFF',
                increasing_line_width=0.8,
                decreasing_line_color='#0000FF',
                decreasing_fillcolor='#FFFFFF',
                decreasing_line_width=0.8,
                line_width=0.8,
                hoverinfo='skip'
            ), row=1, col=1 if has_subplots else None)
        
            ma_settings = [
                {'period': 5, 'color': '#FF6600', 'width': 1.2},
                {'period': 10, 'color': '#0080FF', 'width': 1},
                {'period': 20, 'color': '#FFD700', 'width': 1},
                {'period': 33, 'color': '#FF0000', 'width': 3.0},
                {'period': 60, 'color': '#00FF00', 'width': 1},
                {'period': 120, 'color': '#808080', 'width': 1},
                {'period': 200, 'color': '#00008B', 'width': 2.5},
            ]
        
            df_full = df.copy()
            if df_full.index.tz is None:
                df_full.index = df_full.index.tz_localize(cst)
        
            for ma_set in ma_settings:
                ma_full = df_full['Close'].rolling(window=ma_set['period']).mean()
                ma_filtered = ma_full[(ma_full.index >= chart_start) & (ma_full.index <= chart_end)]
                fig.add_trace(go.Scatter(
                    x=ma_filtered.index,
                    y=ma_filtered,
                    mode='lines',
                    name=f"MA{ma_set['period']}",
                    line=dict(color=ma_set['color'], width=ma_set['width']),
                    hoverinfo='skip'
                ), row=1, col=1 if has_subplots else None)
        
            if show_macd:
                ema_fast = df_chart_data['Close'].ewm(span=macd_fast, adjust=False).mean()
                ema_slow = df_chart_data['Close'].ewm(span=macd_slow, adjust=False).mean()
                macd_line = ema_fast - ema_slow
                signal_line = macd_line.ewm(span=macd_signal, adjust=False).mean()
            
                fig.add_trace(go.Scatter(
                    x=df_chart_data.index,
                    y=macd_line,
                    mode='lines',
                    name=f'MACD({macd_fast},{macd_slow})',
                    line=dict(color='#FF0000', width=1.5),
                    hoverinfo='skip'
                ), row=2, col=1)
            
                fig.add_trace(go.Scatter(
                    x=df_chart_data.index,
                    y=signal_line,
                    mode='lines',
                    name=f'Signal({macd_signal})',
                    line=dict(color='#000000', width=1.5),
                    hoverinfo='skip'
                ), row=2, col=1)
        
            if show_dmi:
                dmi_row = 3 if show_macd else 2
            
                high = df_chart_data['High']
                low = df_chart_data['Low']
                close = df_chart_data['Close']
            
                plus_dm = high - high.shift(1)
                minus_dm = low.shift(1) - low
            
                plus_dm[plus_dm < 0] = 0
                minus_dm[minus_dm < 0] = 0
            
                plus_dm[(plus_dm > 0) & (plus_dm <= minus_dm)] = 0
                minus_dm[(minus_dm > 0) & (minus_dm <= plus_dm)] = 0
            
                tr1 = high - low
                tr2 = (high - close.shift(1)).abs()
                tr3 = (low - close.shift(1)).abs()
                tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            
                atr = tr.rolling(window=dmi_period).mean()
            
                plus_di = 100 * (plus_dm.rolling(window=dmi_period).mean() / atr)
                minus_di = 100 * (minus_dm.rolling(window=dmi_period).mean() / atr)
            
                fig.add_trace(go.Scatter(
                    x=df_chart_data.index,
                    y=plus_di,
                    mode='lines',
                    name=f'+DI({dmi_period})',
                    line=dict(color='#FF0000', width=1.5),
                    hoverinfo='skip'
                ), row=dmi_row, col=1)
            
                fig.add_trace(go.Scatter(
                    x=df_chart_data.index,
                    y=minus_di,
                    mode='lines',
                    name=f'-DI({dmi_period})',
                    line=dict(color='#0000FF', width=1.5),
                    hoverinfo='skip'
                ), row=dmi_row, col=1)
            
                for ref_val in [10, 20, 40]:
                    fig.add_hline(y=ref_val, line_dash="dash", line_color="gray", 
                                  line_width=0.5, row=dmi_row, col=1)
        
            for idx, t in enumerate(trades):
                entry_cst = t['entry_time_cst']
                exit_cst = t['exit_time_cst']
                entry_price = t['entry_price']
                exit_price = t['exit_price']
                profit = t['profit']
                qty = t['quantity']
            
                color = '#FF0000' if profit > 0 else '#000000'
                is_focused = (st.session_state.focused_idx == idx) or (idx in st.session_state.checked_trades)
            
                try:
                    if entry_cst.tzinfo is None:
                        entry_cst = cst.localize(entry_cst)
                    if exit_cst.tzinfo is None:
                        exit_cst = cst.localize(exit_cst)
                except:
                    pass
                # Plotly에서 캔들 X축과 동일한 타입으로 맞춤 (1시간 어긋남 방지)
                entry_ts = pd.Timestamp(entry_cst) if not isinstance(entry_cst, pd.Timestamp) else entry_cst
                exit_ts = pd.Timestamp(exit_cst) if not isinstance(exit_cst, pd.Timestamp) else exit_cst
            
                fig.add_trace(go.Scatter(
                    x=[entry_ts, exit_ts],
                    y=[entry_price, exit_price],
                    mode='markers+text',
                    marker=dict(size=8, symbol='circle', color=color),
                    text=[f'{qty}', f'{profit:.1f}'],
                    textposition=['top center', 'bottom center'],
                    textfont=dict(size=9, color=color),
                    showlegend=False,
                    hoverinfo='skip'
                ), row=1, col=1 if has_subplots else None)
            
                if is_focused:
                    min_price = min(entry_price, exit_price) - 2
                    max_price = max(entry_price, exit_price) + 2
                    shape_kwargs = dict(
                        type="rect",
                        x0=entry_ts - timedelta(seconds=30),
                        x1=exit_ts + timedelta(seconds=30),
                        y0=min_price,
                        y1=max_price,
                        line=dict(color="#FFD700", width=3),
                        fillcolor="rgba(255, 215, 0, 0.3)",
                    )
                    if has_subplots:
                        shape_kwargs['xref'] = 'x'
                        shape_kwargs['yref'] = 'y'
                    fig.add_shape(**shape_kwargs)
            
                fig.add_annotation(
                    x=exit_ts,
                    y=exit_price,
                    ax=entry_ts,
                    ay=entry_price,
                    xref='x',
                    yref='y',
                    axref='x',
                    ayref='y',
                    showarrow=True,
                    arrowhead=2,
                    arrowsize=1.5,
                    arrowwidth=2,
                    arrowcolor=color,
                    text='',
                )
        
            # x축: 유효한 캔들만 있는 구간. Plotly에서 1시간 어긋남 방지를 위해 range/tickvals는 캔들·화살표와 동일하게 timezone-aware CST 유지.
            if df_chart_data is not None and not df_chart_data.empty:
                valid = df_chart_data.dropna(subset=['Open', 'Close'], how='all')
                if not valid.empty:
                    data_min = valid.index.min()
                    data_max = valid.index.max()
                else:
                    data_min = df_chart_data.index.min()
                    data_max = df_chart_data.index.max()
                x_range = [data_min, data_max]
            else:
                x_range = [chart_start, chart_end]
        
            # tickvals를 캔들 인덱스와 동일한 타임존(CST)으로 생성 (naive로 바꾸면 브라우저가 로컬(KST)로 해석해 어긋남)
            cst_times = pd.date_range(start=x_range[0], end=x_range[1], freq='15min')
            if cst_times.tz is None and (hasattr(x_range[0], 'tzinfo') and x_range[0].tzinfo):
                cst_times = cst_times.tz_localize(cst)
            kst_labels = [cst_to_kst(t.to_pydatetime() if hasattr(t, 'to_pydatetime') else t).strftime('%H:%M') for t in cst_times]
            tick_text = [f"{t.strftime('%H:%M')}<br><span style='color:#888;font-size:10px'>KST {kst}</span>" for t, kst in zip(cst_times, kst_labels)]
        
            if show_macd and show_dmi:
                layout_height = 1230
            elif show_macd or show_dmi:
                layout_height = 1000
            else:
                layout_height = 800
        
            fig.update_layout(
                title=f"나스닥 선물 매매 분석 - {trade_date}",
                height=layout_height,
                template="plotly_dark",
                showlegend=True,
                legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center"),
                dragmode='pan',
                hovermode='x',
                spikedistance=1000,
                hoverdistance=100,
                hoverlabel=dict(bgcolor='rgba(0,0,0,0)', font_size=1, namelength=0),
                margin=dict(l=60, r=80, t=60, b=50)
            )
        
            fig.update_xaxes(
                range=x_range, 
                autorange=False, 
                fixedrange=False,
                tickmode='array',
                tickvals=cst_times,
                ticktext=tick_text,
                tickangle=0,
                rangeslider_visible=False,
                showticklabels=True,
                row=1, col=1
            )
        
            fig.update_yaxes(
                autorange=True, 
                fixedrange=True,
                side='right',
                dtick=10,
                tickformat='.2f',
                gridcolor='rgba(128,128,128,0.3)',
                gridwidth=0.5,
                row=1, col=1
            )
        
            if show_macd:
                fig.update_yaxes(
                    autorange=True,
                    fixedrange=True,
                    side='right',
                    gridcolor='rgba(128,128,128,0.3)',
                    gridwidth=0.5,
                    row=2, col=1
                )
                fig.update_xaxes(
                    range=x_range,
                    autorange=False,
                    tickmode='array', tickvals=cst_times, ticktext=tick_text,
                    showticklabels=True, row=2, col=1
                )
        
            if show_dmi:
                dmi_row = 3 if show_macd else 2
                fig.update_yaxes(
                    range=[0, 60],
                    autorange=False,
                    fixedrange=True,
                    side='right',
                    gridcolor='rgba(128,128,128,0.3)',
                    gridwidth=0.5,
                    row=dmi_row, col=1
                )
                fig.update_xaxes(
                    range=x_range,
                    autorange=False,
                    tickmode='array', tickvals=cst_times, ticktext=tick_text,
                    showticklabels=True, row=dmi_row, col=1
                )
        
            st.plotly_chart(fig, width='stretch', config={'scrollZoom': True, 'displayModeBar': True})
        else:
            st.warning("차트 데이터를 불러올 수 없습니다. (최근 7일 데이터만 가능)")
    
    with col2:
        total = len(trades)
        total_profit = sum(t['profit'] for t in trades)
        wins = len([t for t in trades if t['profit'] > 0])
        losses = len([t for t in trades if t['profit'] <= 0])
        
        st.markdown(f"### 거래 목록 ({total}건 | 승률 {wins/total*100:.0f}%)")
        st.caption("행 클릭 → 차트 강조 + 하단 상세")
        
        rows = []
        for idx, t in enumerate(trades):
            kst_str = t['entry_time_kst'].strftime("%H:%M") if hasattr(t['entry_time_kst'], 'strftime') else str(t['entry_time_kst'])
            cst_str = t['entry_time_cst'].strftime("%H:%M") if hasattr(t['entry_time_cst'], 'strftime') else str(t['entry_time_cst'])
            rows.append({
                '한국시간': kst_str,
                '시카고시간': cst_str,
                '구분': t['type'],
                '체결가': f"{t['entry_price']:.2f}",
                '순손익': t['profit']
            })
        
        trade_df = pd.DataFrame(rows)
        
        if show_macd and show_dmi:
            list_height = 1110
        elif show_macd or show_dmi:
            list_height = 880
        else:
            list_height = 680
        
        selection = st.dataframe(
            trade_df,
            height=list_height,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row"
        )
        
        if selection and selection.selection and selection.selection.rows:
            selected = selection.selection.rows[0]
            if selected != st.session_state.focused_idx:
                st.session_state.focused_idx = selected
                st.rerun()
        else:
            if st.session_state.focused_idx is not None:
                st.session_state.focused_idx = None
                st.rerun()
    
    if st.session_state.focused_idx is not None and 0 <= st.session_state.focused_idx < len(trades):
        st.divider()
        try:
            focused_trade = trades[st.session_state.focused_idx]
            
            entry_kst = focused_trade['entry_time_kst'].strftime("%m/%d %H:%M")
            exit_kst = focused_trade['exit_time_kst'].strftime("%H:%M")
            entry_cst_str = focused_trade['entry_time_cst'].strftime("%H:%M")
            exit_cst_str = focused_trade['exit_time_cst'].strftime("%H:%M")
            
            profit = focused_trade['profit']
            entry_p = focused_trade['entry_price']
            exit_p = focused_trade['exit_price']
            points = exit_p - entry_p
            
            if '매도' in focused_trade['type']:
                points = -points
            
            if points >= 0:
                points_text = f"+{points:.1f}P"
            else:
                points_text = f"{points:.1f}P"
            
            if profit > 0:
                result_text = f'<span style="color:#FF0000;font-weight:bold;">수익 +${profit:.2f}</span>'
            else:
                result_text = f'<span style="color:#4444FF;font-weight:bold;">손실 ${profit:.2f}</span>'
            
            detail_left, detail_right = st.columns(2)
            
            with detail_left:
                st.markdown(f"### 선택된 거래")
                st.markdown(
                    f'<div style="padding:8px 0;">'
                    f'<b>{focused_trade["type"]}</b> {focused_trade["quantity"]}계약 | {points_text} | {result_text}'
                    f'</div>',
                    unsafe_allow_html=True
                )
                
                st.markdown(
                    f"""| | 가격 | 시간(KST) | 시간(CST) |
|---|---:|---:|---:|
| **진입** | {entry_p:.2f} | {entry_kst} | {entry_cst_str} |
| **청산** | {exit_p:.2f} | {exit_kst} | {exit_cst_str} |"""
                )

                mae, mfe = calculate_mae_mfe(focused_trade, df_chart_data if df_chart_data is not None else None)
                if mae is not None:
                    st.markdown("**📊 진입 후 가격 움직임**")
                    col_mae, col_mfe = st.columns(2)
                    with col_mae:
                        st.metric(
                            label="MAE (최대 역행)",
                            value=f"{mae:.1f}P",
                            delta=f"{'하락' if '매수' in focused_trade['type'] else '상승'}",
                            delta_color="inverse",
                            help="진입 후 포지션에 불리한 방향으로 최대 몇 포인트 역행했는지"
                        )
                    with col_mfe:
                        st.metric(
                            label="MFE (최대 이익)",
                            value=f"{mfe:.1f}P",
                            delta=f"{'상승' if '매수' in focused_trade['type'] else '하락'}",
                            delta_color="normal",
                            help="진입 후 포지션에 유리한 방향으로 최대 몇 포인트 갔는지"
                        )
                else:
                    st.caption("MAE/MFE: 차트 데이터 없음")
            
            with detail_right:
                focused_cls = None
                if classifications and st.session_state.focused_idx < len(classifications):
                    focused_cls = classifications[st.session_state.focused_idx]
                
                if focused_cls:
                    cls_name = focused_cls['classification']
                    cls_score = focused_cls['score']
                    cls_color_map = {'원칙': '#00C853', '운빨': '#FF9800', '뇌동': '#FF1744', '판정불가': '#9E9E9E'}
                    c_color = cls_color_map.get(cls_name, '#9E9E9E')
                    
                    st.markdown(
                        f'<div style="background:{c_color};color:white;padding:10px 16px;border-radius:8px;'
                        f'text-align:center;font-weight:bold;font-size:18px;margin:8px 0;">'
                        f'{cls_name}매매 ({cls_score}/6)'
                        f'</div>',
                        unsafe_allow_html=True
                    )
                    
                    criteria_labels = [
                        ('ma33', '33선'),
                        ('ma5', '5선'),
                        ('macd', 'MACD'),
                        ('dmi', 'DMI'),
                        ('time_window', '시간'),
                        ('stop_loss', '손절'),
                    ]
                    for i in range(0, len(criteria_labels), 2):
                        c1, c2 = st.columns(2)
                        for col, (key, label) in zip([c1, c2], criteria_labels[i:i+2]):
                            with col:
                                passed = focused_cls['criteria'].get(key, False)
                                msg = focused_cls['messages'].get(key, '')
                                icon = '✅' if passed else '❌'
                                st.markdown(f"{icon} **{label}**<br><span style='color:#888;font-size:12px;'>{msg}</span>", unsafe_allow_html=True)
        except (IndexError, KeyError):
            st.session_state.focused_idx = None
    
    if classifications:
        st.divider()
        st.subheader("매매 상세 복기")
        st.caption("체크박스 선택 시 차트에서 해당 매매가 황금색으로 강조됩니다 (5선=1분봉, 33선·MACD·DMI=3분봉 기준 원칙 판정)")
        
        classification_colors = {
            '원칙': '#00C853',
            '운빨': '#FF9800',
            '뇌동': '#FF1744',
            '판정불가': '#9E9E9E'
        }
        
        for idx, t in enumerate(trades):
            cls = classifications[idx] if idx < len(classifications) else None
            kst_str = t['entry_time_kst'].strftime("%H:%M") if hasattr(t['entry_time_kst'], 'strftime') else str(t['entry_time_kst'])
            
            trade_type = t.get('type', '')
            profit = t['profit']
            score = cls['score'] if cls else 0
            classification = cls['classification'] if cls else '판정불가'
            cls_color = classification_colors.get(classification, '#9E9E9E')
            
            profit_str = f"+${profit:.2f}" if profit > 0 else f"${profit:.2f}"
            profit_color = "#FF0000" if profit > 0 else "#4444FF"
            
            col_check, col_info = st.columns([0.05, 0.95])
            
            with col_check:
                checked = st.checkbox(
                    f"trade_{idx}",
                    value=idx in st.session_state.checked_trades,
                    key=f"trade_check_{idx}",
                    label_visibility="collapsed"
                )
                if checked and idx not in st.session_state.checked_trades:
                    st.session_state.checked_trades.add(idx)
                    st.rerun()
                elif not checked and idx in st.session_state.checked_trades:
                    st.session_state.checked_trades.discard(idx)
                    st.rerun()
            
            with col_info:
                criteria_details = ""
                if cls:
                    for key, label in [('ma33', '33선'), ('ma5', '5선'), ('macd', 'MACD'), ('dmi', 'DMI'), ('time_window', '시간'), ('stop_loss', '손절')]:
                        passed = cls['criteria'].get(key, False)
                        msg = cls['messages'].get(key, '')
                        if passed:
                            criteria_details += f'<span style="color:#00C853;margin-right:8px;font-size:12px;">✔{label}</span>'
                        else:
                            criteria_details += f'<span style="color:#FF1744;margin-right:8px;font-size:12px;">✘{label}</span>'
                
                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:10px;padding:4px 0;flex-wrap:wrap;">'
                    f'<span style="color:#888;min-width:45px;">{kst_str}</span>'
                    f'<span style="min-width:28px;">{trade_type}</span>'
                    f'<span style="background:{cls_color};color:white;padding:1px 8px;border-radius:10px;font-size:12px;">{classification}({score})</span>'
                    f'<span style="color:{profit_color};font-weight:bold;min-width:75px;">{profit_str}</span>'
                    f'<span style="color:#888;font-size:11px;min-width:140px;">{t["entry_price"]:.2f} -> {t["exit_price"]:.2f}</span>'
                    f'<span>{criteria_details}</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )
                
                if cls:
                    detail_parts = []
                    for key, label in [('ma33', '33선'), ('ma5', '5선'), ('macd', 'MACD'), ('dmi', 'DMI'), ('time_window', '시간'), ('stop_loss', '손절')]:
                        msg = cls['messages'].get(key, '')
                        if msg:
                            passed = cls['criteria'].get(key, False)
                            color = '#00C853' if passed else '#FF1744'
                            detail_parts.append(f'<span style="color:{color};font-size:11px;">{label}: {msg}</span>')
                    if detail_parts:
                        st.markdown(
                            f'<div style="margin-left:45px;padding:2px 0 6px 0;display:flex;gap:12px;flex-wrap:wrap;">'
                            + ' '.join(detail_parts)
                            + '</div>',
                            unsafe_allow_html=True
                        )
        
        if st.session_state.checked_trades:
            st.divider()
            st.subheader("선택된 거래 상세")
            for idx in sorted(st.session_state.checked_trades):
                if idx < len(trades) and idx < len(classifications):
                    t = trades[idx]
                    cls = classifications[idx]
                    kst_str = t['entry_time_kst'].strftime("%H:%M") if hasattr(t['entry_time_kst'], 'strftime') else str(t['entry_time_kst'])
                    
                    st.markdown(f"**{kst_str} {t['type']} - {cls['classification']}({cls['score']}점)**")
                    detail_cols = st.columns(6)
                    criteria_labels = ['33선 필터', '5선 안착', 'MACD', 'DMI', '시간대', '손절']
                    criteria_keys = ['ma33', 'ma5', 'macd', 'dmi', 'time_window', 'stop_loss']
                    for j, (label, key) in enumerate(zip(criteria_labels, criteria_keys)):
                        with detail_cols[j]:
                            passed = cls['criteria'].get(key, False)
                            icon = "✅" if passed else "❌"
                            msg = cls['messages'].get(key, '')
                            st.markdown(f"{icon} **{label}**")
                            st.caption(msg)

    st.divider()
    st.subheader("📊 MAE/MFE 분석 - 손절선 설정 도우미")

    # 전체 저장된 거래의 MAE/MFE 계산 (날짜별 캔들 캐시 활용)
    all_trades = db.get_all_paired_trades()
    all_mae_mfe_list = []

    for t in all_trades:
        trade_date_str = t['entry_time_cst'][:10] if isinstance(t['entry_time_cst'], str) else t['entry_time_cst'].strftime('%Y-%m-%d')
        entry_dt = datetime.fromisoformat(t['entry_time_cst']) if isinstance(t['entry_time_cst'], str) else t['entry_time_cst']
        exit_dt = datetime.fromisoformat(t['exit_time_cst']) if isinstance(t['exit_time_cst'], str) else t['exit_time_cst']

        # 해당 날짜 캔들 캐시에서 가져오기
        cached = db.get_cached_candles(trade_date_str, "NQ=F", "1m")
        if not cached:
            cached = db.get_cached_candles(trade_date_str, "NQ=F", "3m")

        if cached:
            cst = pytz.timezone('America/Chicago')
            df_c = pd.DataFrame(cached)
            df_c['timestamp'] = pd.to_datetime(df_c['timestamp'])
            df_c.set_index('timestamp', inplace=True)
            df_c.columns = [c.capitalize() for c in df_c.columns]
            df_c.index = df_c.index.tz_localize(cst)

            trade_obj = {
                'entry_time_cst': entry_dt,
                'exit_time_cst': exit_dt,
                'entry_price': t['entry_price'],
                'type': t['type']
            }
            mae, mfe = calculate_mae_mfe(trade_obj, df_c)
            if mae is not None:
                entry_kst = datetime.fromisoformat(t['entry_time_kst']) if isinstance(t['entry_time_kst'], str) else t['entry_time_kst']
                all_mae_mfe_list.append({
                    '날짜': trade_date_str,
                    '진입시간': entry_kst.strftime("%m/%d %H:%M"),
                    '구분': t['type'],
                    'MAE': mae,
                    'MFE': mfe,
                    '손익': t['profit']
                })

    if not all_mae_mfe_list:
        st.warning("MAE/MFE 계산 가능한 거래가 없습니다. 각 날짜별로 차트를 한 번씩 조회해주세요. (캔들 캐시 필요)")
        if len(all_trades) == 0:
            st.caption("저장된 거래가 없거나, 네트워크/DNS 오류로 전체 목록을 가져오지 못한 경우입니다. 화면 상단 DB 안내를 확인하세요.")
    else:
        st.caption(f"전체 {len(all_mae_mfe_list)}건 거래 분석 ({len(set([x['날짜'] for x in all_mae_mfe_list]))}일)")

        tab1, tab2 = st.tabs(["MAE 분포 (손절선 결정)", "MFE vs MAE 산점도"])
        mae_df = pd.DataFrame(all_mae_mfe_list)

        with tab1:
            colors = ['#FF4444' if '매수' in g else '#4444FF' for g in mae_df['구분']]
            fig_mae = go.Figure(go.Bar(
                x=mae_df['진입시간'],
                y=mae_df['MAE'],
                marker_color=colors,
                text=mae_df['MAE'].apply(lambda x: f"{x:.1f}P"),
                textposition='outside',
                hovertemplate='%{x}<br>MAE: %{y:.1f}P<extra></extra>'
            ))
            fig_mae.update_layout(
                title=f"전체 거래 MAE 분포 (빨강=매수, 파랑=매도)",
                xaxis_title="진입시간",
                yaxis_title="MAE (포인트)",
                height=400,
                template="plotly_dark"
            )
            st.plotly_chart(fig_mae, use_container_width=True)

            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.metric("평균 MAE", f"{mae_df['MAE'].mean():.1f}P")
            with m2:
                st.metric("최대 MAE", f"{mae_df['MAE'].max():.1f}P")
            with m3:
                st.metric("중앙값 MAE", f"{mae_df['MAE'].median():.1f}P")
            with m4:
                buy_df = mae_df[mae_df['구분'].str.contains('매수')]
                sell_df = mae_df[mae_df['구분'].str.contains('매도')]
                st.metric("매수 평균 MAE", f"{buy_df['MAE'].mean():.1f}P" if len(buy_df) else "-")

            st.markdown("#### 🎯 손절선 시뮬레이터")
            stop_loss = st.slider("손절선 설정 (포인트)", 1.0, 50.0, 10.0, 0.5)
            survived = mae_df[mae_df['MAE'] <= stop_loss]
            total = len(mae_df)
            profit_survived = survived[survived['손익'] > 0]
            st.info(
                f"손절선 **{stop_loss}P** 기준 →  "
                f"손절 안 당하는 거래: **{len(survived)}건 / {total}건 ({len(survived)/total*100:.0f}%)**  |  "
                f"그 중 수익: **{len(profit_survived)}건**"
            )

        with tab2:
            colors_scatter = ['#FF4444' if p > 0 else '#4444FF' for p in mae_df['손익']]
            fig_scatter = go.Figure()
            fig_scatter.add_trace(go.Scatter(
                x=mae_df['MAE'],
                y=mae_df['MFE'],
                mode='markers',
                marker=dict(size=12, color=colors_scatter),
                customdata=mae_df[['진입시간', '구분', '손익']].values,
                hovertemplate='진입: %{customdata[0]}<br>구분: %{customdata[1]}<br>MAE: %{x:.1f}P<br>MFE: %{y:.1f}P<br>손익: $%{customdata[2]:.2f}<extra></extra>',
                showlegend=False
            ))
            max_val = max(mae_df['MAE'].max(), mae_df['MFE'].max()) + 5
            fig_scatter.add_trace(go.Scatter(
                x=[0, max_val], y=[0, max_val],
                mode='lines',
                line=dict(color='gray', dash='dash', width=1),
                name='MAE=MFE 기준선'
            ))
            fig_scatter.update_layout(
                title="MFE vs MAE (빨강=수익, 파랑=손실)",
                xaxis_title="MAE (최대 역행 포인트)",
                yaxis_title="MFE (최대 이익 포인트)",
                height=450,
                template="plotly_dark"
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

else:
    st.info("👈 좌측 사이드바에서 체결내역과 청산내역 파일을 업로드하고 '매칭 분석 시작' 버튼을 클릭하세요.")
