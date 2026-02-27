import os
import time
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import json

DB_PATH = "trades.db"

def _sb_retry(fn, max_attempts=5):
    """Supabase HTTP 호출 일시 오류 시 재시도 (ReadError/Errno 11 등)."""
    last = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            last = e
            if attempt >= max_attempts - 1:
                raise
            time.sleep(1.5)
    raise last

# Supabase 사용 여부 (Streamlit Cloud 등에서 환경변수 설정 시)
USE_SUPABASE = bool(os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_KEY"))
_supabase = None
SUPABASE_INIT_ERROR = None  # 연결 실패 시 메시지 저장

def _sb():
    global _supabase, SUPABASE_INIT_ERROR
    if not USE_SUPABASE:
        return None
    if SUPABASE_INIT_ERROR is not None:
        return None
    if _supabase is None:
        try:
            from supabase import create_client
            _supabase = create_client(
                os.environ["SUPABASE_URL"].rstrip("/"),
                os.environ["SUPABASE_KEY"].strip()
            )
        except Exception as e:
            SUPABASE_INIT_ERROR = str(e)
            _supabase = None
            return None
    return _supabase

def get_settlement_date(kst_datetime) -> str:
    """
    한국 증권사 정산일 계산: KST 08:00 ~ 다음날 06:59 = 하루
    07:00 이전 거래는 전날 정산일에 포함
    """
    if isinstance(kst_datetime, str):
        kst_datetime = datetime.fromisoformat(kst_datetime)
    
    if kst_datetime.hour < 7:
        settlement = kst_datetime - timedelta(days=1)
    else:
        settlement = kst_datetime
    
    return settlement.strftime('%Y-%m-%d')

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    if USE_SUPABASE:
        return
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS paired_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_time_kst TEXT NOT NULL,
            entry_time_cst TEXT NOT NULL,
            exit_time_kst TEXT NOT NULL,
            exit_time_cst TEXT NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL NOT NULL,
            quantity INTEGER NOT NULL,
            profit REAL NOT NULL,
            trade_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            trade_date_cst TEXT NOT NULL,
            trade_date_kst TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    try:
        cursor.execute("ALTER TABLE paired_trades ADD COLUMN trade_date_kst TEXT")
    except Exception:
        pass
    try:
        cursor.execute("ALTER TABLE paired_trades ADD COLUMN settlement_date TEXT")
    except Exception:
        pass
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS candle_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(trade_date, symbol, timeframe, timestamp)
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_paired_trade_date ON paired_trades(trade_date_cst)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_paired_symbol ON paired_trades(symbol)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_candle_date ON candle_cache(trade_date, symbol, timeframe)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_settlement_date ON paired_trades(settlement_date)")
    
    cursor.execute("SELECT id, entry_time_kst FROM paired_trades WHERE settlement_date IS NULL")
    rows = cursor.fetchall()
    for row in rows:
        settlement = get_settlement_date(row['entry_time_kst'])
        cursor.execute("UPDATE paired_trades SET settlement_date = ? WHERE id = ?", (settlement, row['id']))
    
    conn.commit()
    conn.close()

def save_paired_trade(trade: Dict) -> int:
    sb = _sb()
    if sb:
        entry_cst = trade['entry_time_cst']
        entry_cst_str = entry_cst.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(entry_cst, datetime) else str(entry_cst)
        exit_cst = trade['exit_time_cst']
        exit_cst_str = exit_cst.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(exit_cst, datetime) else str(exit_cst)
        entry_kst = trade['entry_time_kst']
        entry_kst_str = entry_kst.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(entry_kst, datetime) else str(entry_kst)
        exit_kst = trade['exit_time_kst']
        exit_kst_str = exit_kst.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(exit_kst, datetime) else str(exit_kst)
        trade_date_cst = trade.get('trade_date') or (entry_cst.strftime('%Y-%m-%d') if isinstance(entry_cst, datetime) else str(entry_cst).split('T')[0])
        trade_date_kst = entry_kst.strftime('%Y-%m-%d') if hasattr(entry_kst, 'strftime') else str(entry_kst)[:10]
        settlement_date = get_settlement_date(entry_kst_str)
        symbol = trade.get('symbol', 'MNQ')
        row = {
            "entry_time_kst": entry_kst_str, "entry_time_cst": entry_cst_str,
            "exit_time_kst": exit_kst_str, "exit_time_cst": exit_cst_str,
            "entry_price": trade['entry_price'], "exit_price": trade['exit_price'],
            "quantity": trade['quantity'], "profit": trade['profit'],
            "trade_type": trade.get('type', '매수'), "symbol": symbol,
            "trade_date_cst": trade_date_cst, "trade_date_kst": trade_date_kst,
            "settlement_date": settlement_date,
        }
        r = sb.table("paired_trades").select("id").eq("entry_time_cst", entry_cst_str).eq("exit_time_cst", exit_cst_str).eq("entry_price", trade['entry_price']).eq("exit_price", trade['exit_price']).eq("quantity", trade['quantity']).eq("profit", trade['profit']).execute()
        if r.data and len(r.data) > 0:
            return r.data[0]["id"]
        r = sb.table("paired_trades").insert(row).execute()
        return r.data[0]["id"] if r.data else 0

    if USE_SUPABASE:
        return 0
    conn = get_connection()
    cursor = conn.cursor()
    entry_cst = trade['entry_time_cst']
    entry_cst_str = entry_cst.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(entry_cst, datetime) else str(entry_cst)
    if 'trade_date' in trade and trade['trade_date']:
        trade_date_cst = trade['trade_date']
    elif isinstance(entry_cst, datetime):
        trade_date_cst = entry_cst.strftime('%Y-%m-%d')
    else:
        trade_date_cst = str(entry_cst).split('T')[0]
    exit_cst = trade['exit_time_cst']
    exit_cst_str = exit_cst.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(exit_cst, datetime) else str(exit_cst)
    entry_kst = trade['entry_time_kst']
    entry_kst_str = entry_kst.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(entry_kst, datetime) else str(entry_kst)
    exit_kst = trade['exit_time_kst']
    exit_kst_str = exit_kst.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(exit_kst, datetime) else str(exit_kst)
    symbol = trade.get('symbol', 'MNQ')
    cursor.execute("""SELECT id FROM paired_trades WHERE entry_time_cst = ? AND exit_time_cst = ? AND entry_price = ? AND exit_price = ? AND quantity = ? AND profit = ?""",
        (entry_cst_str, exit_cst_str, trade['entry_price'], trade['exit_price'], trade['quantity'], trade['profit']))
    existing = cursor.fetchone()
    if existing:
        conn.close()
        return existing['id']
    trade_date_kst = entry_kst.strftime('%Y-%m-%d') if hasattr(entry_kst, 'strftime') else str(entry_kst)[:10]
    settlement_date = get_settlement_date(entry_kst_str)
    cursor.execute("""INSERT INTO paired_trades (entry_time_kst, entry_time_cst, exit_time_kst, exit_time_cst, entry_price, exit_price, quantity, profit, trade_type, symbol, trade_date_cst, trade_date_kst, settlement_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (entry_kst_str, entry_cst_str, exit_kst_str, exit_cst_str, trade['entry_price'], trade['exit_price'], trade['quantity'], trade['profit'], trade.get('type', '매수'), symbol, trade_date_cst, trade_date_kst, settlement_date))
    trade_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return trade_id

def save_paired_trades_batch(trades: List[Dict]) -> int:
    count = 0
    for trade in trades:
        save_paired_trade(trade)
        count += 1
    return count

def _trade_row_from_sqlite(r: dict) -> dict:
    """SQLite 한 행을 Supabase paired_trades 행으로 변환."""
    entry_kst = r["entry_time_kst"]
    entry_cst = r["entry_time_cst"]
    return {
        "entry_time_kst": entry_kst if isinstance(entry_kst, str) else (entry_kst.strftime('%Y-%m-%dT%H:%M:%S') if hasattr(entry_kst, 'strftime') else str(entry_kst)),
        "entry_time_cst": entry_cst if isinstance(entry_cst, str) else (entry_cst.strftime('%Y-%m-%dT%H:%M:%S') if hasattr(entry_cst, 'strftime') else str(entry_cst)),
        "exit_time_kst": r["exit_time_kst"] if isinstance(r["exit_time_kst"], str) else (r["exit_time_kst"].strftime('%Y-%m-%dT%H:%M:%S') if hasattr(r["exit_time_kst"], 'strftime') else str(r["exit_time_kst"])),
        "exit_time_cst": r["exit_time_cst"] if isinstance(r["exit_time_cst"], str) else (r["exit_time_cst"].strftime('%Y-%m-%dT%H:%M:%S') if hasattr(r["exit_time_cst"], 'strftime') else str(r["exit_time_cst"])),
        "entry_price": r["entry_price"], "exit_price": r["exit_price"],
        "quantity": r["quantity"], "profit": r["profit"],
        "trade_type": r.get("trade_type", "매수"),
        "symbol": r.get("symbol", "MNQ"),
        "trade_date_cst": r.get("trade_date_cst") or str(entry_cst)[:10],
        "trade_date_kst": r.get("trade_date_kst") or str(entry_kst)[:10],
        "settlement_date": r.get("settlement_date") or get_settlement_date(entry_kst),
    }

def insert_paired_trades_batch_supabase(rows: List[dict], batch_size: int = 80) -> int:
    """Supabase에 거래를 배치로 삽입 (가져오기 시 타임아웃 방지)."""
    sb = _sb()
    if not sb:
        return 0
    total = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        def _insert_chunk():
            sb.table("paired_trades").insert(chunk).execute()
            return len(chunk)
        try:
            n = _sb_retry(_insert_chunk)
            total += n
        except Exception:
            for row in chunk:
                try:
                    _sb_retry(lambda r=row: sb.table("paired_trades").insert(r).execute())
                    total += 1
                except Exception:
                    pass
    return total

def get_available_dates() -> List[str]:
    sb = _sb()
    if sb:
        def _fetch():
            r = sb.table("paired_trades").select("settlement_date").not_.is_("settlement_date", "null").execute()
            return list({row["settlement_date"] for row in (r.data or [])})
        dates = _sb_retry(_fetch)
        dates.sort(reverse=True)
        return dates
    if USE_SUPABASE:
        return []
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""SELECT DISTINCT settlement_date as trade_date FROM paired_trades WHERE settlement_date IS NOT NULL ORDER BY trade_date DESC""")
    dates = [row['trade_date'] for row in cursor.fetchall()]
    conn.close()
    return dates

def _row_to_trade(row: Dict) -> Dict:
    return {
        'id': row['id'],
        'entry_time_kst': row['entry_time_kst'],
        'entry_time_cst': row['entry_time_cst'],
        'exit_time_kst': row['exit_time_kst'],
        'exit_time_cst': row['exit_time_cst'],
        'entry_price': row['entry_price'],
        'exit_price': row['exit_price'],
        'quantity': row['quantity'],
        'profit': row['profit'],
        'type': row['trade_type'],
        'symbol': row['symbol']
    }

def get_paired_trades_by_date(trade_date: str, symbol: Optional[str] = None) -> List[Dict]:
    sb = _sb()
    if sb:
        def _fetch():
            q = sb.table("paired_trades").select("*").eq("settlement_date", trade_date).order("entry_time_kst")
            if symbol:
                q = q.eq("symbol", symbol)
            r = q.execute()
            return [_row_to_trade(row) for row in (r.data or [])]
        return _sb_retry(_fetch)
    if USE_SUPABASE:
        return []
    conn = get_connection()
    cursor = conn.cursor()
    if symbol:
        cursor.execute("""SELECT * FROM paired_trades WHERE settlement_date = ? AND symbol = ? ORDER BY entry_time_kst""", (trade_date, symbol))
    else:
        cursor.execute("""SELECT * FROM paired_trades WHERE settlement_date = ? ORDER BY entry_time_kst""", (trade_date,))
    rows = cursor.fetchall()
    conn.close()
    return [_row_to_trade(dict(row)) for row in rows]

def get_all_paired_trades() -> List[Dict]:
    sb = _sb()
    if sb:
        def _fetch():
            r = sb.table("paired_trades").select("*").order("entry_time_cst", desc=True).execute()
            return [_row_to_trade(row) for row in (r.data or [])]
        return _sb_retry(_fetch)
    if USE_SUPABASE:
        return []
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM paired_trades ORDER BY entry_time_cst DESC")
    rows = cursor.fetchall()
    conn.close()
    return [_row_to_trade(dict(row)) for row in rows]

def clear_paired_trades_by_date(trade_date: str):
    sb = _sb()
    if sb:
        sb.table("paired_trades").delete().eq("settlement_date", trade_date).execute()
        return
    if USE_SUPABASE:
        return
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM paired_trades WHERE settlement_date = ?", (trade_date,))
    conn.commit()
    conn.close()

def clear_all_paired_trades():
    sb = _sb()
    if sb:
        sb.table("paired_trades").delete().gte("id", 0).execute()
        return
    if USE_SUPABASE:
        return
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM paired_trades")
    conn.commit()
    conn.close()

def migrate_from_json(json_file: str = "saved_trades.json") -> int:
    try:
        with open(json_file, 'r') as f:
            trades = json.load(f)
        if trades:
            return save_paired_trades_batch(trades)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"Migration error: {e}")
    return 0

def check_date_exists(trade_date: str) -> bool:
    sb = _sb()
    if sb:
        def _fetch():
            r = sb.table("paired_trades").select("id").eq("trade_date_cst", trade_date).limit(1).execute()
            return len(r.data or []) > 0
        return _sb_retry(_fetch)
    if USE_SUPABASE:
        return False
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as cnt FROM paired_trades WHERE trade_date_cst = ?", (trade_date,))
    row = cursor.fetchone()
    conn.close()
    return row['cnt'] > 0

def save_candle_data(trade_date: str, symbol: str, timeframe: str, candles: List[Dict]) -> int:
    sb = _sb()
    if USE_SUPABASE and sb is None:
        return 0
    if sb:
        rows = []
        for c in candles:
            rows.append({
                "trade_date": trade_date, "symbol": symbol, "timeframe": timeframe,
                "timestamp": c["timestamp"],
                "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"],
                "volume": c.get("volume", 0)
            })
        if not rows:
            return 0
        try:
            sb.table("candle_cache").upsert(rows, on_conflict="trade_date,symbol,timeframe,timestamp").execute()
            return len(rows)
        except Exception as e:
            for row in rows:
                try:
                    sb.table("candle_cache").upsert(row, on_conflict="trade_date,symbol,timeframe,timestamp").execute()
                except Exception:
                    pass
            return len(rows)
    conn = get_connection()
    cursor = conn.cursor()
    count = 0
    for candle in candles:
        try:
            cursor.execute("""INSERT OR REPLACE INTO candle_cache (trade_date, symbol, timeframe, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (trade_date, symbol, timeframe, candle['timestamp'], candle['open'], candle['high'], candle['low'], candle['close'], candle.get('volume', 0)))
            count += 1
        except Exception as e:
            print(f"Candle save error: {e}")
    conn.commit()
    conn.close()
    return count

def get_cached_candles(trade_date: str, symbol: str, timeframe: str) -> List[Dict]:
    sb = _sb()
    if sb:
        def _fetch():
            r = sb.table("candle_cache").select("timestamp,open,high,low,close,volume").eq("trade_date", trade_date).eq("symbol", symbol).eq("timeframe", timeframe).order("timestamp").execute()
            return [{"timestamp": row["timestamp"], "open": row["open"], "high": row["high"], "low": row["low"], "close": row["close"], "volume": row.get("volume", 0)} for row in (r.data or [])]
        return _sb_retry(_fetch)
    if USE_SUPABASE:
        return []
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""SELECT timestamp, open, high, low, close, volume FROM candle_cache WHERE trade_date = ? AND symbol = ? AND timeframe = ? ORDER BY timestamp""", (trade_date, symbol, timeframe))
    rows = cursor.fetchall()
    conn.close()
    return [{"timestamp": row['timestamp'], "open": row['open'], "high": row['high'], "low": row['low'], "close": row['close'], "volume": row['volume']} for row in rows]

def has_cached_candles(trade_date: str, symbol: str, timeframe: str) -> bool:
    sb = _sb()
    if sb:
        def _fetch():
            r = sb.table("candle_cache").select("id").eq("trade_date", trade_date).eq("symbol", symbol).eq("timeframe", timeframe).limit(1).execute()
            return len(r.data or []) > 0
        return _sb_retry(_fetch)
    if USE_SUPABASE:
        return False
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""SELECT COUNT(*) as cnt FROM candle_cache WHERE trade_date = ? AND symbol = ? AND timeframe = ?""", (trade_date, symbol, timeframe))
    row = cursor.fetchone()
    conn.close()
    return row['cnt'] > 0

def clear_candle_cache(trade_date: str = None):
    sb = _sb()
    if sb:
        if trade_date:
            sb.table("candle_cache").delete().eq("trade_date", trade_date).execute()
        else:
            sb.table("candle_cache").delete().gte("id", 0).execute()
        return
    if USE_SUPABASE:
        return
    conn = get_connection()
    cursor = conn.cursor()
    if trade_date:
        cursor.execute("DELETE FROM candle_cache WHERE trade_date = ?", (trade_date,))
    else:
        cursor.execute("DELETE FROM candle_cache")
    conn.commit()
    conn.close()

def import_from_sqlite(db_path: str, skip_candles: bool = False) -> tuple:
    """
    Replit 등에서 받은 trades.db(SQLite) 내용을 현재 DB(Supabase 또는 SQLite)로 옮깁니다.
    skip_candles=True면 거래만 넣고 캔들은 생략 (타임아웃 방지용).
    반환: (넣은 거래 수, 넣은 캔들 수)
    """
    if not os.path.isfile(db_path):
        return (0, 0)
    try:
        src = sqlite3.connect(db_path)
        src.row_factory = sqlite3.Row
        cur = src.cursor()
        trades_done = 0
        candles_done = 0
        # paired_trades
        cur.execute("""SELECT entry_time_kst, entry_time_cst, exit_time_kst, exit_time_cst,
            entry_price, exit_price, quantity, profit, trade_type, symbol, trade_date_cst, trade_date_kst, settlement_date
            FROM paired_trades""")
        all_trades = cur.fetchall()
        sb = _sb()
        if sb:
            rows = [_trade_row_from_sqlite(dict(row)) for row in all_trades]
            trades_done = insert_paired_trades_batch_supabase(rows)
        else:
            for row in all_trades:
                r = dict(row)
                trade = {
                    "entry_time_kst": r["entry_time_kst"],
                    "entry_time_cst": r["entry_time_cst"],
                    "exit_time_kst": r["exit_time_kst"],
                    "exit_time_cst": r["exit_time_cst"],
                    "entry_price": r["entry_price"],
                    "exit_price": r["exit_price"],
                    "quantity": r["quantity"],
                    "profit": r["profit"],
                    "type": r["trade_type"],
                    "symbol": r["symbol"],
                    "trade_date": r.get("trade_date_cst") or (r["entry_time_cst"][:10] if r["entry_time_cst"] else ""),
                }
                save_paired_trade(trade)
                trades_done += 1
        # candle_cache (skip_candles면 생략)
        if not skip_candles:
            cur.execute("""SELECT trade_date, symbol, timeframe, timestamp, open, high, low, close, volume FROM candle_cache ORDER BY trade_date, symbol, timeframe, timestamp""")
            rows = cur.fetchall()
            from collections import defaultdict
            grouped = defaultdict(list)
            for row in rows:
                r = dict(row)
                key = (r["trade_date"], r["symbol"], r["timeframe"])
                grouped[key].append({
                    "timestamp": r["timestamp"],
                    "open": r["open"], "high": r["high"], "low": r["low"], "close": r["close"],
                    "volume": r.get("volume", 0),
                })
            for (td, sym, tf), candles in grouped.items():
                candles_done += save_candle_data(td, sym, tf, candles)
        src.close()
        return (trades_done, candles_done)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise


if not USE_SUPABASE:
    try:
        init_db()
    except Exception:
        pass
