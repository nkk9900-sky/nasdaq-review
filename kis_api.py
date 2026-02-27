import requests
import json
import os
from datetime import datetime, timedelta
import pandas as pd
import pytz
import time

APP_KEY = os.environ.get('APP_KEY', '')
APP_SECRET = os.environ.get('APP_SECRET', '')

BASE_URL = "https://openapi.koreainvestment.com:9443"

_token_cache = {
    'token': None,
    'expires_at': 0
}

def get_access_token():
    """한국투자증권 API 접근 토큰 발급 (캐싱 포함)"""
    global _token_cache
    
    if _token_cache['token'] and time.time() < _token_cache['expires_at'] - 60:
        return _token_cache['token']
    
    url = f"{BASE_URL}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }
    
    try:
        res = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
        if res.status_code == 200:
            data = res.json()
            token = data.get('access_token')
            expires_in = data.get('expires_in', 86400)
            
            _token_cache['token'] = token
            _token_cache['expires_at'] = time.time() + expires_in
            
            return token
        else:
            print(f"Token error: {res.status_code} - {res.text}")
            return None
    except Exception as e:
        print(f"Token request failed: {e}")
        return None

def get_futures_symbol(product="NQ"):
    """
    현재 활성화된 나스닥 선물 종목코드 반환
    product: "NQ" (E-mini NASDAQ), "MNQ" (Micro E-mini NASDAQ)
    한국투자증권 형식: NQH25 또는 MNQH25 (상품코드 + 월코드 + 년도)
    """
    now = datetime.now()
    year = now.year % 100
    month = now.month
    
    month_codes = {
        1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
        7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
    }
    
    quarterly_months = [3, 6, 9, 12]
    
    for qm in quarterly_months:
        if month <= qm:
            exp_month = qm
            exp_year = year
            break
    else:
        exp_month = 3
        exp_year = year + 1
    
    month_code = month_codes[exp_month]
    symbol = f"{product}{month_code}{exp_year:02d}"
    print(f"Generated KIS symbol: {symbol}")
    return symbol

def get_futures_minute_data(symbol=None, period="1", count=500, target_date=None):
    """
    해외선물 분봉 데이터 조회
    period: "1"=1분, "3"=3분, "5"=5분, "10"=10분, "15"=15분
    target_date: 조회할 날짜 (YYYYMMDD 형식)
    """
    access_token = get_access_token()
    if not access_token:
        return None
    
    if symbol is None:
        symbol = get_futures_symbol()
    
    if target_date is None:
        target_date = datetime.now().strftime("%Y%m%d")
    
    url = f"{BASE_URL}/uapi/overseas-futureoption/v1/quotations/inquire-time-futurechartprice"
    
    today = datetime.now().strftime("%Y%m%d")
    
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "HHDFC55020400",
        "custtype": "P"
    }
    
    params = {
        "SRS_CD": symbol,
        "EXCH_CD": "CME",
        "START_DATE_TIME": "",
        "CLOSE_DATE_TIME": target_date if target_date else today,
        "QRY_TP": "Q",
        "QRY_CNT": str(count),
        "QRY_GAP": str(period),
        "INDEX_KEY": ""
    }
    
    try:
        res = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"KIS API Response: {res.status_code}, symbol={symbol}, url={url}")
        print(f"KIS API params: {params}")
        if res.status_code == 200:
            data = res.json()
            print(f"KIS API Data: rt_cd={data.get('rt_cd')}, msg_cd={data.get('msg_cd')}, msg={data.get('msg1')}")
            if data.get('rt_cd') == '0':
                output2 = data.get('output2', [])
                if isinstance(output2, dict):
                    output2 = [output2]
                print(f"KIS API output2 count: {len(output2) if output2 else 0}")
                if output2 and len(output2) > 0:
                    print(f"KIS API sample keys: {output2[0].keys() if isinstance(output2[0], dict) else output2[0]}")
                return parse_minute_data_kis(output2)
            else:
                print(f"API Error: {data.get('msg_cd')} - {data.get('msg1', 'Unknown error')}")
                return None
        else:
            print(f"Request error: {res.status_code} - {res.text[:500]}")
            return None
    except Exception as e:
        print(f"Request failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_futures_daily_data(symbol=None, count=100):
    """해외선물 일봉 데이터 조회"""
    access_token = get_access_token()
    if not access_token:
        return None
    
    if symbol is None:
        symbol = get_futures_symbol()
    
    url = f"{BASE_URL}/uapi/overseas-futureoption/v1/quotations/inquire-daily-chartprice"
    
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "HHDFC55020100",
        "custtype": "P"
    }
    
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=count)).strftime("%Y%m%d")
    
    params = {
        "EXCD": "CME",
        "SYMB": symbol,
        "GUBN": "0",
        "BYMD": end_date,
        "MODP": "0",
        "KEYB": ""
    }
    
    try:
        res = requests.get(url, headers=headers, params=params, timeout=30)
        if res.status_code == 200:
            data = res.json()
            if data.get('rt_cd') == '0':
                return parse_daily_data(data.get('output2', []))
            else:
                print(f"API Error: {data.get('msg1', 'Unknown error')}")
                return None
        else:
            print(f"Request error: {res.status_code} - {res.text}")
            return None
    except Exception as e:
        print(f"Request failed: {e}")
        return None

def parse_minute_data(output_list):
    """분봉 데이터 파싱하여 DataFrame 반환"""
    if not output_list:
        return None
    
    records = []
    cst = pytz.timezone('America/Chicago')
    
    for item in output_list:
        try:
            date_str = item.get('xymd', '')
            time_str = item.get('xhms', '')
            
            if date_str and time_str:
                dt_str = f"{date_str} {time_str}"
                dt = datetime.strptime(dt_str, "%Y%m%d %H%M%S")
                dt = cst.localize(dt)
                
                records.append({
                    'Datetime': dt,
                    'Open': float(item.get('open', 0)),
                    'High': float(item.get('high', 0)),
                    'Low': float(item.get('low', 0)),
                    'Close': float(item.get('clos', item.get('last', 0))),
                    'Volume': int(item.get('tvol', 0))
                })
        except Exception as e:
            continue
    
    if not records:
        return None
    
    df = pd.DataFrame(records)
    df.set_index('Datetime', inplace=True)
    df.sort_index(inplace=True)
    return df

def parse_minute_data_v2(output_list):
    """새로운 API 형식 분봉 데이터 파싱 (output1)"""
    if not output_list:
        return None
    
    records = []
    cst = pytz.timezone('America/Chicago')
    
    for item in output_list:
        try:
            date_str = item.get('xymd', item.get('tymd', ''))
            time_str = item.get('xhms', item.get('thms', ''))
            
            if date_str and time_str:
                dt_str = f"{date_str} {time_str}"
                dt = datetime.strptime(dt_str, "%Y%m%d %H%M%S")
                dt = cst.localize(dt)
                
                open_price = float(item.get('open', item.get('oppr', 0)) or 0)
                high_price = float(item.get('high', item.get('hgpr', 0)) or 0)
                low_price = float(item.get('low', item.get('lwpr', 0)) or 0)
                close_price = float(item.get('clos', item.get('last', item.get('pric', 0))) or 0)
                volume = int(item.get('evol', item.get('tvol', item.get('vol', 0))) or 0)
                
                if close_price > 0:
                    records.append({
                        'Datetime': dt,
                        'Open': open_price if open_price > 0 else close_price,
                        'High': high_price if high_price > 0 else close_price,
                        'Low': low_price if low_price > 0 else close_price,
                        'Close': close_price,
                        'Volume': volume
                    })
        except Exception as e:
            print(f"Parse error: {e}, item: {item}")
            continue
    
    if not records:
        return None
    
    df = pd.DataFrame(records)
    df.set_index('Datetime', inplace=True)
    df.sort_index(inplace=True)
    return df

def parse_minute_data_kis(output_list):
    """해외선물 분봉 데이터 파싱 (inquire-time-futurechartprice API)"""
    if not output_list:
        return None
    
    records = []
    cst = pytz.timezone('America/Chicago')
    
    for item in output_list:
        try:
            if not isinstance(item, dict):
                continue
            
            date_str = item.get('xymd', item.get('tymd', item.get('trd_dt', '')))
            time_str = item.get('xhms', item.get('thms', item.get('trd_tm', '')))
            
            if not date_str or not time_str:
                continue
                
            if len(time_str) == 6:
                dt_str = f"{date_str} {time_str}"
            elif len(time_str) == 4:
                dt_str = f"{date_str} {time_str}00"
            else:
                dt_str = f"{date_str} {time_str}"
            
            try:
                dt = datetime.strptime(dt_str, "%Y%m%d %H%M%S")
            except:
                continue
                
            dt = cst.localize(dt)
            
            open_price = float(item.get('open', item.get('oppr', item.get('open_pric', 0))) or 0)
            high_price = float(item.get('high', item.get('hgpr', item.get('high_pric', 0))) or 0)
            low_price = float(item.get('low', item.get('lwpr', item.get('low_pric', 0))) or 0)
            close_price = float(item.get('clos', item.get('last', item.get('pric', item.get('cls_pric', 0)))) or 0)
            volume = int(float(item.get('evol', item.get('tvol', item.get('vol', item.get('acml_vol', 0)))) or 0))
            
            if close_price > 0:
                records.append({
                    'Datetime': dt,
                    'Open': open_price if open_price > 0 else close_price,
                    'High': high_price if high_price > 0 else close_price,
                    'Low': low_price if low_price > 0 else close_price,
                    'Close': close_price,
                    'Volume': volume
                })
        except Exception as e:
            print(f"Parse error: {e}, item: {item}")
            continue
    
    if not records:
        print(f"No records parsed from {len(output_list)} items")
        return None
    
    print(f"Parsed {len(records)} records successfully")
    df = pd.DataFrame(records)
    df.set_index('Datetime', inplace=True)
    df.sort_index(inplace=True)
    return df

def parse_minute_data_new(output_list):
    """최신 API 형식 분봉 데이터 파싱 (FID_ 파라미터 사용 API)"""
    if not output_list:
        return None
    
    records = []
    cst = pytz.timezone('America/Chicago')
    
    for item in output_list:
        try:
            date_str = item.get('stck_bsop_date', item.get('xymd', ''))
            time_str = item.get('stck_cntg_hour', item.get('xhms', ''))
            
            if not date_str or not time_str:
                date_str = item.get('bsop_date', item.get('bass_dt', ''))
                time_str = item.get('cntg_hour', item.get('hour', ''))
            
            if date_str and time_str:
                if len(time_str) == 6:
                    dt_str = f"{date_str} {time_str}"
                    dt = datetime.strptime(dt_str, "%Y%m%d %H%M%S")
                else:
                    dt_str = f"{date_str} {time_str}00"
                    dt = datetime.strptime(dt_str, "%Y%m%d %H%M%S")
                dt = cst.localize(dt)
                
                open_price = float(item.get('stck_oprc', item.get('open', item.get('oprc', 0))) or 0)
                high_price = float(item.get('stck_hgpr', item.get('high', item.get('hgpr', 0))) or 0)
                low_price = float(item.get('stck_lwpr', item.get('low', item.get('lwpr', 0))) or 0)
                close_price = float(item.get('stck_prpr', item.get('clos', item.get('last', item.get('prpr', 0)))) or 0)
                volume = int(item.get('cntg_vol', item.get('acml_vol', item.get('vol', 0))) or 0)
                
                if close_price > 0:
                    records.append({
                        'Datetime': dt,
                        'Open': open_price if open_price > 0 else close_price,
                        'High': high_price if high_price > 0 else close_price,
                        'Low': low_price if low_price > 0 else close_price,
                        'Close': close_price,
                        'Volume': volume
                    })
        except Exception as e:
            print(f"Parse error: {e}, item: {item}")
            continue
    
    if not records:
        return None
    
    df = pd.DataFrame(records)
    df.set_index('Datetime', inplace=True)
    df.sort_index(inplace=True)
    return df

def parse_daily_data(output_list):
    """일봉 데이터 파싱하여 DataFrame 반환"""
    if not output_list:
        return None
    
    records = []
    cst = pytz.timezone('America/Chicago')
    
    for item in output_list:
        try:
            date_str = item.get('xymd', '')
            
            if date_str:
                dt = datetime.strptime(date_str, "%Y%m%d")
                dt = cst.localize(dt)
                
                records.append({
                    'Datetime': dt,
                    'Open': float(item.get('open', 0)),
                    'High': float(item.get('high', 0)),
                    'Low': float(item.get('low', 0)),
                    'Close': float(item.get('clos', item.get('last', 0))),
                    'Volume': int(item.get('tvol', 0))
                })
        except Exception as e:
            continue
    
    if not records:
        return None
    
    df = pd.DataFrame(records)
    df.set_index('Datetime', inplace=True)
    df.sort_index(inplace=True)
    return df

def test_connection():
    """API 연결 테스트"""
    token = get_access_token()
    if token:
        print("토큰 발급 성공!")
        symbol = get_futures_symbol()
        print(f"현재 나스닥 선물 종목: {symbol}")
        return True
    else:
        print("토큰 발급 실패")
        return False

if __name__ == "__main__":
    if test_connection():
        print("\n1분봉 데이터 조회 중...")
        df = get_futures_minute_data(period="1", count=10)
        if df is not None:
            print(df)
        else:
            print("데이터 조회 실패")
