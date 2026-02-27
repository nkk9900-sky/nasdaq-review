import pandas as pd
from datetime import datetime, timedelta
import pytz

def parse_execution_file(file_path):
    """체결내역 파일 파싱"""
    df = pd.read_excel(file_path, header=1)
    
    trades = []
    for _, row in df.iterrows():
        try:
            time_str = str(row['체결일시']).strip()
            if pd.isna(time_str) or time_str == 'nan':
                continue
            
            if '/' in time_str:
                dt = datetime.strptime(time_str, "%y/%m/%d %H:%M")
            else:
                dt = pd.to_datetime(time_str)
            
            price = float(row['체결가'])
            quantity = int(row['체결량'])
            trade_type = str(row['구분']).strip()
            order_no = str(row['주문번호'])
            exec_no = str(row['체결번호'])
            
            trades.append({
                'datetime': dt,
                'price': price,
                'quantity': quantity,
                'type': trade_type,
                'order_no': order_no,
                'exec_no': exec_no
            })
        except Exception as e:
            continue
    
    return trades

def parse_closing_file(file_path):
    """청산내역 파일 파싱"""
    df = pd.read_excel(file_path, header=1)
    
    closings = []
    for _, row in df.iterrows():
        try:
            time_str = str(row['청산체결시간']).strip()
            if pd.isna(time_str) or time_str == 'nan':
                continue
            
            if '/' in time_str:
                dt = datetime.strptime(time_str, "%Y/%m/%d %H:%M:%S")
            else:
                dt = pd.to_datetime(time_str)
            
            entry_price = float(row['매입가격'])
            exit_price = float(row['청산가격'])
            quantity = int(row['수량'])
            profit = float(row['순손익'])
            trade_type = str(row['구분']).strip()
            
            closings.append({
                'closing_time': dt,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'quantity': quantity,
                'profit': profit,
                'type': trade_type
            })
        except Exception as e:
            continue
    
    return closings

def match_trades(executions, closings):
    """체결내역과 청산내역을 매칭하여 진입-청산 쌍 생성"""
    matched_trades = []
    
    used_executions = set()
    
    for closing in closings:
        closing_time = closing['closing_time']
        
        best_entry = None
        best_entry_idx = None
        min_time_diff = timedelta(hours=24)
        
        for idx, exec_trade in enumerate(executions):
            if idx in used_executions:
                continue
            
            if closing['type'] == '매도':
                if exec_trade['type'] != '매수':
                    continue
            elif closing['type'] == '매수':
                if exec_trade['type'] != '매도':
                    continue
            
            if exec_trade['quantity'] < closing['quantity']:
                continue
            
            time_diff = abs(closing_time - exec_trade['datetime'])
            
            if exec_trade['datetime'] < closing_time and time_diff < min_time_diff:
                if abs(exec_trade['price'] - closing['entry_price']) < 1:
                    min_time_diff = time_diff
                    best_entry = exec_trade
                    best_entry_idx = idx
        
        if best_entry is None:
            for idx, exec_trade in enumerate(executions):
                if idx in used_executions:
                    continue
                
                if abs(exec_trade['price'] - closing['entry_price']) < 1:
                    if exec_trade['datetime'] < closing_time:
                        best_entry = exec_trade
                        best_entry_idx = idx
                        break
        
        if best_entry:
            used_executions.add(best_entry_idx)
            matched_trades.append({
                'entry_time': best_entry['datetime'],
                'entry_price': closing['entry_price'],
                'exit_time': closing_time,
                'exit_price': closing['exit_price'],
                'quantity': closing['quantity'],
                'profit': closing['profit'],
                'type': closing['type']
            })
        else:
            entry_time = closing_time - timedelta(minutes=5)
            matched_trades.append({
                'entry_time': entry_time,
                'entry_price': closing['entry_price'],
                'exit_time': closing_time,
                'exit_price': closing['exit_price'],
                'quantity': closing['quantity'],
                'profit': closing['profit'],
                'type': closing['type']
            })
    
    return matched_trades

def convert_to_cst(dt):
    """한국시간(KST)을 시카고시간(CST)으로 변환 - 15시간 빼기"""
    if dt.tzinfo is None:
        return dt - timedelta(hours=15)
    return dt

def get_matched_trades_from_files(exec_file_path, closing_file_path):
    """두 파일에서 매칭된 거래 데이터 반환 (KST와 CST 모두 보존)"""
    executions = parse_execution_file(exec_file_path)
    closings = parse_closing_file(closing_file_path)
    
    for exec_trade in executions:
        exec_trade['datetime_kst'] = exec_trade['datetime']
        exec_trade['datetime_cst'] = convert_to_cst(exec_trade['datetime'])
    
    for closing in closings:
        closing['closing_time_kst'] = closing['closing_time']
        closing['closing_time_cst'] = convert_to_cst(closing['closing_time'])
    
    matched = match_trades(executions, closings)
    
    for trade in matched:
        trade['entry_time_kst'] = trade['entry_time']
        trade['exit_time_kst'] = trade['exit_time']
        trade['entry_time_cst'] = convert_to_cst(trade['entry_time']) if trade['entry_time'].tzinfo is None else trade['entry_time']
        trade['exit_time_cst'] = convert_to_cst(trade['exit_time']) if trade['exit_time'].tzinfo is None else trade['exit_time']
        trade['entry_time'] = trade['entry_time_cst']
        trade['exit_time'] = trade['exit_time_cst']
    
    return matched
