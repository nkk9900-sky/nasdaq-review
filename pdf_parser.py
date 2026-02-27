import pdfplumber
import re
from datetime import datetime
import pandas as pd

def parse_trade_pdf(pdf_file):
    trades = []
    
    try:
        with pdfplumber.open(pdf_file) as pdf:
            tables = []
            for page in pdf.pages:
                extracted_tables = page.extract_tables()
                if extracted_tables:
                    for table in extracted_tables:
                        tables.extend(table)
            
            if tables:
                trades = parse_table_data(tables)
                if trades:
                    return trades
            
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"
            
            if full_text.strip():
                trades = parse_text_content(full_text)
    
    except Exception as e:
        print(f"PDF parsing error: {e}")
        raise
    
    return trades

def parse_table_data(tables):
    trades = []
    header_found = False
    
    for row in tables:
        if row is None:
            continue
        
        row = [str(cell).strip() if cell else '' for cell in row]
        
        if '주문번호' in row or '체결번호' in row:
            header_found = True
            continue
        
        if '약정금액' in row:
            break
        
        if not header_found:
            continue
        
        trade = parse_table_row(row)
        if trade:
            trades.append(trade)
    
    return trades

def parse_table_row(row):
    try:
        order_num = None
        exec_num = None
        symbol = None
        trade_type = None
        price = None
        quantity = None
        trade_datetime = None
        
        for cell in row:
            if not cell:
                continue
            
            if re.match(r'^\d{9}$', cell) and not order_num:
                order_num = cell
            elif re.match(r'^\d{8}$', cell) and not exec_num:
                exec_num = cell
            elif re.match(r'^(MNQ|NQ)[A-Z]\d{2}$', cell):
                symbol = cell
            elif cell in ['매수', '매도']:
                trade_type = cell
            elif re.match(r'^\d{5,}(\.\d+)?$', cell):
                val = float(cell)
                if 20000 < val < 50000:
                    price = val
            elif re.match(r'^[1-9]\d*$', cell):
                val = int(cell)
                if 1 <= val <= 100:
                    quantity = val
            elif re.match(r'\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}', cell):
                parts = cell.split()
                if len(parts) >= 2:
                    date_str = parts[0]
                    time_str = parts[1]
                    trade_datetime = f"20{date_str.replace('/', '-')} {time_str}"
        
        if all([order_num, symbol, trade_type, price, quantity]):
            return {
                'order_number': order_num or '',
                'execution_number': exec_num or '',
                'symbol': symbol,
                'trade_type': trade_type,
                'execution_price': price,
                'quantity': quantity,
                'trade_datetime': trade_datetime or ''
            }
    except Exception:
        pass
    
    return None

def parse_text_content(text_content):
    trades = []
    lines = text_content.strip().split('\n')
    
    trade_entries = []
    datetime_entries = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('Version=') or '주문번호' in line or '약정금액' in line:
            continue
        
        datetime_match = re.search(r'(\d{2}/\d{2}/\d{2})\s+(\d{2}:\d{2})', line)
        if datetime_match:
            datetime_entries.append({
                'date': datetime_match.group(1),
                'time': datetime_match.group(2),
                'line': line
            })
        
        trade_match = re.match(r'^\s*(\d{9})\s+(\d{8})\s+((?:MNQ|NQ)[A-Z]\d{2})\s+(매수|매도)', line)
        if trade_match:
            trade_entries.append({
                'order_number': trade_match.group(1),
                'execution_number': trade_match.group(2),
                'symbol': trade_match.group(3),
                'trade_type': trade_match.group(4),
                'line': line
            })
    
    for i, entry in enumerate(trade_entries):
        line = entry['line']
        
        price_matches = re.findall(r'(\d{5}(?:\.\d+)?)', line)
        price = None
        for p in price_matches:
            val = float(p)
            if 20000 < val < 50000:
                price = val
                break
        
        quantity_match = re.search(r'(?:' + re.escape(str(price) if price else '') + r')\s+(\d+)\s', line)
        quantity = 1
        if quantity_match:
            quantity = int(quantity_match.group(1))
        else:
            parts = line.split()
            for j, p in enumerate(parts):
                try:
                    if float(p) == price and j + 1 < len(parts):
                        quantity = int(parts[j + 1])
                        break
                except:
                    continue
        
        trade_datetime = ""
        if i < len(datetime_entries):
            dt = datetime_entries[i]
            trade_datetime = f"20{dt['date'].replace('/', '-')} {dt['time']}"
        
        if price:
            trades.append({
                'order_number': entry['order_number'],
                'execution_number': entry['execution_number'],
                'symbol': entry['symbol'],
                'trade_type': entry['trade_type'],
                'execution_price': price,
                'quantity': quantity,
                'trade_datetime': trade_datetime
            })
    
    return trades

def validate_trade(trade):
    required_fields = ['symbol', 'trade_type', 'execution_price', 'quantity']
    for field in required_fields:
        if not trade.get(field):
            return False
    
    if trade['trade_type'] not in ['매수', '매도']:
        return False
    
    if not (20000 < trade['execution_price'] < 50000):
        return False
    
    if not (1 <= trade['quantity'] <= 100):
        return False
    
    return True
