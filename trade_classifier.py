import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def classify_trade(trade, df_chart, macd_fast=7, macd_slow=20, macd_signal_period=6, dmi_period=14, df_chart_1min=None):
    criteria = {
        'ma33': False,
        'ma5': False,
        'macd': False,
        'dmi': False,
        'time_window': False,
        'stop_loss': False,
    }
    messages = {}

    if df_chart is None or df_chart.empty:
        for k in criteria:
            messages[k] = '데이터 부족'
        return criteria, 0, '판정불가', messages

    trade_type = trade.get('type', '')
    is_sell = '매도' in trade_type
    is_buy = '매수' in trade_type

    entry_time = trade['entry_time_cst']
    entry_price = trade['entry_price']
    exit_price = trade['exit_price']
    profit = trade['profit']

    if hasattr(entry_time, 'tzinfo') and entry_time.tzinfo is not None:
        entry_time_naive = entry_time.replace(tzinfo=None)
    else:
        entry_time_naive = entry_time

    chart_index = df_chart.index
    if hasattr(chart_index, 'tz') and chart_index.tz is not None:
        chart_index_naive = chart_index.tz_localize(None)
    else:
        chart_index_naive = chart_index

    df_work = df_chart.copy()
    df_work.index = chart_index_naive

    idx_pos = df_work.index.searchsorted(entry_time_naive, side='right') - 1
    if idx_pos < 0:
        idx_pos = 0

    if idx_pos >= len(df_work):
        idx_pos = len(df_work) - 1

    matched_time = df_work.index[idx_pos]
    time_diff = abs((entry_time_naive - matched_time).total_seconds())
    if time_diff > 300:
        if idx_pos + 1 < len(df_work):
            next_time = df_work.index[idx_pos + 1]
            next_diff = abs((entry_time_naive - next_time).total_seconds())
            if next_diff < time_diff:
                idx_pos = idx_pos + 1

    if idx_pos < 33:
        for k in criteria:
            if k not in ('time_window', 'stop_loss'):
                messages[k] = '데이터 부족'
    else:
        close_series = df_work['Close']
        ma5 = close_series.rolling(window=5).mean()
        ma33 = close_series.rolling(window=33).mean()

        ma5_at_entry = ma5.iloc[idx_pos] if idx_pos < len(ma5) and pd.notna(ma5.iloc[idx_pos]) else None
        ma33_at_entry = ma33.iloc[idx_pos] if idx_pos < len(ma33) and pd.notna(ma33.iloc[idx_pos]) else None
        close_at_entry = close_series.iloc[idx_pos]

        if ma33_at_entry is not None:
            if is_buy and entry_price > ma33_at_entry:
                criteria['ma33'] = True
                messages['ma33'] = f'33선 위({entry_price:.1f}>{ma33_at_entry:.1f})'
            elif is_sell and entry_price < ma33_at_entry:
                criteria['ma33'] = True
                messages['ma33'] = f'33선 아래({entry_price:.1f}<{ma33_at_entry:.1f})'
            else:
                if is_buy:
                    messages['ma33'] = f'33선 아래({entry_price:.1f}<{ma33_at_entry:.1f})'
                else:
                    messages['ma33'] = f'33선 위({entry_price:.1f}>{ma33_at_entry:.1f})'
        else:
            messages['ma33'] = '데이터 부족'

        # 5선 안착: 1분봉 기준 (df_chart_1min 있으면), 없으면 기존 df_chart(3분봉) 기준
        df_5 = df_chart_1min if (df_chart_1min is not None and not df_chart_1min.empty) else df_work
        chart_index_5 = df_5.index
        if hasattr(chart_index_5, 'tz') and chart_index_5.tz is not None:
            chart_index_5_naive = chart_index_5.tz_localize(None)
        else:
            chart_index_5_naive = chart_index_5
        df_5_work = df_5.copy()
        df_5_work.index = chart_index_5_naive
        idx_pos_5 = df_5_work.index.searchsorted(entry_time_naive, side='right') - 1
        if idx_pos_5 < 0:
            idx_pos_5 = 0
        if idx_pos_5 >= len(df_5_work):
            idx_pos_5 = len(df_5_work) - 1
        close_series_5 = df_5_work['Close']
        ma5_series = close_series_5.rolling(window=5).mean()
        ma5_at_entry = ma5_series.iloc[idx_pos_5] if idx_pos_5 < len(ma5_series) and pd.notna(ma5_series.iloc[idx_pos_5]) else None
        close_at_entry_5 = close_series_5.iloc[idx_pos_5]

        if ma5_at_entry is not None and idx_pos_5 >= 3:
            lookback = min(3, idx_pos_5)
            had_cross = False
            for b in range(1, lookback + 1):
                prev_c = close_series_5.iloc[idx_pos_5 - b] if pd.notna(close_series_5.iloc[idx_pos_5 - b]) else None
                prev_m = ma5_series.iloc[idx_pos_5 - b] if pd.notna(ma5_series.iloc[idx_pos_5 - b]) else None
                if prev_c is not None and prev_m is not None:
                    if is_buy and prev_c <= prev_m:
                        had_cross = True
                        break
                    elif is_sell and prev_c >= prev_m:
                        had_cross = True
                        break

            if is_buy:
                if close_at_entry_5 > ma5_at_entry:
                    if had_cross:
                        criteria['ma5'] = True
                        messages['ma5'] = f'5선 상향돌파 안착({close_at_entry_5:.1f}>{ma5_at_entry:.1f}) [1분봉]'
                    else:
                        criteria['ma5'] = True
                        messages['ma5'] = f'5선 위 안착({close_at_entry_5:.1f}>{ma5_at_entry:.1f}) [1분봉]'
                else:
                    messages['ma5'] = f'5선 하락({close_at_entry_5:.1f}<{ma5_at_entry:.1f}) [1분봉]'
            elif is_sell:
                if close_at_entry_5 < ma5_at_entry:
                    if had_cross:
                        criteria['ma5'] = True
                        messages['ma5'] = f'5선 하향돌파 안착({close_at_entry_5:.1f}<{ma5_at_entry:.1f}) [1분봉]'
                    else:
                        criteria['ma5'] = True
                        messages['ma5'] = f'5선 아래 안착({close_at_entry_5:.1f}<{ma5_at_entry:.1f}) [1분봉]'
                else:
                    messages['ma5'] = f'5선 상승({close_at_entry_5:.1f}>{ma5_at_entry:.1f}) [1분봉]'
        else:
            messages['ma5'] = '데이터 부족'

        ema_fast = close_series.ewm(span=macd_fast, adjust=False).mean()
        ema_slow_line = close_series.ewm(span=macd_slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow_line
        signal_line = macd_line.ewm(span=macd_signal_period, adjust=False).mean()

        macd_at_entry = macd_line.iloc[idx_pos] if pd.notna(macd_line.iloc[idx_pos]) else None
        signal_at_entry = signal_line.iloc[idx_pos] if pd.notna(signal_line.iloc[idx_pos]) else None
        macd_prev = macd_line.iloc[idx_pos - 1] if idx_pos > 0 and pd.notna(macd_line.iloc[idx_pos - 1]) else None
        signal_prev = signal_line.iloc[idx_pos - 1] if idx_pos > 0 and pd.notna(signal_line.iloc[idx_pos - 1]) else None

        macd_cross_bar = None

        if macd_at_entry is not None and signal_at_entry is not None:
            if is_buy:
                if macd_at_entry > signal_at_entry:
                    criteria['macd'] = True
                    messages['macd'] = f'MACD 골든크로스({macd_at_entry:.2f}>{signal_at_entry:.2f})'
                    if macd_prev is not None and signal_prev is not None and macd_prev <= signal_prev:
                        macd_cross_bar = idx_pos
                    elif idx_pos >= 2:
                        m2 = macd_line.iloc[idx_pos - 2] if pd.notna(macd_line.iloc[idx_pos - 2]) else None
                        s2 = signal_line.iloc[idx_pos - 2] if pd.notna(signal_line.iloc[idx_pos - 2]) else None
                        if m2 is not None and s2 is not None and m2 <= s2:
                            macd_cross_bar = idx_pos - 1
                else:
                    messages['macd'] = f'MACD 하락({macd_at_entry:.2f}<{signal_at_entry:.2f})'
            elif is_sell:
                if macd_at_entry < signal_at_entry:
                    criteria['macd'] = True
                    messages['macd'] = f'MACD 데드크로스({macd_at_entry:.2f}<{signal_at_entry:.2f})'
                    if macd_prev is not None and signal_prev is not None and macd_prev >= signal_prev:
                        macd_cross_bar = idx_pos
                    elif idx_pos >= 2:
                        m2 = macd_line.iloc[idx_pos - 2] if pd.notna(macd_line.iloc[idx_pos - 2]) else None
                        s2 = signal_line.iloc[idx_pos - 2] if pd.notna(signal_line.iloc[idx_pos - 2]) else None
                        if m2 is not None and s2 is not None and m2 >= s2:
                            macd_cross_bar = idx_pos - 1
                else:
                    messages['macd'] = f'MACD 상승({macd_at_entry:.2f}>{signal_at_entry:.2f})'
        else:
            messages['macd'] = '데이터 부족'

        high = df_work['High']
        low = df_work['Low']
        close = df_work['Close']

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

        plus_di_at = plus_di.iloc[idx_pos] if pd.notna(plus_di.iloc[idx_pos]) else None
        minus_di_at = minus_di.iloc[idx_pos] if pd.notna(minus_di.iloc[idx_pos]) else None
        plus_di_prev = plus_di.iloc[idx_pos - 1] if idx_pos > 0 and pd.notna(plus_di.iloc[idx_pos - 1]) else None
        minus_di_prev = minus_di.iloc[idx_pos - 1] if idx_pos > 0 and pd.notna(minus_di.iloc[idx_pos - 1]) else None

        if plus_di_at is not None and minus_di_at is not None:
            dmi_cross_bar = None
            if is_buy:
                if plus_di_at > minus_di_at and max(plus_di_at, minus_di_at) >= 20:
                    criteria['dmi'] = True
                    messages['dmi'] = f'DI+ 우위({plus_di_at:.1f}>{minus_di_at:.1f})'
                    if plus_di_prev is not None and minus_di_prev is not None and plus_di_prev <= minus_di_prev:
                        dmi_cross_bar = idx_pos
                    elif idx_pos >= 2:
                        p2 = plus_di.iloc[idx_pos - 2] if pd.notna(plus_di.iloc[idx_pos - 2]) else None
                        m2 = minus_di.iloc[idx_pos - 2] if pd.notna(minus_di.iloc[idx_pos - 2]) else None
                        if p2 is not None and m2 is not None and p2 <= m2:
                            dmi_cross_bar = idx_pos - 1
                else:
                    if plus_di_at <= minus_di_at:
                        messages['dmi'] = f'DI- 우위({minus_di_at:.1f}>{plus_di_at:.1f})'
                    else:
                        messages['dmi'] = f'20선 미달(max {max(plus_di_at, minus_di_at):.1f})'
            elif is_sell:
                if minus_di_at > plus_di_at and max(plus_di_at, minus_di_at) >= 20:
                    criteria['dmi'] = True
                    messages['dmi'] = f'DI- 우위({minus_di_at:.1f}>{plus_di_at:.1f})'
                    if minus_di_prev is not None and plus_di_prev is not None and minus_di_prev <= plus_di_prev:
                        dmi_cross_bar = idx_pos
                    elif idx_pos >= 2:
                        p2 = plus_di.iloc[idx_pos - 2] if pd.notna(plus_di.iloc[idx_pos - 2]) else None
                        m2 = minus_di.iloc[idx_pos - 2] if pd.notna(minus_di.iloc[idx_pos - 2]) else None
                        if p2 is not None and m2 is not None and m2 <= p2:
                            dmi_cross_bar = idx_pos - 1
                else:
                    if minus_di_at <= plus_di_at:
                        messages['dmi'] = f'DI+ 우위({plus_di_at:.1f}>{minus_di_at:.1f})'
                    else:
                        messages['dmi'] = f'20선 미달(max {max(plus_di_at, minus_di_at):.1f})'

            if criteria['dmi'] and criteria['macd'] and macd_cross_bar is not None and dmi_cross_bar is not None:
                bar_diff = abs(macd_cross_bar - dmi_cross_bar)
                if bar_diff > 1:
                    criteria['dmi'] = False
                    messages['dmi'] += f' (MACD와 {bar_diff}봉 차이)'
        else:
            messages['dmi'] = '데이터 부족'

    entry_time_kst = trade.get('entry_time_kst', None)
    if entry_time_kst is not None:
        if hasattr(entry_time_kst, 'hour'):
            kst_hour = entry_time_kst.hour
            kst_minute = entry_time_kst.minute
        else:
            kst_hour = entry_time_naive.hour + 15
            if kst_hour >= 24:
                kst_hour -= 24
            kst_minute = entry_time_naive.minute
    else:
        kst_hour = entry_time_naive.hour + 15
        if kst_hour >= 24:
            kst_hour -= 24
        kst_minute = entry_time_naive.minute

    kst_val = kst_hour * 60 + kst_minute

    kst_window1_start = 8 * 60 + 55
    kst_window1_end = 10 * 60
    kst_window2_start = 23 * 60 + 30
    kst_window2_end = 6 * 60

    if (kst_window1_start <= kst_val <= kst_window1_end) or (kst_val >= kst_window2_start) or (kst_val <= kst_window2_end):
        criteria['time_window'] = True
        if kst_window1_start <= kst_val <= kst_window1_end:
            messages['time_window'] = f'유럽장(KST {kst_hour:02d}:{kst_minute:02d})'
        else:
            messages['time_window'] = f'미국장(KST {kst_hour:02d}:{kst_minute:02d})'
    else:
        cst_hour = entry_time_naive.hour
        cst_minute = entry_time_naive.minute
        messages['time_window'] = f'KST {kst_hour:02d}:{kst_minute:02d} (CST {cst_hour:02d}:{cst_minute:02d}) 비적정시간'

    if profit <= 0:
        tick_size = 0.25
        ticks = abs(entry_price - exit_price) / tick_size
        if ticks <= 60:
            criteria['stop_loss'] = True
            messages['stop_loss'] = f'{ticks:.0f}틱 손절(준수)'
        else:
            messages['stop_loss'] = f'{ticks:.0f}틱 손절(초과)'
    else:
        criteria['stop_loss'] = True
        messages['stop_loss'] = '수익 청산'

    score = sum(1 for v in criteria.values() if v)

    if score >= 5:
        classification = '원칙'
    elif score >= 3:
        classification = '운빨'
    else:
        classification = '뇌동'

    return criteria, score, classification, messages


def classify_all_trades(trades, df_chart, macd_fast=7, macd_slow=20, macd_signal_period=6, dmi_period=14):
    # 33선·MACD·DMI: 3분봉 기준 / 5선 안착: 1분봉 기준
    df_1min = df_chart if (df_chart is not None and not df_chart.empty) else None
    df_3min = None
    if df_chart is not None and not df_chart.empty:
        df_3min = df_chart.resample('3min').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()

    results = []
    for trade in trades:
        criteria, score, classification, messages = classify_trade(
            trade, df_3min, macd_fast, macd_slow, macd_signal_period, dmi_period, df_chart_1min=df_1min
        )
        results.append({
            'criteria': criteria,
            'score': score,
            'classification': classification,
            'messages': messages
        })
    return results


def get_statistics(trades, classifications):
    total = len(trades)
    if total == 0:
        return {
            'principle_rate': 0,
            'principle_win_rate': 0,
            'impulse_loss': 0,
        }

    principle_count = sum(1 for c in classifications if c['classification'] == '원칙')
    principle_wins = sum(1 for i, c in enumerate(classifications)
                        if c['classification'] == '원칙' and trades[i]['profit'] > 0)
    impulse_loss = sum(trades[i]['profit'] for i, c in enumerate(classifications)
                       if c['classification'] == '뇌동' and trades[i]['profit'] < 0)

    principle_rate = (principle_count / total * 100) if total > 0 else 0
    principle_win_rate = (principle_wins / principle_count * 100) if principle_count > 0 else 0

    return {
        'principle_rate': principle_rate,
        'principle_win_rate': principle_win_rate,
        'impulse_loss': impulse_loss,
    }
