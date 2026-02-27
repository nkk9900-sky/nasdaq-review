-- Supabase SQL Editor에서 이 스크립트를 한 번 실행하세요.
-- (Dashboard → SQL Editor → New query → 붙여넣기 → Run)

-- 거래 저장 테이블
CREATE TABLE IF NOT EXISTS paired_trades (
    id BIGSERIAL PRIMARY KEY,
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
    settlement_date TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paired_trade_date ON paired_trades(trade_date_cst);
CREATE INDEX IF NOT EXISTS idx_paired_symbol ON paired_trades(symbol);
CREATE INDEX IF NOT EXISTS idx_settlement_date ON paired_trades(settlement_date);

-- 캔들 캐시 테이블 (차트 데이터)
CREATE TABLE IF NOT EXISTS candle_cache (
    id BIGSERIAL PRIMARY KEY,
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(trade_date, symbol, timeframe, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_candle_date ON candle_cache(trade_date, symbol, timeframe);

-- RLS 끄기 (앱에서 anon key로 접근하므로, 테이블 접근 허용)
ALTER TABLE paired_trades ENABLE ROW LEVEL SECURITY;
ALTER TABLE candle_cache ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all for paired_trades" ON paired_trades FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for candle_cache" ON candle_cache FOR ALL USING (true) WITH CHECK (true);
