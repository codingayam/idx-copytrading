-- IDX Copytrading System - PostgreSQL Database Schema
-- Version 1.0

-- ============================================
-- 1. Core broker reference data
-- ============================================
CREATE TABLE IF NOT EXISTS brokers (
    id SERIAL PRIMARY KEY,
    code VARCHAR(4) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 2. Symbol/ticker reference data (auto-populated from crawl)
-- ============================================
CREATE TABLE IF NOT EXISTS symbols (
    symbol VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(200),
    sector VARCHAR(100),
    first_seen DATE,
    last_seen DATE,
    is_active BOOLEAN DEFAULT true
);

-- ============================================
-- 3. Daily raw trading data (one row per broker-ticker-type per day)
-- ============================================
CREATE TABLE IF NOT EXISTS broker_trades (
    id SERIAL PRIMARY KEY,
    broker_code VARCHAR(4) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    table_type VARCHAR(4) NOT NULL,          -- "buy" or "sell"
    trade_date DATE NOT NULL,
    netval DECIMAL(15,4) DEFAULT 0,          -- net value in milyar Rp
    bval DECIMAL(15,4) DEFAULT 0,            -- buy value in milyar Rp
    sval DECIMAL(15,4) DEFAULT 0,            -- sell value in milyar Rp
    bavg DECIMAL(15,4) DEFAULT 0,            -- average buy price
    savg DECIMAL(15,4) DEFAULT 0,            -- average sell price
    crawl_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(broker_code, symbol, table_type, trade_date)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_broker_trades_date ON broker_trades(trade_date);
CREATE INDEX IF NOT EXISTS idx_broker_trades_broker ON broker_trades(broker_code);
CREATE INDEX IF NOT EXISTS idx_broker_trades_symbol ON broker_trades(symbol);
CREATE INDEX IF NOT EXISTS idx_broker_trades_broker_date ON broker_trades(broker_code, trade_date);
CREATE INDEX IF NOT EXISTS idx_broker_trades_symbol_date ON broker_trades(symbol, trade_date);
-- Composite index for filtering + sorting (most common UI query)
CREATE INDEX IF NOT EXISTS idx_broker_trades_broker_date_netval 
    ON broker_trades(broker_code, trade_date DESC, netval DESC);

-- ============================================
-- 4. Aggregates by broker (all symbols combined)
-- ============================================
CREATE TABLE IF NOT EXISTS aggregates_by_broker (
    id SERIAL PRIMARY KEY,
    broker_code VARCHAR(4) NOT NULL,
    period VARCHAR(20) NOT NULL,             -- "today", "week", "month", "ytd", "all"
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    total_netval DECIMAL(18,4) DEFAULT 0,
    total_bval DECIMAL(18,4) DEFAULT 0,
    total_sval DECIMAL(18,4) DEFAULT 0,
    weighted_bavg DECIMAL(15,4) DEFAULT 0,
    weighted_savg DECIMAL(15,4) DEFAULT 0,
    trade_count INTEGER DEFAULT 0,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(broker_code, period)
);

-- ============================================
-- 5. Aggregates by ticker (all brokers combined)
-- ============================================
CREATE TABLE IF NOT EXISTS aggregates_by_ticker (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    period VARCHAR(20) NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    total_netval DECIMAL(18,4) DEFAULT 0,
    total_bval DECIMAL(18,4) DEFAULT 0,
    total_sval DECIMAL(18,4) DEFAULT 0,
    weighted_bavg DECIMAL(15,4) DEFAULT 0,
    weighted_savg DECIMAL(15,4) DEFAULT 0,
    trade_count INTEGER DEFAULT 0,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol, period)
);

-- ============================================
-- 6. Aggregates broker-symbol cross-reference (for drill-down views)
--    "Broker AD's positions in BBCA" or "All brokers trading BBCA"
-- ============================================
CREATE TABLE IF NOT EXISTS aggregates_broker_symbol (
    id SERIAL PRIMARY KEY,
    broker_code VARCHAR(4) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    table_type VARCHAR(4) NOT NULL,
    period VARCHAR(20) NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    netval_sum DECIMAL(18,4) DEFAULT 0,
    bval_sum DECIMAL(18,4) DEFAULT 0,
    sval_sum DECIMAL(18,4) DEFAULT 0,
    weighted_bavg DECIMAL(15,4) DEFAULT 0,
    weighted_savg DECIMAL(15,4) DEFAULT 0,
    pct_of_symbol_volume DECIMAL(8,4),       -- % contribution to symbol's total
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(broker_code, symbol, table_type, period)
);

CREATE INDEX IF NOT EXISTS idx_agg_broker_symbol_broker ON aggregates_broker_symbol(broker_code, period);
CREATE INDEX IF NOT EXISTS idx_agg_broker_symbol_symbol ON aggregates_broker_symbol(symbol, period);

-- ============================================
-- 7. Daily market totals (denominator for % calculations)
-- ============================================
CREATE TABLE IF NOT EXISTS daily_totals (
    trade_date DATE PRIMARY KEY,
    total_market_bval DECIMAL(18,4),
    total_market_sval DECIMAL(18,4),
    total_market_volume DECIMAL(18,4),
    active_symbols INTEGER,
    active_brokers INTEGER,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 8. Insights tab: top movers by netval
-- ============================================
CREATE TABLE IF NOT EXISTS daily_insights (
    id SERIAL PRIMARY KEY,
    insight_date DATE NOT NULL,
    insight_type VARCHAR(50) NOT NULL,       -- "top_netval_5d", "top_netval_month"
    symbol VARCHAR(10) NOT NULL,
    broker_code VARCHAR(4) NOT NULL,
    netval DECIMAL(15,4),
    bval DECIMAL(15,4),
    sval DECIMAL(15,4),
    bavg DECIMAL(15,4),
    savg DECIMAL(15,4),
    rank INTEGER,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(insight_date, insight_type, rank)
);

-- ============================================
-- 9. Track crawl history
-- ============================================
CREATE TABLE IF NOT EXISTS crawl_log (
    id SERIAL PRIMARY KEY,
    crawl_date DATE NOT NULL,
    crawl_start TIMESTAMP NOT NULL,
    crawl_end TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running',    -- running, success, failed
    total_rows INTEGER,
    successful_brokers INTEGER,
    failed_brokers INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Seed broker data
-- ============================================
INSERT INTO brokers (code, name) VALUES
    ('AD', 'OSO Sekuritas Indonesia'),
    ('AF', 'Harita Kencana Sekuritas'),
    ('AG', 'Kiwoom Sekuritas Indonesia'),
    ('AH', 'Shinhan Sekuritas Indonesia'),
    ('AI', 'UOB Kay Hian Sekuritas'),
    ('AK', 'UBS Sekuritas Indonesia'),
    ('AN', 'Wanteg Sekuritas'),
    ('AO', 'ERDIKHA ELIT SEKURITAS'),
    ('AP', 'Pacific Sekuritas Indonesia'),
    ('AR', 'Binaartha Sekuritas'),
    ('AT', 'Phintraco Sekuritas'),
    ('AZ', 'Sucor Sekuritas'),
    ('BB', 'Verdhana Sekuritas Indonesia'),
    ('BF', 'Inti Fikasa Sekuritas'),
    ('BK', 'J.P. Morgan Sekuritas Indonesia'),
    ('BQ', 'Korea Investment and Sekuritas Indonesia'),
    ('BR', 'Trust Sekuritas'),
    ('BS', 'Equity Sekuritas Indonesia'),
    ('CC', 'MANDIRI SEKURITAS'),
    ('CD', 'Mega Capital Sekuritas'),
    ('CP', 'KB Valbury Sekuritas'),
    ('DD', 'Makindo Sekuritas'),
    ('DH', 'SINARMAS SEKURITAS'),
    ('DP', 'DBS Vickers Sekuritas Indonesia'),
    ('DR', 'RHB Sekuritas Indonesia'),
    ('DU', 'KAF Sekuritas Indonesia'),
    ('DX', 'Bahana Sekuritas'),
    ('EL', 'Evergreen Sekuritas Indonesia'),
    ('EP', 'MNC Sekuritas'),
    ('ES', 'EKOKAPITAL SEKURITAS'),
    ('FO', 'Forte Global Sekuritas'),
    ('FS', 'Yuanta Sekuritas Indonesia'),
    ('FZ', 'Waterfront Sekuritas Indonesia'),
    ('GA', 'BNC Sekuritas Indonesia'),
    ('GI', 'Webull Sekuritas Indonesia'),
    ('GR', 'PANIN SEKURITAS Tbk.'),
    ('GW', 'HSBC Sekuritas Indonesia'),
    ('HD', 'KGI Sekuritas Indonesia'),
    ('HP', 'Henan Putihrai Sekuritas'),
    ('IC', 'Integrity Capital Sekuritas'),
    ('ID', 'Anugerah Sekuritas Indonesia'),
    ('IF', 'SAMUEL SEKURITAS INDONESIA'),
    ('IH', 'Indo Harvest Sekuritas'),
    ('II', 'Danatama Makmur Sekuritas'),
    ('IN', 'INVESTINDO NUSANTARA SEKURITA'),
    ('IT', 'INTI TELADAN SEKURITAS'),
    ('IU', 'Indo Capital Sekuritas'),
    ('KI', 'Ciptadana Sekuritas Asia'),
    ('KK', 'Phillip Sekuritas Indonesia'),
    ('KZ', 'CLSA Sekuritas Indonesia'),
    ('LG', 'Trimegah Sekuritas Indonesia Tbk.'),
    ('LS', 'Reliance Sekuritas Indonesia Tbk.'),
    ('MG', 'Semesta Indovest Sekuritas'),
    ('MI', 'Victoria Sekuritas Indonesia'),
    ('MU', 'Minna Padi Investama Sekuritas'),
    ('NI', 'BNI Sekuritas'),
    ('OD', 'BRI Danareksa Sekuritas'),
    ('OK', 'NET SEKURITAS'),
    ('PC', 'FAC Sekuritas Indonesia'),
    ('PD', 'Indo Premier Sekuritas'),
    ('PF', 'Danasakti Sekuritas Indonesia'),
    ('PG', 'Panca Global Sekuritas'),
    ('PI', 'Magenta Kapital Sekuritas Indonesia'),
    ('PO', 'Pilarmas Investindo Sekuritas'),
    ('PP', 'Aldiracita Sekuritas Indonesia'),
    ('QA', 'Tuntun Sekuritas Indonesia'),
    ('RB', 'Ina Sekuritas Indonesia'),
    ('RF', 'Buana Capital Sekuritas'),
    ('RG', 'Profindo Sekuritas Indonesia'),
    ('RO', 'Pluang Maju Sekuritas'),
    ('RS', 'Yulie Sekuritas Indonesia Tbk.'),
    ('RX', 'Macquarie Sekuritas Indonesia'),
    ('SA', 'Elit Sukses Sekuritas'),
    ('SF', 'Surya Fajar Sekuritas'),
    ('SH', 'Artha Sekuritas Indonesia'),
    ('SQ', 'BCA Sekuritas'),
    ('SS', 'Supra Sekuritas Indonesia'),
    ('TF', 'Laba Sekuritas Indonesia'),
    ('TP', 'OCBC Sekuritas Indonesia'),
    ('TS', 'Dwidana Sakti Sekuritas'),
    ('XA', 'NH Korindo Sekuritas Indonesia'),
    ('XC', 'Ajaib Sekuritas Asia'),
    ('XL', 'Stockbit Sekuritas Digital'),
    ('YB', 'Yakin Bertumbuh Sekuritas'),
    ('YJ', 'Lotus Andalan Sekuritas'),
    ('YO', 'Amantara Sekuritas Indonesia'),
    ('YP', 'Mirae Asset Sekuritas Indonesia'),
    ('YU', 'CGS International Sekuritas Indonesia'),
    ('ZP', 'Maybank Sekuritas Indonesia'),
    ('ZR', 'Bumiputera Sekuritas')
ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name, updated_at = CURRENT_TIMESTAMP;
