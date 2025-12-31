-- Migration: Remove table_type column and prepare for new time periods
-- Run this on Railway PostgreSQL database

-- Step 1: Drop old unique constraint from broker_trades
ALTER TABLE broker_trades
    DROP CONSTRAINT IF EXISTS broker_trades_broker_code_symbol_table_type_trade_date_key;

-- Step 2: Remove table_type column from broker_trades
ALTER TABLE broker_trades DROP COLUMN IF EXISTS table_type;

-- Step 3: Add new unique constraint without table_type
-- First check if any duplicates exist (there shouldn't be any)
DO $$
BEGIN
    IF EXISTS (
        SELECT broker_code, symbol, trade_date, COUNT(*)
        FROM broker_trades
        GROUP BY broker_code, symbol, trade_date
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'Duplicate records found! Cannot add unique constraint.';
    END IF;
END $$;

ALTER TABLE broker_trades
    ADD CONSTRAINT broker_trades_broker_symbol_date_key
    UNIQUE(broker_code, symbol, trade_date);

-- Step 4: Truncate aggregate tables (will be recomputed with new periods)
TRUNCATE TABLE aggregates_broker_symbol;
TRUNCATE TABLE aggregates_by_broker;
TRUNCATE TABLE aggregates_by_ticker;
TRUNCATE TABLE daily_insights;

-- Step 5: Remove table_type from aggregates_broker_symbol
ALTER TABLE aggregates_broker_symbol
    DROP CONSTRAINT IF EXISTS aggregates_broker_symbol_broker_code_symbol_table_type_period_key;

ALTER TABLE aggregates_broker_symbol DROP COLUMN IF EXISTS table_type;

ALTER TABLE aggregates_broker_symbol
    ADD CONSTRAINT aggregates_broker_symbol_broker_symbol_period_key
    UNIQUE(broker_code, symbol, period);

-- Done! Now run aggregate recomputation.
