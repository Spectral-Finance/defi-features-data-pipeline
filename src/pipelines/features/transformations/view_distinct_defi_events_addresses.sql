CREATE OR REPLACE VIEW db_analytics_prod.view_distinct_defi_events_addresses AS
WITH addresses AS (
    SELECT sender_address AS wallet_address
    FROM db_analytics_prod.transpose_borrow_events
    UNION ALL
    SELECT account_address AS wallet_address
    FROM db_analytics_prod.transpose_borrow_events
    UNION ALL
    SELECT sender_address AS wallet_address
    FROM db_analytics_prod.transpose_deposit_events
    UNION ALL
    SELECT account_address AS wallet_address
    FROM db_analytics_prod.transpose_deposit_events
    UNION ALL
    SELECT sender_address AS wallet_address
    FROM db_analytics_prod.transpose_withdraw_events
    UNION ALL
    SELECT account_address AS wallet_address
    FROM db_analytics_prod.transpose_withdraw_events
    UNION ALL
    SELECT sender_address AS wallet_address
    FROM db_analytics_prod.transpose_liquidation_events
    UNION ALL
    SELECT account_address AS wallet_address
    FROM db_analytics_prod.transpose_liquidation_events
    UNION ALL
    SELECT sender_address AS wallet_address
    FROM db_analytics_prod.transpose_repay_events
    UNION ALL
    SELECT account_address AS wallet_address
    FROM db_analytics_prod.transpose_repay_events
)
SELECT distinct(wallet_address) as wallet_address FROM addresses