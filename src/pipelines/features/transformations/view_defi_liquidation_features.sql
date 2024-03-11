-- liquidation features
CREATE OR REPLACE VIEW db_analytics_prod.view_defi_liquidation_features AS
WITH liquidation_features AS (
SELECT
    l.account_address,
    COALESCE(COUNT(DISTINCT l.transaction_hash), 0) AS liquidation_total_liquidations,
    cast(to_unixtime(current_timestamp) as decimal) - MAX(l.epoch_timestamp) AS liquidation_time_since_last_liquidated,
    COALESCE(sum(abs(l.quantity_in_eth)), 0) AS liquidation_total_amount_eth
FROM db_analytics_prod.transpose_liquidation_events AS l
GROUP BY l.account_address
)

SELECT
    account_address,
    liquidation_total_liquidations,
    COALESCE(liquidation_total_amount_eth, 0) as liquidation_total_amount_eth,
    COALESCE(liquidation_time_since_last_liquidated, 999999999) as liquidation_time_since_last_liquidated
FROM liquidation_features
