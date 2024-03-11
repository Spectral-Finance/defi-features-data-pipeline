-- lending_time_and_count_features (deposits)
CREATE OR REPLACE VIEW db_analytics_prod.view_defi_lending_time_and_count_features AS
SELECT
    d.sender_address,
    COALESCE(COUNT(DISTINCT d.transaction_hash), 0) AS lending_total_deposits,
    cast(to_unixtime(current_timestamp) as decimal) - min(d.epoch_timestamp) AS lending_time_since_first_lending,
    COALESCE(sum(abs(d.quantity_in_eth)), 0) as total_deposits_in_eth
FROM db_analytics_prod.transpose_deposit_events AS d
GROUP BY d.sender_address
