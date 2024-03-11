-- lending_sum_redeems (redeem -> withdraw)
CREATE OR REPLACE VIEW db_analytics_prod.view_defi_lending_sum_redeems_features AS
SELECT
    r.sender_address,
    COALESCE(sum(abs(r.quantity_in_eth)), 0) as lending_sum_redeems
FROM db_analytics_prod.transpose_withdraw_events AS r
GROUP BY r.sender_address
