-- repay_features
CREATE OR REPLACE VIEW db_analytics_prod.view_defi_repay_features AS
SELECT
    r.sender_address,
    COALESCE(sum(abs(r.quantity_in_eth)), 0) as borrow_total_eth_repaid_sum,
    COALESCE(avg(abs(r.quantity_in_eth)), 0) as borrow_total_eth_repaid_avg,
    COALESCE(count(distinct r.transaction_hash),0) as borrow_total_repays
FROM db_analytics_prod.transpose_repay_events AS r
GROUP BY r.sender_address
