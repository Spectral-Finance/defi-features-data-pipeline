-- borrow_features
CREATE OR REPLACE VIEW db_analytics_prod.view_defi_borrow_features AS
SELECT
    b.sender_address,
    COALESCE(sum(abs(b.quantity_in_eth)),0) as loan_amount_eth_sum,
    COALESCE(avg(abs(b.quantity_in_eth)), 0) as loan_amount_eth_avg,
    coalesce(count(distinct b.transaction_hash),0) as borrow_total_borrows
FROM db_analytics_prod.transpose_borrow_events AS b
GROUP BY b.sender_address
