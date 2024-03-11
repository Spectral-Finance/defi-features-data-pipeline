-- credit_mix_features
CREATE OR REPLACE VIEW db_analytics_prod.view_defi_credit_mix_features AS
WITH credit_mix_count_lending_protocol AS (
    SELECT
      d.sender_address,
      COALESCE(COUNT(DISTINCT d.protocol_name), 0) AS credit_mix_count_lending_protocol
    FROM db_analytics_prod.transpose_deposit_events AS d
    GROUP BY d.sender_address
),
credit_mix_count_withdraw_protocol AS (
    SELECT
      w.sender_address,
      COALESCE(COUNT(DISTINCT w.protocol_name), 0) AS credit_mix_count_withdraw_protocol
    FROM db_analytics_prod.transpose_withdraw_events AS w
    GROUP BY w.sender_address
),
credit_mix_count_repay_protocol AS (
    SELECT
      r.sender_address,
      COALESCE(COUNT(DISTINCT r.protocol_name), 0) AS credit_mix_count_repay_protocol
    FROM db_analytics_prod.transpose_repay_events AS r
    GROUP BY r.sender_address
),
credit_mix_count_borrow_protocol AS (
    SELECT
        b.sender_address,
        COALESCE(COUNT(DISTINCT b.protocol_name), 0) AS credit_mix_count_borrow_protocol
    FROM db_analytics_prod.transpose_borrow_events AS b
    GROUP BY b.sender_address
),
credit_mix_features AS (
    SELECT
        a.wallet_address as sender_address,
        COALESCE(b.credit_mix_count_borrow_protocol, 0) AS credit_mix_count_borrow_protocol,
        COALESCE(l.credit_mix_count_lending_protocol, 0) AS credit_mix_count_lending_protocol,
        COALESCE(w.credit_mix_count_withdraw_protocol, 0) AS credit_mix_count_withdraw_protocol,
        COALESCE(r.credit_mix_count_repay_protocol, 0) AS credit_mix_count_repay_protocol
    FROM db_analytics_prod.view_distinct_defi_events_addresses a
    LEFT JOIN credit_mix_count_borrow_protocol as b
        on a.wallet_address = b.sender_address
    LEFT JOIN credit_mix_count_lending_protocol as l
        ON a.wallet_address = l.sender_address
    LEFT JOIN credit_mix_count_withdraw_protocol as w
        ON a.wallet_address = w.sender_address
    LEFT JOIN credit_mix_count_repay_protocol as r
        ON a.wallet_address = r.sender_address
)
SELECT
    sender_address,
    credit_mix_count_borrow_protocol,
    credit_mix_count_lending_protocol,
    GREATEST(
        credit_mix_count_borrow_protocol,
        credit_mix_count_lending_protocol,
        credit_mix_count_withdraw_protocol,
        credit_mix_count_repay_protocol
    ) AS credit_mix_count_protocol
FROM credit_mix_features
