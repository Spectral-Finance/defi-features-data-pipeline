with total_collateral_eth_values_all_zero as (
    SELECT
        'total_collateral_eth_values_all_zero' AS constraint_name,
    CASE
        WHEN (CAST(COUNT(CASE WHEN total_collateral_eth = 0 THEN 1 ELSE NULL END) as double) / COUNT(*) * 100) = 100 THEN True
        ELSE False
    END AS is_fail
    FROM db_analytics_prod.defi_features
),
factor_values_all_zero as (
    SELECT
        'factor_values_all_zero' AS constraint_name,
        CASE
            WHEN (CAST(COUNT(CASE WHEN risk_factor = 0 THEN 1 ELSE NULL END) as double) / COUNT(*) * 100) = 100 THEN True
            ELSE False
        END AS is_fail
    FROM db_analytics_prod.defi_features
),
borrow_counts_values_all_zero as (
    SELECT
        'borrow_counts_values_all_zero' AS constraint_name,
        CASE
            WHEN (CAST(COUNT(CASE WHEN borrow_count = 0 THEN 1 ELSE NULL END) as double) / COUNT(*) * 100) = 100 THEN True
            ELSE False
        END AS is_fail
    FROM db_analytics_prod.defi_features
),
exist_negative_count as (
    -- All counts must be >= 0
    SELECT
        'exist_negative_count' AS constraint_name,
        CASE
            WHEN COUNT(*) > 0 THEN True
            ELSE False
        END AS is_fail
    FROM db_analytics_prod.defi_features
    WHERE
        unique_borrow_protocol_count < 0 or
        unique_lending_protocol_count < 0 or
        deposit_count < 0 or
        liquidation_count < 0 or
        borrow_count < 0 or
        repay_count < 0
)
SELECT * FROM total_collateral_eth_values_all_zero
UNION ALL
SELECT * FROM factor_values_all_zero
UNION ALL
SELECT * FROM borrow_counts_values_all_zero
UNION ALL
SELECT * FROM exist_negative_count;