WITH repay_features AS (
    SELECT DISTINCT
        ddea.wallet_address,
        COALESCE(rf.borrow_total_eth_repaid_sum, 0) AS borrow_total_eth_repaid_sum,
        COALESCE(rf.borrow_total_eth_repaid_avg, 0) AS borrow_total_eth_repaid_avg,
        COALESCE(rf.borrow_total_repays, 0) AS borrow_total_repays
    FROM db_analytics_prod.view_distinct_defi_events_addresses AS ddea
    LEFT JOIN db_analytics_prod.view_defi_repay_features as rf
        ON ddea.wallet_address = rf.sender_address
),
borrow_features_merged AS (
    SELECT DISTINCT
        rf.*,
        COALESCE(bf.loan_amount_eth_sum, 0) AS loan_amount_eth_sum,
        COALESCE(bf.loan_amount_eth_avg, 0) AS loan_amount_eth_avg,
        COALESCE(bf.borrow_total_borrows, 0) AS borrow_total_borrows
    FROM repay_features as rf
    LEFT JOIN db_analytics_prod.view_defi_borrow_features as bf
        ON bf.sender_address = rf.wallet_address
),
credit_mix_features_merged AS (
    SELECT DISTINCT
        bf.*,
        COALESCE(cmf.credit_mix_count_borrow_protocol, 0) AS credit_mix_count_borrow_protocol,
        COALESCE(cmf.credit_mix_count_lending_protocol, 0) AS credit_mix_count_lending_protocol,
        COALESCE(cmf.credit_mix_count_protocol, 0) AS credit_mix_count_protocol
    FROM borrow_features_merged as bf
    LEFT JOIN db_analytics_prod.view_defi_credit_mix_features as cmf
        ON cmf.sender_address = bf.wallet_address
),
lending_sum_redeems_features_merged AS (
    SELECT DISTINCT
        cmf.*,
        COALESCE(lrf.lending_sum_redeems, 0) AS lending_sum_redeems
    FROM credit_mix_features_merged AS cmf
    LEFT JOIN db_analytics_prod.view_defi_lending_sum_redeems_features as lrf
        ON lrf.sender_address = cmf.wallet_address
),
lending_time_and_count_features_merged AS (
    SELECT DISTINCT
        lrf.*,
        COALESCE(lf.lending_total_deposits, 0) AS lending_total_deposits,
        COALESCE(lf.lending_time_since_first_lending, 0) AS lending_time_since_first_lending,
        COALESCE(lf.total_deposits_in_eth, 0) AS total_deposits_in_eth
    FROM lending_sum_redeems_features_merged as lrf
    LEFT JOIN db_analytics_prod.view_defi_lending_time_and_count_features as lf
        ON lf.sender_address = lrf.wallet_address
),
historical_health_and_risk_factor_features_merged AS (
    SELECT
        lrf.*,
        COALESCE(hf.historical_max_risk_factor, 0) AS historical_max_risk_factor,
        COALESCE(hf.misc_avg_total_collateral_eth, 0) AS misc_avg_total_collateral_eth,
        COALESCE(hf.misc_avg_available_borrows_eth, 0) AS misc_avg_available_borrows_eth,
        COALESCE(hf.historical_weighted_avg_risk_factor, 0) AS historical_weighted_avg_risk_factor,
        COALESCE(hf.historical_average_risk_factor, 0) AS historical_average_risk_factor
    FROM lending_time_and_count_features_merged as lrf
    LEFT JOIN db_analytics_prod.view_defi_historical_health_and_risk_factor as hf
        ON hf.sender_address = lrf.wallet_address
),
historical_count_above_threshold_features_merged AS (
    SELECT DISTINCT
        hf.*,
        COALESCE(hat.historical_count_above_threshold, 0) AS historical_count_above_threshold
    FROM historical_health_and_risk_factor_features_merged as hf
    LEFT JOIN db_analytics_prod.view_defi_historical_count_above_threshold as hat
        ON hat.sender_address = hf.wallet_address
),
borrow_distinct_sender_account_pairs AS (
    SELECT DISTINCT
        sender_address,
        account_address
    FROM db_analytics_prod.transpose_borrow_events
),
liquidation_features_merged AS (
    WITH liquidation_accounts_table AS (
        SELECT
            d.sender_address,
            d.account_address,
            COALESCE(l.liquidation_total_liquidations, 0) AS liquidation_total_liquidations,
            COALESCE(l.liquidation_total_amount_eth, 0) AS liquidation_total_amount_eth,
            COALESCE(l.liquidation_time_since_last_liquidated, 0) AS liquidation_time_since_last_liquidated
        FROM borrow_distinct_sender_account_pairs AS d
        LEFT JOIN db_analytics_prod.view_defi_liquidation_features AS l
            ON d.account_address = l.account_address
    ),
    liquidation_features AS (
        SELECT
            sender_address,
            COALESCE(SUM(liquidation_total_liquidations), 0) as liquidation_total_liquidations,
            COALESCE(SUM(liquidation_total_amount_eth), 0) as liquidation_total_amount_eth,
            COALESCE(MIN(liquidation_time_since_last_liquidated), 0) as liquidation_time_since_last_liquidated
         FROM liquidation_accounts_table
            GROUP BY sender_address
    )
    SELECT DISTINCT
        hat.*,
        COALESCE(liq.liquidation_total_liquidations, 0) AS liquidation_total_liquidations,
        COALESCE(liq.liquidation_total_amount_eth, 0) AS liquidation_total_amount_eth,
        COALESCE(liq.liquidation_time_since_last_liquidated, 0) AS liquidation_time_since_last_liquidated
    FROM historical_count_above_threshold_features_merged as hat
    LEFT JOIN liquidation_features AS liq
        ON liq.sender_address = hat.wallet_address
),
current_health_factor_features_merged AS (
    with current_health_factor_accounts_table as (
        select
            d.sender_address,
            d.account_address,
            chr.misc_total_collateral_eth,
            chr.misc_available_borrows_eth,
            chr.current_risk_factor,
            chr.borrow_weighted_avg_risk_factor,
            chr.borrow_current_risk_factor_capped
            from borrow_distinct_sender_account_pairs d
        left join db_analytics_prod.view_defi_current_health_factor_features chr on d.account_address = chr.account
    ),
    current_health_factor_features AS (
        SELECT
            sender_address,
            COALESCE(SUM(misc_total_collateral_eth), 0) as misc_total_collateral_eth,
            COALESCE(SUM(misc_available_borrows_eth), 0) as misc_available_borrows_eth,
            COALESCE(MAX(current_risk_factor), 0) as current_risk_factor,
            COALESCE(MAX(borrow_weighted_avg_risk_factor), 0) as borrow_weighted_avg_risk_factor,
            COALESCE(MAX(borrow_current_risk_factor_capped), 0) as borrow_current_risk_factor_capped
        FROM current_health_factor_accounts_table
        GROUP BY sender_address
    )
    SELECT DISTINCT
        liq.*,
        COALESCE(hf.misc_total_collateral_eth, 0) AS misc_total_collateral_eth,
        COALESCE(hf.misc_available_borrows_eth, 0) AS misc_available_borrows_eth,
        COALESCE(hf.current_risk_factor, 0) AS current_risk_factor,
        COALESCE(hf.borrow_weighted_avg_risk_factor, 0) AS borrow_weighted_avg_risk_factor,
        COALESCE(hf.borrow_current_risk_factor_capped, 0) AS borrow_current_risk_factor_capped
    FROM liquidation_features_merged as liq
    LEFT JOIN current_health_factor_features AS hf
        ON hf.sender_address = liq.wallet_address
),
additional_defi_features AS (
    SELECT
        tb.*,
        CASE
            WHEN tb.loan_amount_eth_sum - tb.borrow_total_eth_repaid_sum < 0 THEN 0
            ELSE tb.loan_amount_eth_sum - tb.borrow_total_eth_repaid_sum
        END AS borrow_total_current_loan_eth,
        CASE
            WHEN tb.lending_sum_redeems - tb.total_deposits_in_eth < 0 THEN 0
            ELSE tb.lending_sum_redeems - tb.total_deposits_in_eth
        END AS withdraw_deposit_diff_if_positive_eth
    FROM current_health_factor_features_merged as tb
),
defi_final_features AS (
    SELECT
        wallet_address,
        credit_mix_count_borrow_protocol AS unique_borrow_protocol_count,
        credit_mix_count_lending_protocol AS unique_lending_protocol_count,
        misc_total_collateral_eth AS total_collateral_eth,
        current_risk_factor AS risk_factor,
        misc_available_borrows_eth AS total_available_borrows_eth,
        lending_total_deposits AS deposit_count,
        CAST(CASE
            WHEN lending_time_since_first_lending = 0 THEN 999999999
            ELSE lending_time_since_first_lending
        END AS BIGINT) AS time_since_first_deposit,
        total_deposits_in_eth AS deposit_amount_sum_eth,
        liquidation_total_liquidations AS liquidation_count,
        liquidation_total_amount_eth AS liquidation_amount_sum_eth,
        CAST(CASE
            WHEN liquidation_time_since_last_liquidated = 0 THEN 999999999
            ELSE liquidation_time_since_last_liquidated
        END AS BIGINT) AS time_since_last_liquidated,
        lending_sum_redeems AS withdraw_amount_sum_eth,
        loan_amount_eth_sum AS borrow_amount_sum_eth,
        loan_amount_eth_avg AS borrow_amount_avg_eth,
        borrow_total_borrows AS borrow_count,
        borrow_total_eth_repaid_sum AS repay_amount_sum_eth,
        borrow_total_eth_repaid_avg AS repay_amount_avg_eth,
        borrow_total_repays AS repay_count,
        borrow_total_current_loan_eth AS borrow_repay_diff_eth,
        withdraw_deposit_diff_if_positive_eth,
        misc_avg_available_borrows_eth AS total_available_borrows_avg_eth,
        historical_weighted_avg_risk_factor AS avg_weighted_risk_factor,
        historical_average_risk_factor AS avg_risk_factor,
        historical_max_risk_factor AS max_risk_factor,
        historical_count_above_threshold AS risk_factor_above_threshold_daily_count,
        misc_avg_total_collateral_eth AS total_collateral_avg_eth
    FROM additional_defi_features
)
SELECT * FROM defi_final_features
