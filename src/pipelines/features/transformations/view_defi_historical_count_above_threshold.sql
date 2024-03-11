CREATE OR REPLACE VIEW db_analytics_prod.view_defi_historical_count_above_threshold AS
with borrow_table as (
    SELECT  account,
            block_number,
            sum(balance_in_usd) as total_borrow_balance,
            sum(balance_in_eth) as total_borrow_balance_eth,
            protocol
    FROM db_analytics_prod.the_graph_historical_market_data_and_account_positions as ap
    where 1=1
        and side = 'BORROWER'
    group by account, block_number, protocol
),
lending_table as (
    SELECT account,
            block_number,
            sum(balance_in_usd * liquidation_threshold) as health_factor_numerator,
            sum(balance_in_eth * liquidation_threshold) as misc_available_borrows_eth_collateral,
            sum(balance_in_eth) as misc_total_collateral_eth,
            protocol
    FROM db_analytics_prod.the_graph_historical_market_data_and_account_positions as ap
    where 1=1
    and side = 'LENDER'
    and is_collateral = True
    group by account, block_number, protocol
),
account_health_factor as (
    select lending_table.account,
        lending_table.block_number,
        lending_table.health_factor_numerator,
        lending_table.misc_total_collateral_eth,
        borrow_table.total_borrow_balance_eth,
        CASE
            WHEN coalesce(borrow_table.total_borrow_balance, 0) = 0 THEN 1000000
            WHEN (lending_table.health_factor_numerator / borrow_table.total_borrow_balance = 0) and borrow_table.total_borrow_balance <.000001 THEN 1000000
            WHEN (lending_table.health_factor_numerator / borrow_table.total_borrow_balance = 0) and borrow_table.total_borrow_balance >.000001 THEN .000001
            WHEN lending_table.health_factor_numerator / borrow_table.total_borrow_balance = 0 THEN .000001
            WHEN lending_table.health_factor_numerator / borrow_table.total_borrow_balance < .000001 THEN .000001
            ELSE lending_table.health_factor_numerator / borrow_table.total_borrow_balance
        END as health_factor,
        COALESCE(case when borrow_table.total_borrow_balance_eth is null then lending_table.misc_available_borrows_eth_collateral
            else lending_table.misc_available_borrows_eth_collateral - borrow_table.total_borrow_balance_eth
        end, 0) as misc_available_borrows_eth,
        case when borrow_table.total_borrow_balance_eth is null
            then 0
            else borrow_table.total_borrow_balance_eth * (1/(
                CASE
                    WHEN coalesce(borrow_table.total_borrow_balance, 0) = 0 THEN 1000000
                    WHEN (lending_table.health_factor_numerator / borrow_table.total_borrow_balance = 0) and borrow_table.total_borrow_balance <.000001 THEN 1000000
                    WHEN (lending_table.health_factor_numerator / borrow_table.total_borrow_balance = 0) and borrow_table.total_borrow_balance >.000001 THEN .000001
                    WHEN lending_table.health_factor_numerator / borrow_table.total_borrow_balance = 0 THEN .000001
                    WHEN lending_table.health_factor_numerator / borrow_table.total_borrow_balance < .000001 THEN .000001
                    ELSE lending_table.health_factor_numerator / borrow_table.total_borrow_balance
                END))
        end as weighted_risk_factor,
        lending_table.protocol
        from lending_table
    left join borrow_table on borrow_table.account = lending_table.account
        and borrow_table.block_number = lending_table.block_number
    order by lending_table.block_number, lending_table.account asc
),
historical_count_above_threshold_aave as (
    SELECT
        b.sender_address,
        COALESCE(COUNT(distinct hf.block_number),0) AS historical_count_above_threshold
    FROM db_analytics_prod.transpose_borrow_events as b
    LEFT JOIN account_health_factor as hf
    ON (b.account_address = hf.account or b.sender_address = hf.account) and hf.health_factor < 1.2
    where protocol = 'aave-v2-eth'
    group by b.sender_address
),
historical_count_above_threshold_compound as (
    SELECT
        b.sender_address,
        COALESCE(COUNT(distinct hf.block_number),0) AS historical_count_above_threshold
    FROM db_analytics_prod.transpose_borrow_events as b
    LEFT JOIN account_health_factor as hf
    ON (b.account_address = hf.account or b.sender_address = hf.account) and hf.health_factor < 1.2
    where protocol = 'compound-v2-eth'
    group by b.sender_address
),
historical_count_above_threshold_aave_and_compound_merged as (
    SELECT
        COALESCE(c.sender_address, a.sender_address) as sender_address,
        COALESCE(c.historical_count_above_threshold, 0) as historical_count_above_threshold_compound,
        COALESCE(a.historical_count_above_threshold, 0) as historical_count_above_threshold_aave
    FROM historical_count_above_threshold_compound as c
    FULL OUTER JOIN historical_count_above_threshold_aave as a
        ON a.sender_address = c.sender_address
),
historical_count_above_threshold_features as (
    SELECT
        DISTINCT
        sender_address,
        COALESCE((historical_count_above_threshold_compound + historical_count_above_threshold_aave), 0) as historical_count_above_threshold
    FROM historical_count_above_threshold_aave_and_compound_merged
)
select * from historical_count_above_threshold_features
