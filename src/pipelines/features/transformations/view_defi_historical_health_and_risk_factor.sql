CREATE OR REPLACE VIEW db_analytics_prod.view_defi_historical_health_and_risk_factor AS
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
historical_health_and_risk_factor_aave as (
    with b as (
        select
            sender_address,
            account_address,
            block_number
        from db_analytics_prod.transpose_borrow_events as b
    ),
    union_cte as (
        SELECT
            b.sender_address,
            hf.block_number as block_number_hf,
            hf.health_factor,
            hf.weighted_risk_factor,
            hf.misc_available_borrows_eth,
            hf.misc_total_collateral_eth
        FROM b
        left JOIN account_health_factor hf
        ON b.account_address = hf.account
        where protocol = 'aave-v2-eth'
        UNION
        SELECT
            b.sender_address,
            hf.block_number as block_number_hf,
            hf.health_factor,
            hf.weighted_risk_factor,
            hf.misc_available_borrows_eth,
            hf.misc_total_collateral_eth
        FROM b
        left JOIN account_health_factor hf
        ON b.sender_address = hf.account
        WHERE protocol = 'aave-v2-eth'
    )
    select
        sender_address,
        CASE WHEN COUNT(*) = 1 THEN avg(health_factor) ELSE AVG(CASE WHEN health_factor <> 1000000 THEN health_factor END) END AS avg_health_factor,
        avg(1/health_factor) as historical_average_risk_factor,
        avg(weighted_risk_factor) as historical_weighted_avg_risk_factor,
        min(health_factor) as historical_min_health_factor,
        1/min(health_factor) as historical_max_risk_factor,
        avg(misc_available_borrows_eth) as misc_avg_available_borrows_eth,
        avg(misc_total_collateral_eth) as misc_avg_total_collateral_eth
    from union_cte
    group by 1
), --select * from historical_health_and_risk_factor_aave
historical_health_and_risk_factor_compound as (
    with b as (
        select
            sender_address,
            account_address,
            block_number
        from db_analytics_prod.transpose_borrow_events as b
    ),
    union_cte as (
        SELECT
            b.sender_address,
            hf.block_number as block_number_hf,
            hf.health_factor,
            hf.weighted_risk_factor,
            hf.misc_available_borrows_eth,
            hf.misc_total_collateral_eth
        FROM b
        left JOIN account_health_factor hf
        ON b.account_address = hf.account
        where protocol = 'compound-v2-eth'
        UNION
        SELECT
            b.sender_address,
            hf.block_number as block_number_hf,
            hf.health_factor,
            hf.weighted_risk_factor,
            hf.misc_available_borrows_eth,
            hf.misc_total_collateral_eth
        FROM b
        left JOIN account_health_factor hf
        ON b.sender_address = hf.account
        WHERE protocol = 'compound-v2-eth'
    )
    select
        sender_address,
        CASE WHEN COUNT(*) = 1 THEN avg(health_factor) ELSE AVG(CASE WHEN health_factor <> 1000000 THEN health_factor END) END AS avg_health_factor,
        avg(1/health_factor) as historical_average_risk_factor,
        avg(weighted_risk_factor) as historical_weighted_avg_risk_factor,
        min(health_factor) as historical_min_health_factor,
        1/min(health_factor) as historical_max_risk_factor,
        avg(misc_available_borrows_eth) as misc_avg_available_borrows_eth,
        avg(misc_total_collateral_eth) as misc_avg_total_collateral_eth
    from union_cte
    group by 1
),
historical_health_and_risk_factor_aave_compound_merged as (
    SELECT
        COALESCE(c.sender_address, a.sender_address) as sender_address,
        COALESCE(c.avg_health_factor, 0) as avg_health_factor_compound,
        COALESCE(a.avg_health_factor, 0) as avg_health_factor_aave,
        COALESCE(c.historical_average_risk_factor, 0) as historical_average_risk_factor_compound,
        COALESCE(a.historical_average_risk_factor, 0) as historical_average_risk_factor_aave,
        COALESCE(c.historical_weighted_avg_risk_factor, 0) as historical_weighted_avg_risk_factor_compound,
        COALESCE(a.historical_weighted_avg_risk_factor, 0) as historical_weighted_avg_risk_factor_aave,
        COALESCE(c.historical_min_health_factor, 0) as historical_min_health_factor_compound,
        COALESCE(a.historical_min_health_factor, 0) as historical_min_health_factor_aave,
        COALESCE(c.historical_max_risk_factor, 0) as historical_max_risk_factor_compound,
        COALESCE(a.historical_max_risk_factor, 0) as historical_max_risk_factor_aave,
        COALESCE(c.misc_avg_available_borrows_eth, 0) as misc_avg_available_borrows_eth_compound,
        COALESCE(a.misc_avg_available_borrows_eth, 0) as misc_avg_available_borrows_eth_aave,
        COALESCE(c.misc_avg_total_collateral_eth, 0) as misc_avg_total_collateral_eth_compound,
        COALESCE(a.misc_avg_total_collateral_eth, 0) as misc_avg_total_collateral_eth_aave
    FROM historical_health_and_risk_factor_compound as c
    FULL OUTER JOIN historical_health_and_risk_factor_aave as a
        ON a.sender_address = c.sender_address
),
historical_health_and_risk_factor_features as (
    SELECT
        DISTINCT
        sender_address,
        COALESCE(greatest(historical_max_risk_factor_compound, historical_max_risk_factor_aave), 0) as historical_max_risk_factor,
        COALESCE((misc_avg_total_collateral_eth_compound + misc_avg_total_collateral_eth_aave), 0) as misc_avg_total_collateral_eth,
        COALESCE((misc_avg_available_borrows_eth_compound + misc_avg_available_borrows_eth_aave), 0) as misc_avg_available_borrows_eth,
        CASE
            WHEN historical_weighted_avg_risk_factor_aave = 0 AND historical_weighted_avg_risk_factor_compound != 0 THEN historical_weighted_avg_risk_factor_compound
            WHEN historical_weighted_avg_risk_factor_compound = 0 AND historical_weighted_avg_risk_factor_aave != 0 THEN historical_weighted_avg_risk_factor_aave
            ELSE (historical_weighted_avg_risk_factor_compound + historical_weighted_avg_risk_factor_aave) / 2
        END as historical_weighted_avg_risk_factor,
        CASE
            WHEN historical_average_risk_factor_aave = 0 AND historical_average_risk_factor_compound != 0 THEN historical_average_risk_factor_compound
            WHEN historical_average_risk_factor_compound = 0 AND historical_average_risk_factor_aave != 0 THEN historical_average_risk_factor_aave
            ELSE (historical_average_risk_factor_compound + historical_average_risk_factor_aave) / 2
        END as historical_average_risk_factor
    FROM historical_health_and_risk_factor_aave_compound_merged
)
select * from historical_health_and_risk_factor_features
