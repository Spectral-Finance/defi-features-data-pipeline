-- current_health_factor_features
CREATE OR REPLACE VIEW db_analytics_prod.view_defi_current_health_factor_features AS
WITH last_market_data AS (
    SELECT
        name,
        max(block_number) as max_block_number
    FROM db_stage_prod.the_graph_historical_market_data
    GROUP BY name
),
-- Current market data is the most recent market data for each market
current_market_data AS (
    SELECT
        hmd.id,
        hmd.input_token_price_usd,
        hmd.decimals,
        (hmd.liquidation_threshold * .01) AS liquidation_threshold,
        hmd.protocol
    FROM db_stage_prod.the_graph_historical_market_data AS hmd
    INNER JOIN last_market_data AS lmd
        ON lmd.name = hmd.name
    AND hmd.block_number >= lmd.max_block_number
),
current_market_data_by_protocol AS (
    SELECT
        hmd.input_token_price_usd,
        hmd.protocol
    FROM db_stage_prod.the_graph_historical_market_data AS hmd
    INNER JOIN last_market_data AS lmd
        ON lmd.name = hmd.name
    WHERE hmd.name in ('Aave interest bearing WETH', 'Compound Ether')
    AND hmd.block_number >= lmd.max_block_number
),
-- Current market data and positions is the current market data joined with the current collateral positions
current_market_data_and_positions AS (
    SELECT
        cmd.id,
        cmd.input_token_price_usd,
        cmd.liquidation_threshold,
        (cp.balance * cmd.input_token_price_usd) / POWER(10, cmd.decimals) AS balance_in_usd,
        (1 / cmdp.input_token_price_usd) * ((cp.balance * cmd.input_token_price_usd) / POWER(10, cmd.decimals)) AS balance_in_eth,
        cp.is_collateral,
        cp.side,
        cp.account,
        cp.protocol
    FROM db_analytics_prod.the_graph_current_collateral_positions AS cp
    INNER JOIN current_market_data AS cmd
        ON cmd.id = cp.market_id
    INNER JOIN current_market_data_by_protocol AS cmdp
        ON cmdp.protocol = cp.protocol
), --select * from current_market_data_and_positions where account = '0x00b8fc7e9c45032cba298251f08c0ea71342b96b'
 -- Calculates total borrow balance for Aave protocol.
borrow_table_aave as (
    SELECT  account,
        sum(balance_in_usd) as total_borrow_balance,
        sum(balance_in_eth) as total_borrow_balance_eth
    FROM current_market_data_and_positions
        where 1=1
        and protocol = 'aave-v2-eth'
        and side = 'BORROWER'
    group by account
),
-- Calculates total collateral balance for Aave protocol.
lending_table_aave as (
    SELECT account,
        sum(balance_in_usd * liquidation_threshold) as health_factor_numerator,
        sum(balance_in_eth * liquidation_threshold) as misc_available_borrows_eth_collateral,
        sum(balance_in_eth) as misc_total_collateral_eth
    FROM current_market_data_and_positions
        where 1=1
        and protocol = 'aave-v2-eth'
        and side = 'LENDER'
        and is_collateral = True
    group by account
),
-- Calculates health factor for Aave protocol.
aave_health_factor AS (
    select lending_table_aave.account,
        lending_table_aave.health_factor_numerator,
        lending_table_aave.misc_total_collateral_eth,
        borrow_table_aave.total_borrow_balance_eth,
        CASE
            WHEN coalesce(borrow_table_aave.total_borrow_balance, 0) = 0 THEN 1000000
            WHEN (lending_table_aave.health_factor_numerator / borrow_table_aave.total_borrow_balance = 0) and borrow_table_aave.total_borrow_balance <.000001 THEN 1000000
            WHEN (lending_table_aave.health_factor_numerator / borrow_table_aave.total_borrow_balance = 0) and borrow_table_aave.total_borrow_balance >.000001 THEN .000001
            WHEN lending_table_aave.health_factor_numerator / borrow_table_aave.total_borrow_balance = 0 THEN .000001
            WHEN lending_table_aave.health_factor_numerator / borrow_table_aave.total_borrow_balance < .000001 THEN .000001
            ELSE lending_table_aave.health_factor_numerator / borrow_table_aave.total_borrow_balance
        END as health_factor,
        case when borrow_table_aave.total_borrow_balance_eth is null then lending_table_aave.misc_available_borrows_eth_collateral
            else lending_table_aave.misc_available_borrows_eth_collateral - borrow_table_aave.total_borrow_balance_eth
        end as misc_available_borrows_eth,
        case when borrow_table_aave.total_borrow_balance_eth is null
            then 0
            else borrow_table_aave.total_borrow_balance_eth * (1/(
                CASE
                    WHEN coalesce(borrow_table_aave.total_borrow_balance, 0) = 0 THEN 1000000
                    WHEN (lending_table_aave.health_factor_numerator / borrow_table_aave.total_borrow_balance = 0) and borrow_table_aave.total_borrow_balance <.000001 THEN 1000000
                    WHEN (lending_table_aave.health_factor_numerator / borrow_table_aave.total_borrow_balance = 0) and borrow_table_aave.total_borrow_balance >.000001 THEN .000001
                    WHEN lending_table_aave.health_factor_numerator / borrow_table_aave.total_borrow_balance = 0 THEN .000001
                    WHEN lending_table_aave.health_factor_numerator / borrow_table_aave.total_borrow_balance < .000001 THEN .000001
                    ELSE lending_table_aave.health_factor_numerator / borrow_table_aave.total_borrow_balance
                END))
        end as weighted_risk_factor
        from lending_table_aave
    left join borrow_table_aave on borrow_table_aave.account = lending_table_aave.account
),
-- Calculates total borrow balance for Compound protocol.
borrow_table_compound as (
    SELECT  account,
        sum(balance_in_usd) as total_borrow_balance,
        sum(balance_in_eth) as total_borrow_balance_eth
    FROM current_market_data_and_positions
        where 1=1
        and protocol = 'compound-v2-eth'
        and side = 'BORROWER'
    group by account
),
-- Calculates total collateral balance for Compound protocol.
lending_table_compound as (
    SELECT account,
        sum(balance_in_usd * liquidation_threshold) as health_factor_numerator,
        sum(balance_in_eth * liquidation_threshold) as misc_available_borrows_eth_collateral,
        sum(balance_in_eth) as misc_total_collateral_eth
    FROM current_market_data_and_positions
        where 1=1
        and protocol = 'compound-v2-eth'
        and side = 'LENDER'
        and is_collateral = True
    group by account
),
-- Calculates health factor for Compound protocol.
compound_health_factor AS (
    select lending_table_compound.account,
        lending_table_compound.health_factor_numerator,
        lending_table_compound.misc_total_collateral_eth,
        borrow_table_compound.total_borrow_balance_eth,
        CASE
            WHEN coalesce(borrow_table_compound.total_borrow_balance, 0) = 0 THEN 1000000
            WHEN (lending_table_compound.health_factor_numerator / borrow_table_compound.total_borrow_balance = 0) and borrow_table_compound.total_borrow_balance <.000001 THEN 1000000
            WHEN (lending_table_compound.health_factor_numerator / borrow_table_compound.total_borrow_balance = 0) and borrow_table_compound.total_borrow_balance >.000001 THEN .000001
            WHEN lending_table_compound.health_factor_numerator / borrow_table_compound.total_borrow_balance = 0 THEN .000001
            WHEN lending_table_compound.health_factor_numerator / borrow_table_compound.total_borrow_balance < .000001 THEN .000001
            ELSE lending_table_compound.health_factor_numerator / borrow_table_compound.total_borrow_balance
        END as health_factor,
        case when borrow_table_compound.total_borrow_balance_eth is null then lending_table_compound.misc_available_borrows_eth_collateral
            else lending_table_compound.misc_available_borrows_eth_collateral - borrow_table_compound.total_borrow_balance_eth
        end as misc_available_borrows_eth,
        case when borrow_table_compound.total_borrow_balance_eth is null
            then 0
            else borrow_table_compound.total_borrow_balance_eth * (1/(
                CASE
                    WHEN coalesce(borrow_table_compound.total_borrow_balance, 0) = 0 THEN 1000000
                    WHEN (lending_table_compound.health_factor_numerator / borrow_table_compound.total_borrow_balance = 0) and borrow_table_compound.total_borrow_balance <.000001 THEN 1000000
                    WHEN (lending_table_compound.health_factor_numerator / borrow_table_compound.total_borrow_balance = 0) and borrow_table_compound.total_borrow_balance >.000001 THEN .000001
                    WHEN lending_table_compound.health_factor_numerator / borrow_table_compound.total_borrow_balance = 0 THEN .000001
                    WHEN lending_table_compound.health_factor_numerator / borrow_table_compound.total_borrow_balance < .000001 THEN .000001
                    ELSE lending_table_compound.health_factor_numerator / borrow_table_compound.total_borrow_balance
                END))
        end as weighted_risk_factor
        from lending_table_compound
    left join borrow_table_compound on borrow_table_compound.account = lending_table_compound.account
),
-- Combines Aave and Compound health factor data.
aave_and_compound_health_factor as (
    SELECT COALESCE(c.account, a.account) as account,
        COALESCE(a.health_factor, 1000000) as health_factor_aave,
        a.weighted_risk_factor as weighted_risk_factor_aave,
        COALESCE(c.health_factor, 1000000) as health_factor_compound,
        COALESCE(c.weighted_risk_factor, 0) as weighted_risk_factor_compound,
        COALESCE(a.misc_total_collateral_eth, 0) AS misc_total_collateral_eth_aave,
        COALESCE(c.misc_total_collateral_eth, 0) AS misc_total_collateral_eth_compound,
        COALESCE(a.misc_available_borrows_eth, 0) AS misc_available_borrows_eth_aave,
        COALESCE(c.misc_available_borrows_eth, 0) AS misc_available_borrows_eth_compound,
        COALESCE(a.total_borrow_balance_eth, 0) AS total_borrow_balance_eth_aave,
        COALESCE(c.total_borrow_balance_eth, 0) AS total_borrow_balance_eth_compound
    FROM compound_health_factor AS c
    FULL OUTER JOIN aave_health_factor AS a ON a.account = c.account
),
-- Transforms Aave and Compound health factor data.
aave_and_compound_health_factor_transformations AS (
    SELECT
        account,
        CASE WHEN LEAST(health_factor_compound, health_factor_aave) > 1000000
            THEN 1000000
            ELSE LEAST(health_factor_compound, health_factor_aave)
        END AS current_health_factor,
        misc_total_collateral_eth_compound + misc_total_collateral_eth_aave AS total_collateral_eth,
        misc_available_borrows_eth_compound + misc_available_borrows_eth_aave AS available_borrows_eth,
        weighted_risk_factor_compound + weighted_risk_factor_aave AS weighted_risk_factor,
        total_borrow_balance_eth_compound + total_borrow_balance_eth_aave AS total_borrow_balance_eth,
        1 / LEAST(health_factor_compound, health_factor_aave) AS current_risk_factor,
        CASE WHEN 1 / LEAST(health_factor_compound, health_factor_aave) > 1000000
            THEN 1000000
            ELSE 1 / (CASE WHEN LEAST(health_factor_compound, health_factor_aave) > 1000000 THEN 1000000 ELSE LEAST(health_factor_compound, health_factor_aave) END)
        END AS borrow_current_risk_factor_capped
    FROM aave_and_compound_health_factor
),
-- Calculates features for Aave and Compound health factor data.
aave_and_compound_health_factor_features AS (
    SELECT
        account,
        current_health_factor,
        SUM(total_collateral_eth) AS misc_total_collateral_eth,
        SUM(available_borrows_eth) AS misc_available_borrows_eth,
        SUM(weighted_risk_factor) AS weighted_risk_factor,
        SUM(total_borrow_balance_eth) AS total_borrow_balance_eth,
        COALESCE(SUM(weighted_risk_factor) / NULLIF(SUM(total_borrow_balance_eth), 0), 0) AS borrow_weighted_avg_risk_factor,
        current_risk_factor,
        borrow_current_risk_factor_capped
    FROM aave_and_compound_health_factor_transformations
    GROUP BY account, current_health_factor, current_risk_factor, borrow_current_risk_factor_capped
)

SELECT * FROM aave_and_compound_health_factor_features
