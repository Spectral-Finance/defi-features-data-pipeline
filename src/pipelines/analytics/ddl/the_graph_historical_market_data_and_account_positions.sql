CREATE table db_analytics_prod.the_graph_historical_market_data_and_account_positions WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array ['address_partition'],
	external_location = 's3://data-lakehouse-prod/analytics/the_graph/the_graph_historical_market_data_and_account_positions/'
) AS
with market_data_prices_by_protocol AS (
    SELECT
        hmd.input_token_price_usd,
        hmd.block_number,
        protocol
    FROM db_stage_prod.the_graph_historical_market_data AS hmd
        WHERE hmd.name in ('Aave interest bearing WETH', 'Compound Ether')
),
merged_market_data_and_account_positions as (
-- we need to create this as a table and ingest incrementing data into it based on the latest block number
select
    ap.balance,
    (ap.balance * md.input_token_price_usd) / POWER(10, md.decimals) AS balance_in_usd,
    CASE WHEN (ap.balance * md.input_token_price_usd) / POWER(10, md.decimals) = 0.0 or mdp.input_token_price_usd = 0.0 THEN 0.0
        ELSE (1 / mdp.input_token_price_usd) * ((ap.balance * md.input_token_price_usd) / POWER(10, md.decimals))
    END AS balance_in_eth,
    ap.id,
    ap.is_collateral,
    ap.market_id,
    ap.side,
    ap.account,
    md.liquidation_threshold * 0.01 as liquidation_threshold,
    md.input_token_price_usd,
    mdp.input_token_price_usd as input_token_price_usd_protocol,
    md.decimals,
    ap.protocol,
    ap.block_number,
    ap.block_timestamp,
    ap.year,
    ap.month,
    SUBSTR(ap.account, 3, 2) AS address_partition
from db_stage_prod.the_graph_historical_account_positions as ap
inner join db_stage_prod.the_graph_historical_market_data as md
    on md.id = ap.market_id and md.block_number = ap.block_number
inner join market_data_prices_by_protocol as mdp
    on mdp.block_number = ap.block_number and mdp.protocol = ap.protocol
WHERE ap.year = '1970' AND ap.month = '1'
)

SELECT * FROM merged_market_data_and_account_positions
