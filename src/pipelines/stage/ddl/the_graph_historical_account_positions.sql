CREATE TABLE db_stage_prod.the_graph_historical_account_positions WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    partitioned_by = ARRAY['year', 'month'],
    external_location = 's3://data-lakehouse-prod/stage/the_graph/the_graph_historical_account_positions/'
) AS
SELECT
    CAST(balance AS DOUBLE) AS balance,
    id,
    iscollateral AS is_collateral,
    market.name AS market,
    LOWER(market.id) AS market_id,
    side,
    LOWER(account.id) AS account,
    block_number,
    protocol,
    timestamp as block_timestamp,
    year,
    month
FROM
    db_raw_prod.the_graph_historical_account_positions
