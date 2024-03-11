CREATE TABLE target_database.table_name WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    partitioned_by = ARRAY['year', 'month'],
    external_location = 'bucket_name/layer/data_source/table_name/'
) AS
SELECT
    cast(liquidationthreshold as double) AS liquidation_threshold,
    name,
    cast(inputtokenpriceusd as Double) AS input_token_price_usd,
    id,
    inputtoken.decimals AS decimals,
    protocol,
    block_number,
    timestamp AS block_timestamp,
    year,
    month
FROM db_raw_prod.the_graph_historical_market_data
