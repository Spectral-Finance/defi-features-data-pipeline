CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array [ 'year', 'month' ],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
SELECT
    block_number,
    log_index,
    transaction_hash,
    timestamp,
    CAST(TO_UNIXTIME(timestamp) AS DECIMAL) AS epoch_timestamp,
    protocol_name,
    contract_version,
    LOWER(market_address) AS market_address,
    LOWER(token_address) AS token_address,
    category,
    LOWER(account_address) AS account_address,
    quantity,
    LOWER(sender_address) AS sender_address,
    year,
    month
FROM source_database.transpose_withdraw_events;
