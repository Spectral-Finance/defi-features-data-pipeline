CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array ['address_partition'],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH max_token_prices AS (
    SELECT re.epoch_timestamp,
        re.token_address,
        MAX(tp.timestamp) as max_price_timestamp
    FROM db_analytics_prod.features_daily_token_prices AS tp -- tokens_price
    INNER JOIN db_stage_prod.transpose_repay_events AS re -- repay_events
        ON tp.address = re.token_address
        AND tp.timestamp BETWEEN (re.epoch_timestamp - (86400 * 7)) AND re.epoch_timestamp -- get price from a week period
    GROUP BY re.epoch_timestamp, re.token_address
),
repay_events_with_quantity_in_eth AS (
    SELECT
        re.block_number,
        re.log_index,
        re.transaction_hash,
        re.timestamp,
        re.epoch_timestamp,
        re.protocol_name,
        re.contract_version,
        re.market_address,
        re.token_address,
        tm.decimals AS token_decimal,
        re.category,
        re.account_address,
        re.quantity,
        CASE
            WHEN ttd.contract_address IS NOT NULL THEN 0 --tokens to drop
            WHEN re.token_address = '0x0000000000000000000000000000000000000000'
            THEN re.quantity / POWER(10, tm.decimals)
            ELSE (re.quantity / POWER(10, tm.decimals)) * tp.price
        END AS quantity_in_eth,
        re.sender_address,
        re.sender_address AS index_address,
        re.year,
        re.month,
        SUBSTR(re.sender_address, 3, 2) AS address_partition
    FROM db_stage_prod.transpose_repay_events AS re -- repay_events
    INNER JOIN db_stage_prod.ethereum_tokens_metadata AS tm -- tokens_metadata
        ON tm.contract_address = re.token_address
    LEFT JOIN db_sandbox_prod.defi_events_tokens_to_drop AS ttd -- tokens_to_drop
        ON ttd.contract_address = be.token_address
    LEFT JOIN max_token_prices AS mtp -- max_tokens_price
        ON mtp.epoch_timestamp = re.epoch_timestamp AND mtp.token_address = re.token_address
    LEFT JOIN db_analytics_prod.features_daily_token_prices as tp -- token_prices
        ON tp.timestamp = mtp.max_price_timestamp
        AND tp.address = mtp.token_address
    WHERE tm.decimals > 0
)

SELECT * FROM repay_events_with_quantity_in_eth WHERE year = '1970' AND month = '1'
