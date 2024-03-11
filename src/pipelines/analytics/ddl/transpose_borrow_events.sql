CREATE table db_analytics_prod.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array ['address_partition'],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH max_token_prices AS (
    SELECT be.epoch_timestamp,
        be.token_address,
        MAX(tp.timestamp) as max_price_timestamp
    FROM db_analytics_prod.features_daily_token_prices AS tp -- tokens_price
    INNER JOIN db_stage_prod.transpose_borrow_events AS be -- borrow_events
        ON tp.address = be.token_address
        AND tp.timestamp BETWEEN (be.epoch_timestamp - (86400 * 7)) AND be.epoch_timestamp -- get price from a week period
    GROUP BY be.epoch_timestamp, be.token_address
),
borrow_events_with_quantity_in_eth AS (
    SELECT
        be.block_number,
        be.log_index,
        be.transaction_hash,
        be.timestamp,
        be.epoch_timestamp,
        be.protocol_name,
        be.contract_version,
        be.market_address,
        be.token_address,
        tm.decimals AS token_decimal,
        be.category,
        be.account_address,
        be.quantity,
CASE
    WHEN ttd.contract_address IS NOT NULL THEN 0 --tokens to drop
    WHEN be.token_address = '0x0000000000000000000000000000000000000000'
    THEN be.quantity / POWER(10, tm.decimals)
    ELSE (be.quantity / POWER(10, tm.decimals)) * tp.price
END AS quantity_in_eth,
        be.sender_address,
        be.sender_address AS index_address,
        be.year,
        be.month,
        SUBSTR(be.sender_address, 3, 2) AS address_partition
    FROM db_stage_prod.transpose_borrow_events AS be -- borrow_events
    INNER JOIN db_stage_prod.ethereum_tokens_metadata AS tm -- tokens_metadata
        ON tm.contract_address = be.token_address
    LEFT JOIN db_sandbox_prod.defi_events_tokens_to_drop AS ttd -- tokens_to_drop
        ON ttd.contract_address = be.token_address
    LEFT JOIN max_token_prices AS mtp -- max_tokens_price
        ON mtp.epoch_timestamp = be.epoch_timestamp AND mtp.token_address = be.token_address
    LEFT JOIN db_analytics_prod.features_daily_token_prices as tp -- token_prices
        ON tp.timestamp = mtp.max_price_timestamp
        AND tp.address = mtp.token_address
    WHERE tm.decimals > 0
)

SELECT * FROM borrow_events_with_quantity_in_eth WHERE year = '1970' AND month = '1'
