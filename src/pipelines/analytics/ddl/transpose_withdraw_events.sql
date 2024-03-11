CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array ['address_partition'],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH max_token_prices AS (
    SELECT we.epoch_timestamp,
        we.token_address,
        MAX(tp.timestamp) as max_price_timestamp
    FROM db_analytics_prod.features_daily_token_prices AS tp -- tokens_price
    INNER JOIN db_stage_prod.transpose_withdraw_events AS we -- withdraw_events
        ON tp.address = we.token_address
        AND tp.timestamp BETWEEN (we.epoch_timestamp - (86400 * 7)) AND we.epoch_timestamp -- get price from a week period
    GROUP BY we.epoch_timestamp, we.token_address
),
withdraw_events_with_quantity_in_eth AS (
    SELECT
        we.block_number,
        we.log_index,
        we.transaction_hash,
        we.timestamp,
        we.epoch_timestamp,
        we.protocol_name,
        we.contract_version,
        we.market_address,
        we.token_address,
        tm.decimals AS token_decimal,
        we.category,
        we.account_address,
        we.quantity,
        CASE
            WHEN ttd.contract_address IS NOT NULL THEN 0 --tokens to drop
            WHEN we.token_address = '0x0000000000000000000000000000000000000000'
            THEN we.quantity / POWER(10, tm.decimals)
            ELSE (we.quantity / POWER(10, tm.decimals)) * tp.price
        END AS quantity_in_eth,
        we.sender_address,
        we.sender_address AS index_address,
        we.year,
        we.month,
        SUBSTR(we.sender_address, 3, 2) AS address_partition
    FROM db_stage_prod.transpose_withdraw_events AS we -- withdraw_events
    INNER JOIN db_stage_prod.ethereum_tokens_metadata AS tm -- tokens_metadata
        ON tm.contract_address = we.token_address
    LEFT JOIN db_sandbox_prod.defi_events_tokens_to_drop AS ttd -- tokens_to_drop
        ON ttd.contract_address = be.token_address
    LEFT JOIN max_token_prices AS mtp -- max_tokens_price
        ON mtp.epoch_timestamp = we.epoch_timestamp AND mtp.token_address = we.token_address
    LEFT JOIN db_analytics_prod.features_daily_token_prices as tp -- token_prices
        ON tp.timestamp = mtp.max_price_timestamp
        AND tp.address = mtp.token_address
    WHERE tm.decimals > 0
)

SELECT * FROM withdraw_events_with_quantity_in_eth WHERE year = '1970' AND month = '1'
