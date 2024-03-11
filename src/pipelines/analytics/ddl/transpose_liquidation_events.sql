CREATE table target_database.table_name WITH (
	format = 'PARQUET',
	parquet_compression = 'SNAPPY',
	partitioned_by = array ['address_partition'],
	external_location = 'bucket_name/layer/data_source/table_name/'
) AS
WITH max_token_prices AS (
    SELECT le.epoch_timestamp,
        le.liquidated_token_address,
        MAX(tp.timestamp) as max_price_timestamp
    FROM db_analytics_prod.features_daily_token_prices AS tp -- tokens_price
    INNER JOIN db_stage_prod.transpose_liquidation_events AS le -- liquidation_events
        ON tp.address = le.liquidated_token_address
        AND tp.timestamp BETWEEN (le.epoch_timestamp - (86400 * 7)) AND le.epoch_timestamp -- get price from a week period
    GROUP BY le.epoch_timestamp, le.liquidated_token_address
),
liquidation_events_with_quantity_in_eth AS (
    SELECT
        le.block_number,
        le.log_index,
        le.transaction_hash,
        le.timestamp,
        le.epoch_timestamp,
        le.protocol_name,
        le.contract_version,
        le.market_address,
        le.liquidated_token_address,
        tm.decimals AS token_decimal,
        le.category,
        le.account_address,
        le.quantity_liquidated,
        CASE
            WHEN ttd.contract_address IS NOT NULL THEN 0 --tokens to drop
            WHEN le.liquidated_token_address = '0x0000000000000000000000000000000000000000'
            THEN le.quantity_liquidated / POWER(10, tm.decimals)
            ELSE (le.quantity_liquidated / POWER(10, tm.decimals)) * tp.price
        END AS quantity_in_eth,
        le.sender_address,
        le.account_address AS index_address,
        le.year,
        le.month,
        SUBSTR(le.account_address, 3, 2) AS address_partition
    FROM db_stage_prod.transpose_liquidation_events AS le -- liquidation_events
    INNER JOIN db_stage_prod.ethereum_tokens_metadata AS tm -- tokens_metadata
        ON tm.contract_address = le.liquidated_token_address
    LEFT JOIN db_sandbox_prod.defi_events_tokens_to_drop AS ttd -- tokens_to_drop
        ON ttd.contract_address = be.token_address
    LEFT JOIN max_token_prices AS mtp -- max_tokens_price
        ON mtp.epoch_timestamp = le.epoch_timestamp and mtp.liquidated_token_address = le.liquidated_token_address
    LEFT JOIN db_analytics_prod.features_daily_token_prices as tp -- token_prices
        ON tp.timestamp = mtp.max_price_timestamp
        AND tp.address = mtp.liquidated_token_address
    WHERE tm.decimals > 0
)

SELECT * FROM liquidation_events_with_quantity_in_eth WHERE year = '1970' AND month = '1'
