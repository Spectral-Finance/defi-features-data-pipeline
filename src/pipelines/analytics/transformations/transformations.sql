INSERT INTO db_analytics_prod.transpose_{event_name}_events
WITH max_token_prices AS (
    SELECT tb.epoch_timestamp,
        tb.{token_column},
        MAX(tp.timestamp) as max_price_timestamp
    FROM db_analytics_prod.features_daily_token_prices AS tp -- tokens_price
    INNER JOIN db_stage_prod.transpose_{event_name}_events AS tb -- borrow_events
        ON tp.address = tb.{token_column}
        AND tp.timestamp BETWEEN (tb.epoch_timestamp - (86400 * 7)) AND tb.epoch_timestamp -- get price from a week period
    WHERE tb.epoch_timestamp > {last_timestamp}
    AND SUBSTR(tb.{index_column}, 3, 2) IN {address_partitions}
    GROUP BY tb.epoch_timestamp, tb.{token_column}
),
{event_name}_events_with_quantity_in_eth AS (
    SELECT
        tb.block_number,
        tb.log_index,
        tb.transaction_hash,
        tb.timestamp,
        tb.epoch_timestamp,
        tb.protocol_name,
        tb.contract_version,
        tb.market_address,
        tb.{token_column},
        tm.decimals AS token_decimal,
        tb.category,
        tb.account_address,
        tb.{quantity_column},
        CASE
            WHEN tb.{token_column} = '0x0000000000000000000000000000000000000000'
            THEN tb.{quantity_column} / POWER(10, tm.decimals)
            ELSE (tb.{quantity_column} / POWER(10, tm.decimals)) * tp.price
        END AS quantity_in_eth,
        tb.sender_address,
        tb.{index_column} AS index_address,
        tb.year,
        tb.month,
        SUBSTR(tb.{index_column}, 3, 2) AS address_partition
    FROM db_stage_prod.transpose_{event_name}_events AS tb
    INNER JOIN db_stage_prod.ethereum_tokens_metadata AS tm -- tokens_metadata
        ON tm.contract_address = tb.{token_column}
    LEFT JOIN db_sandbox_prod.defi_events_tokens_to_drop AS ttd -- tokens_to_drop
        ON ttd.contract_address = tb.{token_column}
    LEFT JOIN max_token_prices AS mtp -- max_tokens_price
        ON mtp.epoch_timestamp = tb.epoch_timestamp AND mtp.{token_column} = tb.{token_column}
    LEFT JOIN db_analytics_prod.features_daily_token_prices as tp -- token_prices
        ON tp.timestamp = mtp.max_price_timestamp
        AND tp.address = mtp.{token_column}
    WHERE tb.epoch_timestamp > {last_timestamp}
    AND SUBSTR(tb.{index_column}, 3, 2) IN {address_partitions}
    AND ttd.contract_address IS NULL
    AND tm.decimals > 0
)
SELECT * FROM {event_name}_events_with_quantity_in_eth
