general_query = """
INSERT INTO db_stage_prod.transpose_{0}_events
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
FROM db_raw_prod.transpose_{0}_events raw
WHERE to_unixtime(raw.timestamp) > (select max(to_unixtime(timestamp)) from db_stage_prod.transpose_{0}_events)
"""


liquidation_query = """
INSERT INTO db_stage_prod.transpose_{0}_events
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
    LOWER(liquidated_token_address) AS liquidated_token_address,
    category,
    LOWER(account_address) AS account_address,
    LOWER(liquidator_address) AS liquidator_address,
    quantity,
    quantity_liquidated,
    LOWER(sender_address) AS sender_address,
    year,
    month
FROM db_raw_prod.transpose_{0}_events raw
WHERE to_unixtime(raw.timestamp) > (select max(to_unixtime(timestamp)) from db_stage_prod.transpose_{0}_events)
"""
