WITH
    markets
    AS
    (
        SELECT fixedProductMarketMaker
        FROM polymarketfactory_polygon.FixedProductMarketMakerFactory_evt_FixedProductMarketMakerCreation
        WHERE collateralToken = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174
    ),

    log_events
    AS
    (
        SELECT block_time,
            bytearray_to_uint256(substr(DATA, 1, 32)) / 1e6 AS usd
        FROM polygon.logs
        WHERE topic0 IN (0x4f62630f51608fc8a7603a9391a5101e58bd7c276139366fc107dc3b67c3dcf8, 0xadcf2a240ed9300d681d9a3f5382b6c1beed1b7e46643e0c7b42cbe6e2d766b4)
            AND contract_address IN (SELECT fixedProductMarketMaker
            FROM markets)
            AND block_number >= 4023680
    ),

    order_filled
    AS
    (
                    SELECT evt_block_time AS block_time,
                CASE
             WHEN makerAssetId = 0 THEN makerAmountFilled
             WHEN takerAssetId = 0 THEN takerAmountFilled
         END / 1e6 AS usd
            FROM polymarket_polygon.CTFExchange_evt_OrderFilled
        UNION ALL
            SELECT evt_block_time AS block_time,
                CASE
             WHEN makerAssetId = 0 THEN makerAmountFilled
             WHEN takerAssetId = 0 THEN takerAmountFilled
         END / 1e6 AS usd
            FROM polymarket_polygon.NegRiskCtfExchange_evt_OrderFilled
    ),

    combined_events
    AS
    (
                    SELECT block_time, usd
            FROM log_events
        UNION ALL
            SELECT block_time, usd
            FROM order_filled
    ),

    weekly_volume
    AS
    (
        SELECT date_trunc('week', block_time) AS week,
            SUM(usd) AS total_usd
        FROM combined_events
        GROUP BY date_trunc('week', block_time)
        ORDER BY week
    )

SELECT week,
    total_usd,
    SUM(total_usd) OVER (ORDER BY week) AS cumulative_usd
FROM weekly_volume;