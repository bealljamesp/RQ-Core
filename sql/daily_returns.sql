WITH daily_returns AS (
    SELECT 
        trade_date, 
        adj_close, 
        -- Calculate the percentage change (Return)
        (adj_close / LAG(adj_close) OVER (ORDER BY trade_date) - 1) AS raw_return
    FROM daily_metrics
    WHERE security_id = 2
)
SELECT * FROM daily_returns
-- Now we can filter because 'raw_return' is a defined column
WHERE raw_return < -0.0408
ORDER BY raw_return ASC;