
CREATE VIEW test_data_S_20_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_20 
    FROM
        test_data td 
        
),
first_last AS
(
    SELECT 
        customer_ID, 
        MIN(S_2) AS first_dt, 
        MAX(S_2) AS last_dt
    FROM subset
    WHERE S_20 IS NOT NULL
    GROUP BY customer_ID
),
first_S_20 AS
(
    SELECT
        f.customer_ID, s.S_20 AS S_20_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_20 AS
(
    SELECT
        f.customer_ID, s.S_20 AS S_20_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_20_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_20_span
    FROM
        first_last
),
S_20_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_20,
        s.S_20 - LAG(s.S_20, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_20_delta
    FROM
        subset s
),
S_20_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_20_delta
    FROM
        S_20_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_20_delta_per_day AS
(
    SELECT
        customer_ID,
        S_20_delta / date_delta AS S_20_delta_per_day
    FROM
        S_20_delta
),
S_20_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_20_delta_per_day) AS S_20_delta_pd
    FROM
        S_20_delta_per_day
    GROUP BY
        customer_ID
),      
S_20_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_20_delta) AS S_20_delta_mean,
        MAX(S_20_delta) AS S_20_delta_max,
        MIN(S_20_delta) AS S_20_delta_min
    FROM
        S_20_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_20) AS S_20_mean,
        MIN(S_20) AS S_20_min, 
        MAX(S_20) AS S_20_max, 
        SUM(S_20) AS S_20_sum,
        COUNT(S_20) AS S_20_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_20_mean,
        a.S_20_min, 
        a.S_20_max, 
        a.S_20_sum,
        a.S_20_max - a.S_20_min AS S_20_range,
        a.S_20_count,
        f.S_20_first,
        l.S_20_last,
        d.S_20_delta_mean,
        d.S_20_delta_max,
        d.S_20_delta_min,
        pd.S_20_delta_pd,
        cs.S_20_span
    FROM
        aggs a
        LEFT JOIN first_S_20 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_20 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_20_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_20_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_20_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_20_mean, 
    v.S_20_min,
    v.S_20_max, 
    v.S_20_range,
    v.S_20_sum,
    ISNULL(v.S_20_count, 0) AS S_20_count,
    v.S_20_first, 
    v.S_20_last,
    v.S_20_delta_mean,
    v.S_20_delta_max,
    v.S_20_delta_min,
    v.S_20_delta_pd,
    v.S_20_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;