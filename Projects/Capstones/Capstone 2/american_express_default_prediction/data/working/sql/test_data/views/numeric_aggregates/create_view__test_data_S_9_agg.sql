
CREATE VIEW test_data_S_9_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_9 
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
    WHERE S_9 IS NOT NULL
    GROUP BY customer_ID
),
first_S_9 AS
(
    SELECT
        f.customer_ID, s.S_9 AS S_9_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_9 AS
(
    SELECT
        f.customer_ID, s.S_9 AS S_9_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_9_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_9_span
    FROM
        first_last
),
S_9_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_9,
        s.S_9 - LAG(s.S_9, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_9_delta
    FROM
        subset s
),
S_9_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_9_delta
    FROM
        S_9_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_9_delta_per_day AS
(
    SELECT
        customer_ID,
        S_9_delta / date_delta AS S_9_delta_per_day
    FROM
        S_9_delta
),
S_9_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_9_delta_per_day) AS S_9_delta_pd
    FROM
        S_9_delta_per_day
    GROUP BY
        customer_ID
),      
S_9_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_9_delta) AS S_9_delta_mean,
        MAX(S_9_delta) AS S_9_delta_max,
        MIN(S_9_delta) AS S_9_delta_min
    FROM
        S_9_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_9) AS S_9_mean,
        MIN(S_9) AS S_9_min, 
        MAX(S_9) AS S_9_max, 
        SUM(S_9) AS S_9_sum,
        COUNT(S_9) AS S_9_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_9_mean,
        a.S_9_min, 
        a.S_9_max, 
        a.S_9_sum,
        a.S_9_max - a.S_9_min AS S_9_range,
        a.S_9_count,
        f.S_9_first,
        l.S_9_last,
        d.S_9_delta_mean,
        d.S_9_delta_max,
        d.S_9_delta_min,
        pd.S_9_delta_pd,
        cs.S_9_span
    FROM
        aggs a
        LEFT JOIN first_S_9 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_9 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_9_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_9_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_9_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_9_mean, 
    v.S_9_min,
    v.S_9_max, 
    v.S_9_range,
    v.S_9_sum,
    ISNULL(v.S_9_count, 0) AS S_9_count,
    v.S_9_first, 
    v.S_9_last,
    v.S_9_delta_mean,
    v.S_9_delta_max,
    v.S_9_delta_min,
    v.S_9_delta_pd,
    v.S_9_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;