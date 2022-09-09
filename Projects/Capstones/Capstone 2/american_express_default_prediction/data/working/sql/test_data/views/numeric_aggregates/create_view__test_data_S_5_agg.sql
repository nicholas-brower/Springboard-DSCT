
CREATE VIEW test_data_S_5_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_5 
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
    WHERE S_5 IS NOT NULL
    GROUP BY customer_ID
),
first_S_5 AS
(
    SELECT
        f.customer_ID, s.S_5 AS S_5_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_5 AS
(
    SELECT
        f.customer_ID, s.S_5 AS S_5_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_5_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_5_span
    FROM
        first_last
),
S_5_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_5,
        s.S_5 - LAG(s.S_5, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_5_delta
    FROM
        subset s
),
S_5_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_5_delta
    FROM
        S_5_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_5_delta_per_day AS
(
    SELECT
        customer_ID,
        S_5_delta / date_delta AS S_5_delta_per_day
    FROM
        S_5_delta
),
S_5_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_5_delta_per_day) AS S_5_delta_pd
    FROM
        S_5_delta_per_day
    GROUP BY
        customer_ID
),      
S_5_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_5_delta) AS S_5_delta_mean,
        MAX(S_5_delta) AS S_5_delta_max,
        MIN(S_5_delta) AS S_5_delta_min
    FROM
        S_5_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_5) AS S_5_mean,
        MIN(S_5) AS S_5_min, 
        MAX(S_5) AS S_5_max, 
        SUM(S_5) AS S_5_sum,
        COUNT(S_5) AS S_5_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_5_mean,
        a.S_5_min, 
        a.S_5_max, 
        a.S_5_sum,
        a.S_5_max - a.S_5_min AS S_5_range,
        a.S_5_count,
        f.S_5_first,
        l.S_5_last,
        d.S_5_delta_mean,
        d.S_5_delta_max,
        d.S_5_delta_min,
        pd.S_5_delta_pd,
        cs.S_5_span
    FROM
        aggs a
        LEFT JOIN first_S_5 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_5 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_5_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_5_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_5_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_5_mean, 
    v.S_5_min,
    v.S_5_max, 
    v.S_5_range,
    v.S_5_sum,
    ISNULL(v.S_5_count, 0) AS S_5_count,
    v.S_5_first, 
    v.S_5_last,
    v.S_5_delta_mean,
    v.S_5_delta_max,
    v.S_5_delta_min,
    v.S_5_delta_pd,
    v.S_5_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;