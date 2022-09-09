
CREATE VIEW test_data_S_13_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_13 
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
    WHERE S_13 IS NOT NULL
    GROUP BY customer_ID
),
first_S_13 AS
(
    SELECT
        f.customer_ID, s.S_13 AS S_13_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_13 AS
(
    SELECT
        f.customer_ID, s.S_13 AS S_13_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_13_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_13_span
    FROM
        first_last
),
S_13_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_13,
        s.S_13 - LAG(s.S_13, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_13_delta
    FROM
        subset s
),
S_13_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_13_delta
    FROM
        S_13_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_13_delta_per_day AS
(
    SELECT
        customer_ID,
        S_13_delta / date_delta AS S_13_delta_per_day
    FROM
        S_13_delta
),
S_13_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_13_delta_per_day) AS S_13_delta_pd
    FROM
        S_13_delta_per_day
    GROUP BY
        customer_ID
),      
S_13_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_13_delta) AS S_13_delta_mean,
        MAX(S_13_delta) AS S_13_delta_max,
        MIN(S_13_delta) AS S_13_delta_min
    FROM
        S_13_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_13) AS S_13_mean,
        MIN(S_13) AS S_13_min, 
        MAX(S_13) AS S_13_max, 
        SUM(S_13) AS S_13_sum,
        COUNT(S_13) AS S_13_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_13_mean,
        a.S_13_min, 
        a.S_13_max, 
        a.S_13_sum,
        a.S_13_max - a.S_13_min AS S_13_range,
        a.S_13_count,
        f.S_13_first,
        l.S_13_last,
        d.S_13_delta_mean,
        d.S_13_delta_max,
        d.S_13_delta_min,
        pd.S_13_delta_pd,
        cs.S_13_span
    FROM
        aggs a
        LEFT JOIN first_S_13 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_13 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_13_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_13_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_13_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_13_mean, 
    v.S_13_min,
    v.S_13_max, 
    v.S_13_range,
    v.S_13_sum,
    ISNULL(v.S_13_count, 0) AS S_13_count,
    v.S_13_first, 
    v.S_13_last,
    v.S_13_delta_mean,
    v.S_13_delta_max,
    v.S_13_delta_min,
    v.S_13_delta_pd,
    v.S_13_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;