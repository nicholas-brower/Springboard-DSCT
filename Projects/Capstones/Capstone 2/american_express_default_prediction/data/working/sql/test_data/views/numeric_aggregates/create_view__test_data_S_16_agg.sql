
CREATE VIEW test_data_S_16_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_16 
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
    WHERE S_16 IS NOT NULL
    GROUP BY customer_ID
),
first_S_16 AS
(
    SELECT
        f.customer_ID, s.S_16 AS S_16_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_16 AS
(
    SELECT
        f.customer_ID, s.S_16 AS S_16_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_16_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_16_span
    FROM
        first_last
),
S_16_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_16,
        s.S_16 - LAG(s.S_16, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_16_delta
    FROM
        subset s
),
S_16_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_16_delta
    FROM
        S_16_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_16_delta_per_day AS
(
    SELECT
        customer_ID,
        S_16_delta / date_delta AS S_16_delta_per_day
    FROM
        S_16_delta
),
S_16_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_16_delta_per_day) AS S_16_delta_pd
    FROM
        S_16_delta_per_day
    GROUP BY
        customer_ID
),      
S_16_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_16_delta) AS S_16_delta_mean,
        MAX(S_16_delta) AS S_16_delta_max,
        MIN(S_16_delta) AS S_16_delta_min
    FROM
        S_16_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_16) AS S_16_mean,
        MIN(S_16) AS S_16_min, 
        MAX(S_16) AS S_16_max, 
        SUM(S_16) AS S_16_sum,
        COUNT(S_16) AS S_16_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_16_mean,
        a.S_16_min, 
        a.S_16_max, 
        a.S_16_sum,
        a.S_16_max - a.S_16_min AS S_16_range,
        a.S_16_count,
        f.S_16_first,
        l.S_16_last,
        d.S_16_delta_mean,
        d.S_16_delta_max,
        d.S_16_delta_min,
        pd.S_16_delta_pd,
        cs.S_16_span
    FROM
        aggs a
        LEFT JOIN first_S_16 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_16 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_16_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_16_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_16_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_16_mean, 
    v.S_16_min,
    v.S_16_max, 
    v.S_16_range,
    v.S_16_sum,
    ISNULL(v.S_16_count, 0) AS S_16_count,
    v.S_16_first, 
    v.S_16_last,
    v.S_16_delta_mean,
    v.S_16_delta_max,
    v.S_16_delta_min,
    v.S_16_delta_pd,
    v.S_16_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;