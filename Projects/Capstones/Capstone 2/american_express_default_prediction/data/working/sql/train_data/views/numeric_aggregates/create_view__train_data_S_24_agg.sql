
CREATE VIEW train_data_S_24_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_24 
    FROM
        train_data td 
        
),
first_last AS
(
    SELECT 
        customer_ID, 
        MIN(S_2) AS first_dt, 
        MAX(S_2) AS last_dt
    FROM subset
    WHERE S_24 IS NOT NULL
    GROUP BY customer_ID
),
first_S_24 AS
(
    SELECT
        f.customer_ID, s.S_24 AS S_24_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_24 AS
(
    SELECT
        f.customer_ID, s.S_24 AS S_24_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_24_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_24_span
    FROM
        first_last
),
S_24_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_24,
        s.S_24 - LAG(s.S_24, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_24_delta
    FROM
        subset s
),
S_24_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_24_delta
    FROM
        S_24_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_24_delta_per_day AS
(
    SELECT
        customer_ID,
        S_24_delta / date_delta AS S_24_delta_per_day
    FROM
        S_24_delta
),
S_24_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_24_delta_per_day) AS S_24_delta_pd
    FROM
        S_24_delta_per_day
    GROUP BY
        customer_ID
),      
S_24_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_24_delta) AS S_24_delta_mean,
        MAX(S_24_delta) AS S_24_delta_max,
        MIN(S_24_delta) AS S_24_delta_min
    FROM
        S_24_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_24) AS S_24_mean,
        MIN(S_24) AS S_24_min, 
        MAX(S_24) AS S_24_max, 
        SUM(S_24) AS S_24_sum,
        COUNT(S_24) AS S_24_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_24_mean,
        a.S_24_min, 
        a.S_24_max, 
        a.S_24_sum,
        a.S_24_max - a.S_24_min AS S_24_range,
        a.S_24_count,
        f.S_24_first,
        l.S_24_last,
        d.S_24_delta_mean,
        d.S_24_delta_max,
        d.S_24_delta_min,
        pd.S_24_delta_pd,
        cs.S_24_span
    FROM
        aggs a
        LEFT JOIN first_S_24 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_24 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_24_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_24_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_24_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_24_mean, 
    v.S_24_min,
    v.S_24_max, 
    v.S_24_range,
    v.S_24_sum,
    ISNULL(v.S_24_count, 0) AS S_24_count,
    v.S_24_first, 
    v.S_24_last,
    v.S_24_delta_mean,
    v.S_24_delta_max,
    v.S_24_delta_min,
    v.S_24_delta_pd,
    v.S_24_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;