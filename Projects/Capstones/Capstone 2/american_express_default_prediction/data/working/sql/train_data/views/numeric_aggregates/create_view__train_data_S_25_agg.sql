
CREATE VIEW train_data_S_25_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_25 
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
    WHERE S_25 IS NOT NULL
    GROUP BY customer_ID
),
first_S_25 AS
(
    SELECT
        f.customer_ID, s.S_25 AS S_25_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_25 AS
(
    SELECT
        f.customer_ID, s.S_25 AS S_25_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_25_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_25_span
    FROM
        first_last
),
S_25_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_25,
        s.S_25 - LAG(s.S_25, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_25_delta
    FROM
        subset s
),
S_25_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_25_delta
    FROM
        S_25_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_25_delta_per_day AS
(
    SELECT
        customer_ID,
        S_25_delta / date_delta AS S_25_delta_per_day
    FROM
        S_25_delta
),
S_25_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_25_delta_per_day) AS S_25_delta_pd
    FROM
        S_25_delta_per_day
    GROUP BY
        customer_ID
),      
S_25_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_25_delta) AS S_25_delta_mean,
        MAX(S_25_delta) AS S_25_delta_max,
        MIN(S_25_delta) AS S_25_delta_min
    FROM
        S_25_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_25) AS S_25_mean,
        MIN(S_25) AS S_25_min, 
        MAX(S_25) AS S_25_max, 
        SUM(S_25) AS S_25_sum,
        COUNT(S_25) AS S_25_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_25_mean,
        a.S_25_min, 
        a.S_25_max, 
        a.S_25_sum,
        a.S_25_max - a.S_25_min AS S_25_range,
        a.S_25_count,
        f.S_25_first,
        l.S_25_last,
        d.S_25_delta_mean,
        d.S_25_delta_max,
        d.S_25_delta_min,
        pd.S_25_delta_pd,
        cs.S_25_span
    FROM
        aggs a
        LEFT JOIN first_S_25 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_25 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_25_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_25_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_25_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_25_mean, 
    v.S_25_min,
    v.S_25_max, 
    v.S_25_range,
    v.S_25_sum,
    ISNULL(v.S_25_count, 0) AS S_25_count,
    v.S_25_first, 
    v.S_25_last,
    v.S_25_delta_mean,
    v.S_25_delta_max,
    v.S_25_delta_min,
    v.S_25_delta_pd,
    v.S_25_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;