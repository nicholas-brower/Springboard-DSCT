
CREATE VIEW train_data_S_17_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_17 
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
    WHERE S_17 IS NOT NULL
    GROUP BY customer_ID
),
first_S_17 AS
(
    SELECT
        f.customer_ID, s.S_17 AS S_17_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_17 AS
(
    SELECT
        f.customer_ID, s.S_17 AS S_17_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_17_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_17_span
    FROM
        first_last
),
S_17_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_17,
        s.S_17 - LAG(s.S_17, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_17_delta
    FROM
        subset s
),
S_17_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_17_delta
    FROM
        S_17_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_17_delta_per_day AS
(
    SELECT
        customer_ID,
        S_17_delta / date_delta AS S_17_delta_per_day
    FROM
        S_17_delta
),
S_17_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_17_delta_per_day) AS S_17_delta_pd
    FROM
        S_17_delta_per_day
    GROUP BY
        customer_ID
),      
S_17_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_17_delta) AS S_17_delta_mean,
        MAX(S_17_delta) AS S_17_delta_max,
        MIN(S_17_delta) AS S_17_delta_min
    FROM
        S_17_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_17) AS S_17_mean,
        MIN(S_17) AS S_17_min, 
        MAX(S_17) AS S_17_max, 
        SUM(S_17) AS S_17_sum,
        COUNT(S_17) AS S_17_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_17_mean,
        a.S_17_min, 
        a.S_17_max, 
        a.S_17_sum,
        a.S_17_max - a.S_17_min AS S_17_range,
        a.S_17_count,
        f.S_17_first,
        l.S_17_last,
        d.S_17_delta_mean,
        d.S_17_delta_max,
        d.S_17_delta_min,
        pd.S_17_delta_pd,
        cs.S_17_span
    FROM
        aggs a
        LEFT JOIN first_S_17 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_17 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_17_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_17_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_17_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_17_mean, 
    v.S_17_min,
    v.S_17_max, 
    v.S_17_range,
    v.S_17_sum,
    ISNULL(v.S_17_count, 0) AS S_17_count,
    v.S_17_first, 
    v.S_17_last,
    v.S_17_delta_mean,
    v.S_17_delta_max,
    v.S_17_delta_min,
    v.S_17_delta_pd,
    v.S_17_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;