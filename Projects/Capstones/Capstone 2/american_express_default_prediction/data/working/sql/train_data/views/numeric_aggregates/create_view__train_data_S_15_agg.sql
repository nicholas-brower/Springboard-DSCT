
CREATE VIEW train_data_S_15_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_15 
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
    WHERE S_15 IS NOT NULL
    GROUP BY customer_ID
),
first_S_15 AS
(
    SELECT
        f.customer_ID, s.S_15 AS S_15_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_15 AS
(
    SELECT
        f.customer_ID, s.S_15 AS S_15_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_15_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_15_span
    FROM
        first_last
),
S_15_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_15,
        s.S_15 - LAG(s.S_15, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_15_delta
    FROM
        subset s
),
S_15_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_15_delta
    FROM
        S_15_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_15_delta_per_day AS
(
    SELECT
        customer_ID,
        S_15_delta / date_delta AS S_15_delta_per_day
    FROM
        S_15_delta
),
S_15_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_15_delta_per_day) AS S_15_delta_pd
    FROM
        S_15_delta_per_day
    GROUP BY
        customer_ID
),      
S_15_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_15_delta) AS S_15_delta_mean,
        MAX(S_15_delta) AS S_15_delta_max,
        MIN(S_15_delta) AS S_15_delta_min
    FROM
        S_15_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_15) AS S_15_mean,
        MIN(S_15) AS S_15_min, 
        MAX(S_15) AS S_15_max, 
        SUM(S_15) AS S_15_sum,
        COUNT(S_15) AS S_15_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_15_mean,
        a.S_15_min, 
        a.S_15_max, 
        a.S_15_sum,
        a.S_15_max - a.S_15_min AS S_15_range,
        a.S_15_count,
        f.S_15_first,
        l.S_15_last,
        d.S_15_delta_mean,
        d.S_15_delta_max,
        d.S_15_delta_min,
        pd.S_15_delta_pd,
        cs.S_15_span
    FROM
        aggs a
        LEFT JOIN first_S_15 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_15 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_15_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_15_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_15_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_15_mean, 
    v.S_15_min,
    v.S_15_max, 
    v.S_15_range,
    v.S_15_sum,
    ISNULL(v.S_15_count, 0) AS S_15_count,
    v.S_15_first, 
    v.S_15_last,
    v.S_15_delta_mean,
    v.S_15_delta_max,
    v.S_15_delta_min,
    v.S_15_delta_pd,
    v.S_15_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;