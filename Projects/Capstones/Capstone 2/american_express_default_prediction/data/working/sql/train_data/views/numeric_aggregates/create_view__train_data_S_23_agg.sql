
CREATE VIEW train_data_S_23_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_23 
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
    WHERE S_23 IS NOT NULL
    GROUP BY customer_ID
),
first_S_23 AS
(
    SELECT
        f.customer_ID, s.S_23 AS S_23_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_23 AS
(
    SELECT
        f.customer_ID, s.S_23 AS S_23_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_23_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_23_span
    FROM
        first_last
),
S_23_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_23,
        s.S_23 - LAG(s.S_23, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_23_delta
    FROM
        subset s
),
S_23_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_23_delta
    FROM
        S_23_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_23_delta_per_day AS
(
    SELECT
        customer_ID,
        S_23_delta / date_delta AS S_23_delta_per_day
    FROM
        S_23_delta
),
S_23_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_23_delta_per_day) AS S_23_delta_pd
    FROM
        S_23_delta_per_day
    GROUP BY
        customer_ID
),      
S_23_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_23_delta) AS S_23_delta_mean,
        MAX(S_23_delta) AS S_23_delta_max,
        MIN(S_23_delta) AS S_23_delta_min
    FROM
        S_23_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_23) AS S_23_mean,
        MIN(S_23) AS S_23_min, 
        MAX(S_23) AS S_23_max, 
        SUM(S_23) AS S_23_sum,
        COUNT(S_23) AS S_23_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_23_mean,
        a.S_23_min, 
        a.S_23_max, 
        a.S_23_sum,
        a.S_23_max - a.S_23_min AS S_23_range,
        a.S_23_count,
        f.S_23_first,
        l.S_23_last,
        d.S_23_delta_mean,
        d.S_23_delta_max,
        d.S_23_delta_min,
        pd.S_23_delta_pd,
        cs.S_23_span
    FROM
        aggs a
        LEFT JOIN first_S_23 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_23 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_23_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_23_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_23_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_23_mean, 
    v.S_23_min,
    v.S_23_max, 
    v.S_23_range,
    v.S_23_sum,
    ISNULL(v.S_23_count, 0) AS S_23_count,
    v.S_23_first, 
    v.S_23_last,
    v.S_23_delta_mean,
    v.S_23_delta_max,
    v.S_23_delta_min,
    v.S_23_delta_pd,
    v.S_23_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;