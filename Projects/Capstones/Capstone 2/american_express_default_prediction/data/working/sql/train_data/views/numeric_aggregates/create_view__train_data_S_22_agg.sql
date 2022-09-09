
CREATE VIEW train_data_S_22_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_22 
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
    WHERE S_22 IS NOT NULL
    GROUP BY customer_ID
),
first_S_22 AS
(
    SELECT
        f.customer_ID, s.S_22 AS S_22_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_22 AS
(
    SELECT
        f.customer_ID, s.S_22 AS S_22_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_22_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_22_span
    FROM
        first_last
),
S_22_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_22,
        s.S_22 - LAG(s.S_22, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_22_delta
    FROM
        subset s
),
S_22_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_22_delta
    FROM
        S_22_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_22_delta_per_day AS
(
    SELECT
        customer_ID,
        S_22_delta / date_delta AS S_22_delta_per_day
    FROM
        S_22_delta
),
S_22_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_22_delta_per_day) AS S_22_delta_pd
    FROM
        S_22_delta_per_day
    GROUP BY
        customer_ID
),      
S_22_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_22_delta) AS S_22_delta_mean,
        MAX(S_22_delta) AS S_22_delta_max,
        MIN(S_22_delta) AS S_22_delta_min
    FROM
        S_22_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_22) AS S_22_mean,
        MIN(S_22) AS S_22_min, 
        MAX(S_22) AS S_22_max, 
        SUM(S_22) AS S_22_sum,
        COUNT(S_22) AS S_22_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_22_mean,
        a.S_22_min, 
        a.S_22_max, 
        a.S_22_sum,
        a.S_22_max - a.S_22_min AS S_22_range,
        a.S_22_count,
        f.S_22_first,
        l.S_22_last,
        d.S_22_delta_mean,
        d.S_22_delta_max,
        d.S_22_delta_min,
        pd.S_22_delta_pd,
        cs.S_22_span
    FROM
        aggs a
        LEFT JOIN first_S_22 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_22 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_22_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_22_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_22_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_22_mean, 
    v.S_22_min,
    v.S_22_max, 
    v.S_22_range,
    v.S_22_sum,
    ISNULL(v.S_22_count, 0) AS S_22_count,
    v.S_22_first, 
    v.S_22_last,
    v.S_22_delta_mean,
    v.S_22_delta_max,
    v.S_22_delta_min,
    v.S_22_delta_pd,
    v.S_22_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;