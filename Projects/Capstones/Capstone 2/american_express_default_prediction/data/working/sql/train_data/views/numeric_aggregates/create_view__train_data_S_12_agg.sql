
CREATE VIEW train_data_S_12_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_12 
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
    WHERE S_12 IS NOT NULL
    GROUP BY customer_ID
),
first_S_12 AS
(
    SELECT
        f.customer_ID, s.S_12 AS S_12_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_12 AS
(
    SELECT
        f.customer_ID, s.S_12 AS S_12_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_12_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_12_span
    FROM
        first_last
),
S_12_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_12,
        s.S_12 - LAG(s.S_12, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_12_delta
    FROM
        subset s
),
S_12_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_12_delta
    FROM
        S_12_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_12_delta_per_day AS
(
    SELECT
        customer_ID,
        S_12_delta / date_delta AS S_12_delta_per_day
    FROM
        S_12_delta
),
S_12_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_12_delta_per_day) AS S_12_delta_pd
    FROM
        S_12_delta_per_day
    GROUP BY
        customer_ID
),      
S_12_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_12_delta) AS S_12_delta_mean,
        MAX(S_12_delta) AS S_12_delta_max,
        MIN(S_12_delta) AS S_12_delta_min
    FROM
        S_12_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_12) AS S_12_mean,
        MIN(S_12) AS S_12_min, 
        MAX(S_12) AS S_12_max, 
        SUM(S_12) AS S_12_sum,
        COUNT(S_12) AS S_12_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_12_mean,
        a.S_12_min, 
        a.S_12_max, 
        a.S_12_sum,
        a.S_12_max - a.S_12_min AS S_12_range,
        a.S_12_count,
        f.S_12_first,
        l.S_12_last,
        d.S_12_delta_mean,
        d.S_12_delta_max,
        d.S_12_delta_min,
        pd.S_12_delta_pd,
        cs.S_12_span
    FROM
        aggs a
        LEFT JOIN first_S_12 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_12 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_12_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_12_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_12_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_12_mean, 
    v.S_12_min,
    v.S_12_max, 
    v.S_12_range,
    v.S_12_sum,
    ISNULL(v.S_12_count, 0) AS S_12_count,
    v.S_12_first, 
    v.S_12_last,
    v.S_12_delta_mean,
    v.S_12_delta_max,
    v.S_12_delta_min,
    v.S_12_delta_pd,
    v.S_12_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;