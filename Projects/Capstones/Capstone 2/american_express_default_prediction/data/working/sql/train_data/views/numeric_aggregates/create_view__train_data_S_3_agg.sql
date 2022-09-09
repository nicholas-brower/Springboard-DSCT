
CREATE VIEW train_data_S_3_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_3 
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
    WHERE S_3 IS NOT NULL
    GROUP BY customer_ID
),
first_S_3 AS
(
    SELECT
        f.customer_ID, s.S_3 AS S_3_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_3 AS
(
    SELECT
        f.customer_ID, s.S_3 AS S_3_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_3_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_3_span
    FROM
        first_last
),
S_3_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_3,
        s.S_3 - LAG(s.S_3, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_3_delta
    FROM
        subset s
),
S_3_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_3_delta
    FROM
        S_3_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_3_delta_per_day AS
(
    SELECT
        customer_ID,
        S_3_delta / date_delta AS S_3_delta_per_day
    FROM
        S_3_delta
),
S_3_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_3_delta_per_day) AS S_3_delta_pd
    FROM
        S_3_delta_per_day
    GROUP BY
        customer_ID
),      
S_3_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_3_delta) AS S_3_delta_mean,
        MAX(S_3_delta) AS S_3_delta_max,
        MIN(S_3_delta) AS S_3_delta_min
    FROM
        S_3_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_3) AS S_3_mean,
        MIN(S_3) AS S_3_min, 
        MAX(S_3) AS S_3_max, 
        SUM(S_3) AS S_3_sum,
        COUNT(S_3) AS S_3_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_3_mean,
        a.S_3_min, 
        a.S_3_max, 
        a.S_3_sum,
        a.S_3_max - a.S_3_min AS S_3_range,
        a.S_3_count,
        f.S_3_first,
        l.S_3_last,
        d.S_3_delta_mean,
        d.S_3_delta_max,
        d.S_3_delta_min,
        pd.S_3_delta_pd,
        cs.S_3_span
    FROM
        aggs a
        LEFT JOIN first_S_3 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_3 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_3_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_3_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_3_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_3_mean, 
    v.S_3_min,
    v.S_3_max, 
    v.S_3_range,
    v.S_3_sum,
    ISNULL(v.S_3_count, 0) AS S_3_count,
    v.S_3_first, 
    v.S_3_last,
    v.S_3_delta_mean,
    v.S_3_delta_max,
    v.S_3_delta_min,
    v.S_3_delta_pd,
    v.S_3_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;