
CREATE VIEW train_data_S_8_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_8 
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
    WHERE S_8 IS NOT NULL
    GROUP BY customer_ID
),
first_S_8 AS
(
    SELECT
        f.customer_ID, s.S_8 AS S_8_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_8 AS
(
    SELECT
        f.customer_ID, s.S_8 AS S_8_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_8_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_8_span
    FROM
        first_last
),
S_8_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_8,
        s.S_8 - LAG(s.S_8, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_8_delta
    FROM
        subset s
),
S_8_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_8_delta
    FROM
        S_8_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_8_delta_per_day AS
(
    SELECT
        customer_ID,
        S_8_delta / date_delta AS S_8_delta_per_day
    FROM
        S_8_delta
),
S_8_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_8_delta_per_day) AS S_8_delta_pd
    FROM
        S_8_delta_per_day
    GROUP BY
        customer_ID
),      
S_8_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_8_delta) AS S_8_delta_mean,
        MAX(S_8_delta) AS S_8_delta_max,
        MIN(S_8_delta) AS S_8_delta_min
    FROM
        S_8_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_8) AS S_8_mean,
        MIN(S_8) AS S_8_min, 
        MAX(S_8) AS S_8_max, 
        SUM(S_8) AS S_8_sum,
        COUNT(S_8) AS S_8_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_8_mean,
        a.S_8_min, 
        a.S_8_max, 
        a.S_8_sum,
        a.S_8_max - a.S_8_min AS S_8_range,
        a.S_8_count,
        f.S_8_first,
        l.S_8_last,
        d.S_8_delta_mean,
        d.S_8_delta_max,
        d.S_8_delta_min,
        pd.S_8_delta_pd,
        cs.S_8_span
    FROM
        aggs a
        LEFT JOIN first_S_8 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_8 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_8_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_8_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_8_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_8_mean, 
    v.S_8_min,
    v.S_8_max, 
    v.S_8_range,
    v.S_8_sum,
    ISNULL(v.S_8_count, 0) AS S_8_count,
    v.S_8_first, 
    v.S_8_last,
    v.S_8_delta_mean,
    v.S_8_delta_max,
    v.S_8_delta_min,
    v.S_8_delta_pd,
    v.S_8_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;