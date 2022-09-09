
CREATE VIEW train_data_S_26_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_26 
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
    WHERE S_26 IS NOT NULL
    GROUP BY customer_ID
),
first_S_26 AS
(
    SELECT
        f.customer_ID, s.S_26 AS S_26_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_26 AS
(
    SELECT
        f.customer_ID, s.S_26 AS S_26_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_26_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_26_span
    FROM
        first_last
),
S_26_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_26,
        s.S_26 - LAG(s.S_26, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_26_delta
    FROM
        subset s
),
S_26_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_26_delta
    FROM
        S_26_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_26_delta_per_day AS
(
    SELECT
        customer_ID,
        S_26_delta / date_delta AS S_26_delta_per_day
    FROM
        S_26_delta
),
S_26_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_26_delta_per_day) AS S_26_delta_pd
    FROM
        S_26_delta_per_day
    GROUP BY
        customer_ID
),      
S_26_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_26_delta) AS S_26_delta_mean,
        MAX(S_26_delta) AS S_26_delta_max,
        MIN(S_26_delta) AS S_26_delta_min
    FROM
        S_26_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_26) AS S_26_mean,
        MIN(S_26) AS S_26_min, 
        MAX(S_26) AS S_26_max, 
        SUM(S_26) AS S_26_sum,
        COUNT(S_26) AS S_26_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_26_mean,
        a.S_26_min, 
        a.S_26_max, 
        a.S_26_sum,
        a.S_26_max - a.S_26_min AS S_26_range,
        a.S_26_count,
        f.S_26_first,
        l.S_26_last,
        d.S_26_delta_mean,
        d.S_26_delta_max,
        d.S_26_delta_min,
        pd.S_26_delta_pd,
        cs.S_26_span
    FROM
        aggs a
        LEFT JOIN first_S_26 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_26 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_26_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_26_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_26_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_26_mean, 
    v.S_26_min,
    v.S_26_max, 
    v.S_26_range,
    v.S_26_sum,
    ISNULL(v.S_26_count, 0) AS S_26_count,
    v.S_26_first, 
    v.S_26_last,
    v.S_26_delta_mean,
    v.S_26_delta_max,
    v.S_26_delta_min,
    v.S_26_delta_pd,
    v.S_26_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;