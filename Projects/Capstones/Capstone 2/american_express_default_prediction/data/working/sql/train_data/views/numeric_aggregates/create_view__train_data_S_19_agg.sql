
CREATE VIEW train_data_S_19_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_19 
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
    WHERE S_19 IS NOT NULL
    GROUP BY customer_ID
),
first_S_19 AS
(
    SELECT
        f.customer_ID, s.S_19 AS S_19_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_19 AS
(
    SELECT
        f.customer_ID, s.S_19 AS S_19_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_19_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_19_span
    FROM
        first_last
),
S_19_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_19,
        s.S_19 - LAG(s.S_19, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_19_delta
    FROM
        subset s
),
S_19_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_19_delta
    FROM
        S_19_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_19_delta_per_day AS
(
    SELECT
        customer_ID,
        S_19_delta / date_delta AS S_19_delta_per_day
    FROM
        S_19_delta
),
S_19_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_19_delta_per_day) AS S_19_delta_pd
    FROM
        S_19_delta_per_day
    GROUP BY
        customer_ID
),      
S_19_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_19_delta) AS S_19_delta_mean,
        MAX(S_19_delta) AS S_19_delta_max,
        MIN(S_19_delta) AS S_19_delta_min
    FROM
        S_19_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_19) AS S_19_mean,
        MIN(S_19) AS S_19_min, 
        MAX(S_19) AS S_19_max, 
        SUM(S_19) AS S_19_sum,
        COUNT(S_19) AS S_19_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_19_mean,
        a.S_19_min, 
        a.S_19_max, 
        a.S_19_sum,
        a.S_19_max - a.S_19_min AS S_19_range,
        a.S_19_count,
        f.S_19_first,
        l.S_19_last,
        d.S_19_delta_mean,
        d.S_19_delta_max,
        d.S_19_delta_min,
        pd.S_19_delta_pd,
        cs.S_19_span
    FROM
        aggs a
        LEFT JOIN first_S_19 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_19 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_19_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_19_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_19_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_19_mean, 
    v.S_19_min,
    v.S_19_max, 
    v.S_19_range,
    v.S_19_sum,
    ISNULL(v.S_19_count, 0) AS S_19_count,
    v.S_19_first, 
    v.S_19_last,
    v.S_19_delta_mean,
    v.S_19_delta_max,
    v.S_19_delta_min,
    v.S_19_delta_pd,
    v.S_19_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;