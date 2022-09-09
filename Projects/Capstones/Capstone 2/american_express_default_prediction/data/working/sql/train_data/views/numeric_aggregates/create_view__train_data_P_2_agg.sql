
CREATE VIEW train_data_P_2_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.P_2 
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
    WHERE P_2 IS NOT NULL
    GROUP BY customer_ID
),
first_P_2 AS
(
    SELECT
        f.customer_ID, s.P_2 AS P_2_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_P_2 AS
(
    SELECT
        f.customer_ID, s.P_2 AS P_2_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
P_2_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS P_2_span
    FROM
        first_last
),
P_2_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.P_2,
        s.P_2 - LAG(s.P_2, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS P_2_delta
    FROM
        subset s
),
P_2_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.P_2_delta
    FROM
        P_2_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
P_2_delta_per_day AS
(
    SELECT
        customer_ID,
        P_2_delta / date_delta AS P_2_delta_per_day
    FROM
        P_2_delta
),
P_2_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(P_2_delta_per_day) AS P_2_delta_pd
    FROM
        P_2_delta_per_day
    GROUP BY
        customer_ID
),      
P_2_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(P_2_delta) AS P_2_delta_mean,
        MAX(P_2_delta) AS P_2_delta_max,
        MIN(P_2_delta) AS P_2_delta_min
    FROM
        P_2_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(P_2) AS P_2_mean,
        MIN(P_2) AS P_2_min, 
        MAX(P_2) AS P_2_max, 
        SUM(P_2) AS P_2_sum,
        COUNT(P_2) AS P_2_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.P_2_mean,
        a.P_2_min, 
        a.P_2_max, 
        a.P_2_sum,
        a.P_2_max - a.P_2_min AS P_2_range,
        a.P_2_count,
        f.P_2_first,
        l.P_2_last,
        d.P_2_delta_mean,
        d.P_2_delta_max,
        d.P_2_delta_min,
        pd.P_2_delta_pd,
        cs.P_2_span
    FROM
        aggs a
        LEFT JOIN first_P_2 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_P_2 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN P_2_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN P_2_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN P_2_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.P_2_mean, 
    v.P_2_min,
    v.P_2_max, 
    v.P_2_range,
    v.P_2_sum,
    ISNULL(v.P_2_count, 0) AS P_2_count,
    v.P_2_first, 
    v.P_2_last,
    v.P_2_delta_mean,
    v.P_2_delta_max,
    v.P_2_delta_min,
    v.P_2_delta_pd,
    v.P_2_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;