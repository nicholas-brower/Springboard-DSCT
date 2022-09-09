
CREATE VIEW train_data_P_3_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.P_3 
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
    WHERE P_3 IS NOT NULL
    GROUP BY customer_ID
),
first_P_3 AS
(
    SELECT
        f.customer_ID, s.P_3 AS P_3_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_P_3 AS
(
    SELECT
        f.customer_ID, s.P_3 AS P_3_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
P_3_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS P_3_span
    FROM
        first_last
),
P_3_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.P_3,
        s.P_3 - LAG(s.P_3, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS P_3_delta
    FROM
        subset s
),
P_3_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.P_3_delta
    FROM
        P_3_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
P_3_delta_per_day AS
(
    SELECT
        customer_ID,
        P_3_delta / date_delta AS P_3_delta_per_day
    FROM
        P_3_delta
),
P_3_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(P_3_delta_per_day) AS P_3_delta_pd
    FROM
        P_3_delta_per_day
    GROUP BY
        customer_ID
),      
P_3_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(P_3_delta) AS P_3_delta_mean,
        MAX(P_3_delta) AS P_3_delta_max,
        MIN(P_3_delta) AS P_3_delta_min
    FROM
        P_3_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(P_3) AS P_3_mean,
        MIN(P_3) AS P_3_min, 
        MAX(P_3) AS P_3_max, 
        SUM(P_3) AS P_3_sum,
        COUNT(P_3) AS P_3_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.P_3_mean,
        a.P_3_min, 
        a.P_3_max, 
        a.P_3_sum,
        a.P_3_max - a.P_3_min AS P_3_range,
        a.P_3_count,
        f.P_3_first,
        l.P_3_last,
        d.P_3_delta_mean,
        d.P_3_delta_max,
        d.P_3_delta_min,
        pd.P_3_delta_pd,
        cs.P_3_span
    FROM
        aggs a
        LEFT JOIN first_P_3 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_P_3 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN P_3_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN P_3_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN P_3_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.P_3_mean, 
    v.P_3_min,
    v.P_3_max, 
    v.P_3_range,
    v.P_3_sum,
    ISNULL(v.P_3_count, 0) AS P_3_count,
    v.P_3_first, 
    v.P_3_last,
    v.P_3_delta_mean,
    v.P_3_delta_max,
    v.P_3_delta_min,
    v.P_3_delta_pd,
    v.P_3_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;