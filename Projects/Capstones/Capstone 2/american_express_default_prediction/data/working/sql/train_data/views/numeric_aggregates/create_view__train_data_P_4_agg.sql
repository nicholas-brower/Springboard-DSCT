
CREATE VIEW train_data_P_4_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.P_4 
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
    WHERE P_4 IS NOT NULL
    GROUP BY customer_ID
),
first_P_4 AS
(
    SELECT
        f.customer_ID, s.P_4 AS P_4_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_P_4 AS
(
    SELECT
        f.customer_ID, s.P_4 AS P_4_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
P_4_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS P_4_span
    FROM
        first_last
),
P_4_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.P_4,
        s.P_4 - LAG(s.P_4, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS P_4_delta
    FROM
        subset s
),
P_4_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.P_4_delta
    FROM
        P_4_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
P_4_delta_per_day AS
(
    SELECT
        customer_ID,
        P_4_delta / date_delta AS P_4_delta_per_day
    FROM
        P_4_delta
),
P_4_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(P_4_delta_per_day) AS P_4_delta_pd
    FROM
        P_4_delta_per_day
    GROUP BY
        customer_ID
),      
P_4_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(P_4_delta) AS P_4_delta_mean,
        MAX(P_4_delta) AS P_4_delta_max,
        MIN(P_4_delta) AS P_4_delta_min
    FROM
        P_4_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(P_4) AS P_4_mean,
        MIN(P_4) AS P_4_min, 
        MAX(P_4) AS P_4_max, 
        SUM(P_4) AS P_4_sum,
        COUNT(P_4) AS P_4_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.P_4_mean,
        a.P_4_min, 
        a.P_4_max, 
        a.P_4_sum,
        a.P_4_max - a.P_4_min AS P_4_range,
        a.P_4_count,
        f.P_4_first,
        l.P_4_last,
        d.P_4_delta_mean,
        d.P_4_delta_max,
        d.P_4_delta_min,
        pd.P_4_delta_pd,
        cs.P_4_span
    FROM
        aggs a
        LEFT JOIN first_P_4 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_P_4 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN P_4_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN P_4_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN P_4_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.P_4_mean, 
    v.P_4_min,
    v.P_4_max, 
    v.P_4_range,
    v.P_4_sum,
    ISNULL(v.P_4_count, 0) AS P_4_count,
    v.P_4_first, 
    v.P_4_last,
    v.P_4_delta_mean,
    v.P_4_delta_max,
    v.P_4_delta_min,
    v.P_4_delta_pd,
    v.P_4_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;