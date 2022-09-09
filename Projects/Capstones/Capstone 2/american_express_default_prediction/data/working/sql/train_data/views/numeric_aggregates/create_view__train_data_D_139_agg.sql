
CREATE VIEW train_data_D_139_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_139 
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
    WHERE D_139 IS NOT NULL
    GROUP BY customer_ID
),
first_D_139 AS
(
    SELECT
        f.customer_ID, s.D_139 AS D_139_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_139 AS
(
    SELECT
        f.customer_ID, s.D_139 AS D_139_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_139_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_139_span
    FROM
        first_last
),
D_139_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_139,
        s.D_139 - LAG(s.D_139, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_139_delta
    FROM
        subset s
),
D_139_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_139_delta
    FROM
        D_139_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_139_delta_per_day AS
(
    SELECT
        customer_ID,
        D_139_delta / date_delta AS D_139_delta_per_day
    FROM
        D_139_delta
),
D_139_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_139_delta_per_day) AS D_139_delta_pd
    FROM
        D_139_delta_per_day
    GROUP BY
        customer_ID
),      
D_139_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_139_delta) AS D_139_delta_mean,
        MAX(D_139_delta) AS D_139_delta_max,
        MIN(D_139_delta) AS D_139_delta_min
    FROM
        D_139_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_139) AS D_139_mean,
        MIN(D_139) AS D_139_min, 
        MAX(D_139) AS D_139_max, 
        SUM(D_139) AS D_139_sum,
        COUNT(D_139) AS D_139_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_139_mean,
        a.D_139_min, 
        a.D_139_max, 
        a.D_139_sum,
        a.D_139_max - a.D_139_min AS D_139_range,
        a.D_139_count,
        f.D_139_first,
        l.D_139_last,
        d.D_139_delta_mean,
        d.D_139_delta_max,
        d.D_139_delta_min,
        pd.D_139_delta_pd,
        cs.D_139_span
    FROM
        aggs a
        LEFT JOIN first_D_139 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_139 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_139_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_139_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_139_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_139_mean, 
    v.D_139_min,
    v.D_139_max, 
    v.D_139_range,
    v.D_139_sum,
    ISNULL(v.D_139_count, 0) AS D_139_count,
    v.D_139_first, 
    v.D_139_last,
    v.D_139_delta_mean,
    v.D_139_delta_max,
    v.D_139_delta_min,
    v.D_139_delta_pd,
    v.D_139_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;