
CREATE VIEW train_data_D_112_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_112 
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
    WHERE D_112 IS NOT NULL
    GROUP BY customer_ID
),
first_D_112 AS
(
    SELECT
        f.customer_ID, s.D_112 AS D_112_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_112 AS
(
    SELECT
        f.customer_ID, s.D_112 AS D_112_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_112_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_112_span
    FROM
        first_last
),
D_112_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_112,
        s.D_112 - LAG(s.D_112, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_112_delta
    FROM
        subset s
),
D_112_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_112_delta
    FROM
        D_112_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_112_delta_per_day AS
(
    SELECT
        customer_ID,
        D_112_delta / date_delta AS D_112_delta_per_day
    FROM
        D_112_delta
),
D_112_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_112_delta_per_day) AS D_112_delta_pd
    FROM
        D_112_delta_per_day
    GROUP BY
        customer_ID
),      
D_112_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_112_delta) AS D_112_delta_mean,
        MAX(D_112_delta) AS D_112_delta_max,
        MIN(D_112_delta) AS D_112_delta_min
    FROM
        D_112_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_112) AS D_112_mean,
        MIN(D_112) AS D_112_min, 
        MAX(D_112) AS D_112_max, 
        SUM(D_112) AS D_112_sum,
        COUNT(D_112) AS D_112_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_112_mean,
        a.D_112_min, 
        a.D_112_max, 
        a.D_112_sum,
        a.D_112_max - a.D_112_min AS D_112_range,
        a.D_112_count,
        f.D_112_first,
        l.D_112_last,
        d.D_112_delta_mean,
        d.D_112_delta_max,
        d.D_112_delta_min,
        pd.D_112_delta_pd,
        cs.D_112_span
    FROM
        aggs a
        LEFT JOIN first_D_112 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_112 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_112_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_112_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_112_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_112_mean, 
    v.D_112_min,
    v.D_112_max, 
    v.D_112_range,
    v.D_112_sum,
    ISNULL(v.D_112_count, 0) AS D_112_count,
    v.D_112_first, 
    v.D_112_last,
    v.D_112_delta_mean,
    v.D_112_delta_max,
    v.D_112_delta_min,
    v.D_112_delta_pd,
    v.D_112_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;