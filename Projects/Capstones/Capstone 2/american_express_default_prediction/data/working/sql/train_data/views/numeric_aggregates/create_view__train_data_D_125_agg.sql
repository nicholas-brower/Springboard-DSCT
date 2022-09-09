
CREATE VIEW train_data_D_125_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_125 
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
    WHERE D_125 IS NOT NULL
    GROUP BY customer_ID
),
first_D_125 AS
(
    SELECT
        f.customer_ID, s.D_125 AS D_125_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_125 AS
(
    SELECT
        f.customer_ID, s.D_125 AS D_125_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_125_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_125_span
    FROM
        first_last
),
D_125_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_125,
        s.D_125 - LAG(s.D_125, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_125_delta
    FROM
        subset s
),
D_125_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_125_delta
    FROM
        D_125_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_125_delta_per_day AS
(
    SELECT
        customer_ID,
        D_125_delta / date_delta AS D_125_delta_per_day
    FROM
        D_125_delta
),
D_125_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_125_delta_per_day) AS D_125_delta_pd
    FROM
        D_125_delta_per_day
    GROUP BY
        customer_ID
),      
D_125_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_125_delta) AS D_125_delta_mean,
        MAX(D_125_delta) AS D_125_delta_max,
        MIN(D_125_delta) AS D_125_delta_min
    FROM
        D_125_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_125) AS D_125_mean,
        MIN(D_125) AS D_125_min, 
        MAX(D_125) AS D_125_max, 
        SUM(D_125) AS D_125_sum,
        COUNT(D_125) AS D_125_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_125_mean,
        a.D_125_min, 
        a.D_125_max, 
        a.D_125_sum,
        a.D_125_max - a.D_125_min AS D_125_range,
        a.D_125_count,
        f.D_125_first,
        l.D_125_last,
        d.D_125_delta_mean,
        d.D_125_delta_max,
        d.D_125_delta_min,
        pd.D_125_delta_pd,
        cs.D_125_span
    FROM
        aggs a
        LEFT JOIN first_D_125 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_125 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_125_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_125_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_125_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_125_mean, 
    v.D_125_min,
    v.D_125_max, 
    v.D_125_range,
    v.D_125_sum,
    ISNULL(v.D_125_count, 0) AS D_125_count,
    v.D_125_first, 
    v.D_125_last,
    v.D_125_delta_mean,
    v.D_125_delta_max,
    v.D_125_delta_min,
    v.D_125_delta_pd,
    v.D_125_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;