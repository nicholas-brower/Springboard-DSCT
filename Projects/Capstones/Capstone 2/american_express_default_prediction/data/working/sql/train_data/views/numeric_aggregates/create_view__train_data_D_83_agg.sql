
CREATE VIEW train_data_D_83_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_83 
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
    WHERE D_83 IS NOT NULL
    GROUP BY customer_ID
),
first_D_83 AS
(
    SELECT
        f.customer_ID, s.D_83 AS D_83_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_83 AS
(
    SELECT
        f.customer_ID, s.D_83 AS D_83_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_83_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_83_span
    FROM
        first_last
),
D_83_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_83,
        s.D_83 - LAG(s.D_83, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_83_delta
    FROM
        subset s
),
D_83_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_83_delta
    FROM
        D_83_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_83_delta_per_day AS
(
    SELECT
        customer_ID,
        D_83_delta / date_delta AS D_83_delta_per_day
    FROM
        D_83_delta
),
D_83_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_83_delta_per_day) AS D_83_delta_pd
    FROM
        D_83_delta_per_day
    GROUP BY
        customer_ID
),      
D_83_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_83_delta) AS D_83_delta_mean,
        MAX(D_83_delta) AS D_83_delta_max,
        MIN(D_83_delta) AS D_83_delta_min
    FROM
        D_83_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_83) AS D_83_mean,
        MIN(D_83) AS D_83_min, 
        MAX(D_83) AS D_83_max, 
        SUM(D_83) AS D_83_sum,
        COUNT(D_83) AS D_83_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_83_mean,
        a.D_83_min, 
        a.D_83_max, 
        a.D_83_sum,
        a.D_83_max - a.D_83_min AS D_83_range,
        a.D_83_count,
        f.D_83_first,
        l.D_83_last,
        d.D_83_delta_mean,
        d.D_83_delta_max,
        d.D_83_delta_min,
        pd.D_83_delta_pd,
        cs.D_83_span
    FROM
        aggs a
        LEFT JOIN first_D_83 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_83 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_83_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_83_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_83_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_83_mean, 
    v.D_83_min,
    v.D_83_max, 
    v.D_83_range,
    v.D_83_sum,
    ISNULL(v.D_83_count, 0) AS D_83_count,
    v.D_83_first, 
    v.D_83_last,
    v.D_83_delta_mean,
    v.D_83_delta_max,
    v.D_83_delta_min,
    v.D_83_delta_pd,
    v.D_83_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;