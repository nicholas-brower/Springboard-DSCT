
CREATE VIEW train_data_D_86_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_86 
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
    WHERE D_86 IS NOT NULL
    GROUP BY customer_ID
),
first_D_86 AS
(
    SELECT
        f.customer_ID, s.D_86 AS D_86_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_86 AS
(
    SELECT
        f.customer_ID, s.D_86 AS D_86_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_86_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_86_span
    FROM
        first_last
),
D_86_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_86,
        s.D_86 - LAG(s.D_86, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_86_delta
    FROM
        subset s
),
D_86_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_86_delta
    FROM
        D_86_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_86_delta_per_day AS
(
    SELECT
        customer_ID,
        D_86_delta / date_delta AS D_86_delta_per_day
    FROM
        D_86_delta
),
D_86_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_86_delta_per_day) AS D_86_delta_pd
    FROM
        D_86_delta_per_day
    GROUP BY
        customer_ID
),      
D_86_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_86_delta) AS D_86_delta_mean,
        MAX(D_86_delta) AS D_86_delta_max,
        MIN(D_86_delta) AS D_86_delta_min
    FROM
        D_86_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_86) AS D_86_mean,
        MIN(D_86) AS D_86_min, 
        MAX(D_86) AS D_86_max, 
        SUM(D_86) AS D_86_sum,
        COUNT(D_86) AS D_86_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_86_mean,
        a.D_86_min, 
        a.D_86_max, 
        a.D_86_sum,
        a.D_86_max - a.D_86_min AS D_86_range,
        a.D_86_count,
        f.D_86_first,
        l.D_86_last,
        d.D_86_delta_mean,
        d.D_86_delta_max,
        d.D_86_delta_min,
        pd.D_86_delta_pd,
        cs.D_86_span
    FROM
        aggs a
        LEFT JOIN first_D_86 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_86 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_86_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_86_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_86_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_86_mean, 
    v.D_86_min,
    v.D_86_max, 
    v.D_86_range,
    v.D_86_sum,
    ISNULL(v.D_86_count, 0) AS D_86_count,
    v.D_86_first, 
    v.D_86_last,
    v.D_86_delta_mean,
    v.D_86_delta_max,
    v.D_86_delta_min,
    v.D_86_delta_pd,
    v.D_86_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;