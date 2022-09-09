
CREATE VIEW train_data_D_103_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_103 
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
    WHERE D_103 IS NOT NULL
    GROUP BY customer_ID
),
first_D_103 AS
(
    SELECT
        f.customer_ID, s.D_103 AS D_103_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_103 AS
(
    SELECT
        f.customer_ID, s.D_103 AS D_103_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_103_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_103_span
    FROM
        first_last
),
D_103_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_103,
        s.D_103 - LAG(s.D_103, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_103_delta
    FROM
        subset s
),
D_103_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_103_delta
    FROM
        D_103_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_103_delta_per_day AS
(
    SELECT
        customer_ID,
        D_103_delta / date_delta AS D_103_delta_per_day
    FROM
        D_103_delta
),
D_103_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_103_delta_per_day) AS D_103_delta_pd
    FROM
        D_103_delta_per_day
    GROUP BY
        customer_ID
),      
D_103_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_103_delta) AS D_103_delta_mean,
        MAX(D_103_delta) AS D_103_delta_max,
        MIN(D_103_delta) AS D_103_delta_min
    FROM
        D_103_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_103) AS D_103_mean,
        MIN(D_103) AS D_103_min, 
        MAX(D_103) AS D_103_max, 
        SUM(D_103) AS D_103_sum,
        COUNT(D_103) AS D_103_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_103_mean,
        a.D_103_min, 
        a.D_103_max, 
        a.D_103_sum,
        a.D_103_max - a.D_103_min AS D_103_range,
        a.D_103_count,
        f.D_103_first,
        l.D_103_last,
        d.D_103_delta_mean,
        d.D_103_delta_max,
        d.D_103_delta_min,
        pd.D_103_delta_pd,
        cs.D_103_span
    FROM
        aggs a
        LEFT JOIN first_D_103 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_103 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_103_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_103_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_103_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_103_mean, 
    v.D_103_min,
    v.D_103_max, 
    v.D_103_range,
    v.D_103_sum,
    ISNULL(v.D_103_count, 0) AS D_103_count,
    v.D_103_first, 
    v.D_103_last,
    v.D_103_delta_mean,
    v.D_103_delta_max,
    v.D_103_delta_min,
    v.D_103_delta_pd,
    v.D_103_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;