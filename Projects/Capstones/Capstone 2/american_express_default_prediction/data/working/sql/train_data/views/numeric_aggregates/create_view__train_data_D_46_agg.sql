
CREATE VIEW train_data_D_46_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_46 
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
    WHERE D_46 IS NOT NULL
    GROUP BY customer_ID
),
first_D_46 AS
(
    SELECT
        f.customer_ID, s.D_46 AS D_46_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_46 AS
(
    SELECT
        f.customer_ID, s.D_46 AS D_46_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_46_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_46_span
    FROM
        first_last
),
D_46_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_46,
        s.D_46 - LAG(s.D_46, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_46_delta
    FROM
        subset s
),
D_46_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_46_delta
    FROM
        D_46_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_46_delta_per_day AS
(
    SELECT
        customer_ID,
        D_46_delta / date_delta AS D_46_delta_per_day
    FROM
        D_46_delta
),
D_46_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_46_delta_per_day) AS D_46_delta_pd
    FROM
        D_46_delta_per_day
    GROUP BY
        customer_ID
),      
D_46_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_46_delta) AS D_46_delta_mean,
        MAX(D_46_delta) AS D_46_delta_max,
        MIN(D_46_delta) AS D_46_delta_min
    FROM
        D_46_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_46) AS D_46_mean,
        MIN(D_46) AS D_46_min, 
        MAX(D_46) AS D_46_max, 
        SUM(D_46) AS D_46_sum,
        COUNT(D_46) AS D_46_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_46_mean,
        a.D_46_min, 
        a.D_46_max, 
        a.D_46_sum,
        a.D_46_max - a.D_46_min AS D_46_range,
        a.D_46_count,
        f.D_46_first,
        l.D_46_last,
        d.D_46_delta_mean,
        d.D_46_delta_max,
        d.D_46_delta_min,
        pd.D_46_delta_pd,
        cs.D_46_span
    FROM
        aggs a
        LEFT JOIN first_D_46 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_46 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_46_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_46_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_46_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_46_mean, 
    v.D_46_min,
    v.D_46_max, 
    v.D_46_range,
    v.D_46_sum,
    ISNULL(v.D_46_count, 0) AS D_46_count,
    v.D_46_first, 
    v.D_46_last,
    v.D_46_delta_mean,
    v.D_46_delta_max,
    v.D_46_delta_min,
    v.D_46_delta_pd,
    v.D_46_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;