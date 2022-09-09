
CREATE VIEW test_data_D_130_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_130 
    FROM
        test_data td 
        
),
first_last AS
(
    SELECT 
        customer_ID, 
        MIN(S_2) AS first_dt, 
        MAX(S_2) AS last_dt
    FROM subset
    WHERE D_130 IS NOT NULL
    GROUP BY customer_ID
),
first_D_130 AS
(
    SELECT
        f.customer_ID, s.D_130 AS D_130_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_130 AS
(
    SELECT
        f.customer_ID, s.D_130 AS D_130_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_130_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_130_span
    FROM
        first_last
),
D_130_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_130,
        s.D_130 - LAG(s.D_130, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_130_delta
    FROM
        subset s
),
D_130_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_130_delta
    FROM
        D_130_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_130_delta_per_day AS
(
    SELECT
        customer_ID,
        D_130_delta / date_delta AS D_130_delta_per_day
    FROM
        D_130_delta
),
D_130_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_130_delta_per_day) AS D_130_delta_pd
    FROM
        D_130_delta_per_day
    GROUP BY
        customer_ID
),      
D_130_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_130_delta) AS D_130_delta_mean,
        MAX(D_130_delta) AS D_130_delta_max,
        MIN(D_130_delta) AS D_130_delta_min
    FROM
        D_130_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_130) AS D_130_mean,
        MIN(D_130) AS D_130_min, 
        MAX(D_130) AS D_130_max, 
        SUM(D_130) AS D_130_sum,
        COUNT(D_130) AS D_130_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_130_mean,
        a.D_130_min, 
        a.D_130_max, 
        a.D_130_sum,
        a.D_130_max - a.D_130_min AS D_130_range,
        a.D_130_count,
        f.D_130_first,
        l.D_130_last,
        d.D_130_delta_mean,
        d.D_130_delta_max,
        d.D_130_delta_min,
        pd.D_130_delta_pd,
        cs.D_130_span
    FROM
        aggs a
        LEFT JOIN first_D_130 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_130 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_130_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_130_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_130_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_130_mean, 
    v.D_130_min,
    v.D_130_max, 
    v.D_130_range,
    v.D_130_sum,
    ISNULL(v.D_130_count, 0) AS D_130_count,
    v.D_130_first, 
    v.D_130_last,
    v.D_130_delta_mean,
    v.D_130_delta_max,
    v.D_130_delta_min,
    v.D_130_delta_pd,
    v.D_130_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;