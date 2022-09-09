
CREATE VIEW test_data_D_70_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_70 
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
    WHERE D_70 IS NOT NULL
    GROUP BY customer_ID
),
first_D_70 AS
(
    SELECT
        f.customer_ID, s.D_70 AS D_70_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_70 AS
(
    SELECT
        f.customer_ID, s.D_70 AS D_70_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_70_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_70_span
    FROM
        first_last
),
D_70_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_70,
        s.D_70 - LAG(s.D_70, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_70_delta
    FROM
        subset s
),
D_70_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_70_delta
    FROM
        D_70_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_70_delta_per_day AS
(
    SELECT
        customer_ID,
        D_70_delta / date_delta AS D_70_delta_per_day
    FROM
        D_70_delta
),
D_70_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_70_delta_per_day) AS D_70_delta_pd
    FROM
        D_70_delta_per_day
    GROUP BY
        customer_ID
),      
D_70_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_70_delta) AS D_70_delta_mean,
        MAX(D_70_delta) AS D_70_delta_max,
        MIN(D_70_delta) AS D_70_delta_min
    FROM
        D_70_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_70) AS D_70_mean,
        MIN(D_70) AS D_70_min, 
        MAX(D_70) AS D_70_max, 
        SUM(D_70) AS D_70_sum,
        COUNT(D_70) AS D_70_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_70_mean,
        a.D_70_min, 
        a.D_70_max, 
        a.D_70_sum,
        a.D_70_max - a.D_70_min AS D_70_range,
        a.D_70_count,
        f.D_70_first,
        l.D_70_last,
        d.D_70_delta_mean,
        d.D_70_delta_max,
        d.D_70_delta_min,
        pd.D_70_delta_pd,
        cs.D_70_span
    FROM
        aggs a
        LEFT JOIN first_D_70 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_70 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_70_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_70_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_70_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_70_mean, 
    v.D_70_min,
    v.D_70_max, 
    v.D_70_range,
    v.D_70_sum,
    ISNULL(v.D_70_count, 0) AS D_70_count,
    v.D_70_first, 
    v.D_70_last,
    v.D_70_delta_mean,
    v.D_70_delta_max,
    v.D_70_delta_min,
    v.D_70_delta_pd,
    v.D_70_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;