
CREATE VIEW test_data_D_50_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_50 
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
    WHERE D_50 IS NOT NULL
    GROUP BY customer_ID
),
first_D_50 AS
(
    SELECT
        f.customer_ID, s.D_50 AS D_50_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_50 AS
(
    SELECT
        f.customer_ID, s.D_50 AS D_50_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_50_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_50_span
    FROM
        first_last
),
D_50_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_50,
        s.D_50 - LAG(s.D_50, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_50_delta
    FROM
        subset s
),
D_50_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_50_delta
    FROM
        D_50_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_50_delta_per_day AS
(
    SELECT
        customer_ID,
        D_50_delta / date_delta AS D_50_delta_per_day
    FROM
        D_50_delta
),
D_50_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_50_delta_per_day) AS D_50_delta_pd
    FROM
        D_50_delta_per_day
    GROUP BY
        customer_ID
),      
D_50_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_50_delta) AS D_50_delta_mean,
        MAX(D_50_delta) AS D_50_delta_max,
        MIN(D_50_delta) AS D_50_delta_min
    FROM
        D_50_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_50) AS D_50_mean,
        MIN(D_50) AS D_50_min, 
        MAX(D_50) AS D_50_max, 
        SUM(D_50) AS D_50_sum,
        COUNT(D_50) AS D_50_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_50_mean,
        a.D_50_min, 
        a.D_50_max, 
        a.D_50_sum,
        a.D_50_max - a.D_50_min AS D_50_range,
        a.D_50_count,
        f.D_50_first,
        l.D_50_last,
        d.D_50_delta_mean,
        d.D_50_delta_max,
        d.D_50_delta_min,
        pd.D_50_delta_pd,
        cs.D_50_span
    FROM
        aggs a
        LEFT JOIN first_D_50 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_50 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_50_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_50_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_50_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_50_mean, 
    v.D_50_min,
    v.D_50_max, 
    v.D_50_range,
    v.D_50_sum,
    ISNULL(v.D_50_count, 0) AS D_50_count,
    v.D_50_first, 
    v.D_50_last,
    v.D_50_delta_mean,
    v.D_50_delta_max,
    v.D_50_delta_min,
    v.D_50_delta_pd,
    v.D_50_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;