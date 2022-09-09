
CREATE VIEW test_data_D_109_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_109 
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
    WHERE D_109 IS NOT NULL
    GROUP BY customer_ID
),
first_D_109 AS
(
    SELECT
        f.customer_ID, s.D_109 AS D_109_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_109 AS
(
    SELECT
        f.customer_ID, s.D_109 AS D_109_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_109_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_109_span
    FROM
        first_last
),
D_109_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_109,
        s.D_109 - LAG(s.D_109, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_109_delta
    FROM
        subset s
),
D_109_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_109_delta
    FROM
        D_109_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_109_delta_per_day AS
(
    SELECT
        customer_ID,
        D_109_delta / date_delta AS D_109_delta_per_day
    FROM
        D_109_delta
),
D_109_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_109_delta_per_day) AS D_109_delta_pd
    FROM
        D_109_delta_per_day
    GROUP BY
        customer_ID
),      
D_109_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_109_delta) AS D_109_delta_mean,
        MAX(D_109_delta) AS D_109_delta_max,
        MIN(D_109_delta) AS D_109_delta_min
    FROM
        D_109_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_109) AS D_109_mean,
        MIN(D_109) AS D_109_min, 
        MAX(D_109) AS D_109_max, 
        SUM(D_109) AS D_109_sum,
        COUNT(D_109) AS D_109_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_109_mean,
        a.D_109_min, 
        a.D_109_max, 
        a.D_109_sum,
        a.D_109_max - a.D_109_min AS D_109_range,
        a.D_109_count,
        f.D_109_first,
        l.D_109_last,
        d.D_109_delta_mean,
        d.D_109_delta_max,
        d.D_109_delta_min,
        pd.D_109_delta_pd,
        cs.D_109_span
    FROM
        aggs a
        LEFT JOIN first_D_109 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_109 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_109_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_109_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_109_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_109_mean, 
    v.D_109_min,
    v.D_109_max, 
    v.D_109_range,
    v.D_109_sum,
    ISNULL(v.D_109_count, 0) AS D_109_count,
    v.D_109_first, 
    v.D_109_last,
    v.D_109_delta_mean,
    v.D_109_delta_max,
    v.D_109_delta_min,
    v.D_109_delta_pd,
    v.D_109_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;