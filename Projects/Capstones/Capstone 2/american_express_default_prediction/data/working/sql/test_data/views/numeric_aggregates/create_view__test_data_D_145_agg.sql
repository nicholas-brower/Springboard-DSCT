
CREATE VIEW test_data_D_145_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_145 
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
    WHERE D_145 IS NOT NULL
    GROUP BY customer_ID
),
first_D_145 AS
(
    SELECT
        f.customer_ID, s.D_145 AS D_145_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_145 AS
(
    SELECT
        f.customer_ID, s.D_145 AS D_145_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_145_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_145_span
    FROM
        first_last
),
D_145_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_145,
        s.D_145 - LAG(s.D_145, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_145_delta
    FROM
        subset s
),
D_145_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_145_delta
    FROM
        D_145_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_145_delta_per_day AS
(
    SELECT
        customer_ID,
        D_145_delta / date_delta AS D_145_delta_per_day
    FROM
        D_145_delta
),
D_145_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_145_delta_per_day) AS D_145_delta_pd
    FROM
        D_145_delta_per_day
    GROUP BY
        customer_ID
),      
D_145_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_145_delta) AS D_145_delta_mean,
        MAX(D_145_delta) AS D_145_delta_max,
        MIN(D_145_delta) AS D_145_delta_min
    FROM
        D_145_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_145) AS D_145_mean,
        MIN(D_145) AS D_145_min, 
        MAX(D_145) AS D_145_max, 
        SUM(D_145) AS D_145_sum,
        COUNT(D_145) AS D_145_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_145_mean,
        a.D_145_min, 
        a.D_145_max, 
        a.D_145_sum,
        a.D_145_max - a.D_145_min AS D_145_range,
        a.D_145_count,
        f.D_145_first,
        l.D_145_last,
        d.D_145_delta_mean,
        d.D_145_delta_max,
        d.D_145_delta_min,
        pd.D_145_delta_pd,
        cs.D_145_span
    FROM
        aggs a
        LEFT JOIN first_D_145 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_145 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_145_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_145_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_145_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_145_mean, 
    v.D_145_min,
    v.D_145_max, 
    v.D_145_range,
    v.D_145_sum,
    ISNULL(v.D_145_count, 0) AS D_145_count,
    v.D_145_first, 
    v.D_145_last,
    v.D_145_delta_mean,
    v.D_145_delta_max,
    v.D_145_delta_min,
    v.D_145_delta_pd,
    v.D_145_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;