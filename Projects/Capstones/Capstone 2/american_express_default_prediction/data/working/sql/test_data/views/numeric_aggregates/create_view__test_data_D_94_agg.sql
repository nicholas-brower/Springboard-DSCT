
CREATE VIEW test_data_D_94_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_94 
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
    WHERE D_94 IS NOT NULL
    GROUP BY customer_ID
),
first_D_94 AS
(
    SELECT
        f.customer_ID, s.D_94 AS D_94_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_94 AS
(
    SELECT
        f.customer_ID, s.D_94 AS D_94_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_94_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_94_span
    FROM
        first_last
),
D_94_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_94,
        s.D_94 - LAG(s.D_94, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_94_delta
    FROM
        subset s
),
D_94_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_94_delta
    FROM
        D_94_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_94_delta_per_day AS
(
    SELECT
        customer_ID,
        D_94_delta / date_delta AS D_94_delta_per_day
    FROM
        D_94_delta
),
D_94_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_94_delta_per_day) AS D_94_delta_pd
    FROM
        D_94_delta_per_day
    GROUP BY
        customer_ID
),      
D_94_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_94_delta) AS D_94_delta_mean,
        MAX(D_94_delta) AS D_94_delta_max,
        MIN(D_94_delta) AS D_94_delta_min
    FROM
        D_94_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_94) AS D_94_mean,
        MIN(D_94) AS D_94_min, 
        MAX(D_94) AS D_94_max, 
        SUM(D_94) AS D_94_sum,
        COUNT(D_94) AS D_94_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_94_mean,
        a.D_94_min, 
        a.D_94_max, 
        a.D_94_sum,
        a.D_94_max - a.D_94_min AS D_94_range,
        a.D_94_count,
        f.D_94_first,
        l.D_94_last,
        d.D_94_delta_mean,
        d.D_94_delta_max,
        d.D_94_delta_min,
        pd.D_94_delta_pd,
        cs.D_94_span
    FROM
        aggs a
        LEFT JOIN first_D_94 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_94 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_94_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_94_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_94_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_94_mean, 
    v.D_94_min,
    v.D_94_max, 
    v.D_94_range,
    v.D_94_sum,
    ISNULL(v.D_94_count, 0) AS D_94_count,
    v.D_94_first, 
    v.D_94_last,
    v.D_94_delta_mean,
    v.D_94_delta_max,
    v.D_94_delta_min,
    v.D_94_delta_pd,
    v.D_94_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;