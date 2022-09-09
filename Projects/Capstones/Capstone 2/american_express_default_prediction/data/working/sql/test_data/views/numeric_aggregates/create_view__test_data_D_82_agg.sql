
CREATE VIEW test_data_D_82_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_82 
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
    WHERE D_82 IS NOT NULL
    GROUP BY customer_ID
),
first_D_82 AS
(
    SELECT
        f.customer_ID, s.D_82 AS D_82_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_82 AS
(
    SELECT
        f.customer_ID, s.D_82 AS D_82_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_82_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_82_span
    FROM
        first_last
),
D_82_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_82,
        s.D_82 - LAG(s.D_82, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_82_delta
    FROM
        subset s
),
D_82_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_82_delta
    FROM
        D_82_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_82_delta_per_day AS
(
    SELECT
        customer_ID,
        D_82_delta / date_delta AS D_82_delta_per_day
    FROM
        D_82_delta
),
D_82_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_82_delta_per_day) AS D_82_delta_pd
    FROM
        D_82_delta_per_day
    GROUP BY
        customer_ID
),      
D_82_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_82_delta) AS D_82_delta_mean,
        MAX(D_82_delta) AS D_82_delta_max,
        MIN(D_82_delta) AS D_82_delta_min
    FROM
        D_82_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_82) AS D_82_mean,
        MIN(D_82) AS D_82_min, 
        MAX(D_82) AS D_82_max, 
        SUM(D_82) AS D_82_sum,
        COUNT(D_82) AS D_82_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_82_mean,
        a.D_82_min, 
        a.D_82_max, 
        a.D_82_sum,
        a.D_82_max - a.D_82_min AS D_82_range,
        a.D_82_count,
        f.D_82_first,
        l.D_82_last,
        d.D_82_delta_mean,
        d.D_82_delta_max,
        d.D_82_delta_min,
        pd.D_82_delta_pd,
        cs.D_82_span
    FROM
        aggs a
        LEFT JOIN first_D_82 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_82 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_82_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_82_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_82_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_82_mean, 
    v.D_82_min,
    v.D_82_max, 
    v.D_82_range,
    v.D_82_sum,
    ISNULL(v.D_82_count, 0) AS D_82_count,
    v.D_82_first, 
    v.D_82_last,
    v.D_82_delta_mean,
    v.D_82_delta_max,
    v.D_82_delta_min,
    v.D_82_delta_pd,
    v.D_82_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;