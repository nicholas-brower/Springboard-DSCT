
CREATE VIEW test_data_D_102_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_102 
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
    WHERE D_102 IS NOT NULL
    GROUP BY customer_ID
),
first_D_102 AS
(
    SELECT
        f.customer_ID, s.D_102 AS D_102_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_102 AS
(
    SELECT
        f.customer_ID, s.D_102 AS D_102_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_102_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_102_span
    FROM
        first_last
),
D_102_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_102,
        s.D_102 - LAG(s.D_102, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_102_delta
    FROM
        subset s
),
D_102_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_102_delta
    FROM
        D_102_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_102_delta_per_day AS
(
    SELECT
        customer_ID,
        D_102_delta / date_delta AS D_102_delta_per_day
    FROM
        D_102_delta
),
D_102_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_102_delta_per_day) AS D_102_delta_pd
    FROM
        D_102_delta_per_day
    GROUP BY
        customer_ID
),      
D_102_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_102_delta) AS D_102_delta_mean,
        MAX(D_102_delta) AS D_102_delta_max,
        MIN(D_102_delta) AS D_102_delta_min
    FROM
        D_102_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_102) AS D_102_mean,
        MIN(D_102) AS D_102_min, 
        MAX(D_102) AS D_102_max, 
        SUM(D_102) AS D_102_sum,
        COUNT(D_102) AS D_102_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_102_mean,
        a.D_102_min, 
        a.D_102_max, 
        a.D_102_sum,
        a.D_102_max - a.D_102_min AS D_102_range,
        a.D_102_count,
        f.D_102_first,
        l.D_102_last,
        d.D_102_delta_mean,
        d.D_102_delta_max,
        d.D_102_delta_min,
        pd.D_102_delta_pd,
        cs.D_102_span
    FROM
        aggs a
        LEFT JOIN first_D_102 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_102 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_102_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_102_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_102_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_102_mean, 
    v.D_102_min,
    v.D_102_max, 
    v.D_102_range,
    v.D_102_sum,
    ISNULL(v.D_102_count, 0) AS D_102_count,
    v.D_102_first, 
    v.D_102_last,
    v.D_102_delta_mean,
    v.D_102_delta_max,
    v.D_102_delta_min,
    v.D_102_delta_pd,
    v.D_102_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;