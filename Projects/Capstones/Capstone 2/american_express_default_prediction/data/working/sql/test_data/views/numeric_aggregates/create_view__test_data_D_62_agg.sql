
CREATE VIEW test_data_D_62_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_62 
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
    WHERE D_62 IS NOT NULL
    GROUP BY customer_ID
),
first_D_62 AS
(
    SELECT
        f.customer_ID, s.D_62 AS D_62_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_62 AS
(
    SELECT
        f.customer_ID, s.D_62 AS D_62_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_62_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_62_span
    FROM
        first_last
),
D_62_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_62,
        s.D_62 - LAG(s.D_62, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_62_delta
    FROM
        subset s
),
D_62_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_62_delta
    FROM
        D_62_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_62_delta_per_day AS
(
    SELECT
        customer_ID,
        D_62_delta / date_delta AS D_62_delta_per_day
    FROM
        D_62_delta
),
D_62_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_62_delta_per_day) AS D_62_delta_pd
    FROM
        D_62_delta_per_day
    GROUP BY
        customer_ID
),      
D_62_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_62_delta) AS D_62_delta_mean,
        MAX(D_62_delta) AS D_62_delta_max,
        MIN(D_62_delta) AS D_62_delta_min
    FROM
        D_62_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_62) AS D_62_mean,
        MIN(D_62) AS D_62_min, 
        MAX(D_62) AS D_62_max, 
        SUM(D_62) AS D_62_sum,
        COUNT(D_62) AS D_62_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_62_mean,
        a.D_62_min, 
        a.D_62_max, 
        a.D_62_sum,
        a.D_62_max - a.D_62_min AS D_62_range,
        a.D_62_count,
        f.D_62_first,
        l.D_62_last,
        d.D_62_delta_mean,
        d.D_62_delta_max,
        d.D_62_delta_min,
        pd.D_62_delta_pd,
        cs.D_62_span
    FROM
        aggs a
        LEFT JOIN first_D_62 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_62 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_62_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_62_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_62_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_62_mean, 
    v.D_62_min,
    v.D_62_max, 
    v.D_62_range,
    v.D_62_sum,
    ISNULL(v.D_62_count, 0) AS D_62_count,
    v.D_62_first, 
    v.D_62_last,
    v.D_62_delta_mean,
    v.D_62_delta_max,
    v.D_62_delta_min,
    v.D_62_delta_pd,
    v.D_62_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;