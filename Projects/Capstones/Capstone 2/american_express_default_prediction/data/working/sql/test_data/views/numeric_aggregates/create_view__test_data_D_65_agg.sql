
CREATE VIEW test_data_D_65_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_65 
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
    WHERE D_65 IS NOT NULL
    GROUP BY customer_ID
),
first_D_65 AS
(
    SELECT
        f.customer_ID, s.D_65 AS D_65_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_65 AS
(
    SELECT
        f.customer_ID, s.D_65 AS D_65_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_65_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_65_span
    FROM
        first_last
),
D_65_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_65,
        s.D_65 - LAG(s.D_65, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_65_delta
    FROM
        subset s
),
D_65_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_65_delta
    FROM
        D_65_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_65_delta_per_day AS
(
    SELECT
        customer_ID,
        D_65_delta / date_delta AS D_65_delta_per_day
    FROM
        D_65_delta
),
D_65_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_65_delta_per_day) AS D_65_delta_pd
    FROM
        D_65_delta_per_day
    GROUP BY
        customer_ID
),      
D_65_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_65_delta) AS D_65_delta_mean,
        MAX(D_65_delta) AS D_65_delta_max,
        MIN(D_65_delta) AS D_65_delta_min
    FROM
        D_65_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_65) AS D_65_mean,
        MIN(D_65) AS D_65_min, 
        MAX(D_65) AS D_65_max, 
        SUM(D_65) AS D_65_sum,
        COUNT(D_65) AS D_65_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_65_mean,
        a.D_65_min, 
        a.D_65_max, 
        a.D_65_sum,
        a.D_65_max - a.D_65_min AS D_65_range,
        a.D_65_count,
        f.D_65_first,
        l.D_65_last,
        d.D_65_delta_mean,
        d.D_65_delta_max,
        d.D_65_delta_min,
        pd.D_65_delta_pd,
        cs.D_65_span
    FROM
        aggs a
        LEFT JOIN first_D_65 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_65 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_65_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_65_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_65_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_65_mean, 
    v.D_65_min,
    v.D_65_max, 
    v.D_65_range,
    v.D_65_sum,
    ISNULL(v.D_65_count, 0) AS D_65_count,
    v.D_65_first, 
    v.D_65_last,
    v.D_65_delta_mean,
    v.D_65_delta_max,
    v.D_65_delta_min,
    v.D_65_delta_pd,
    v.D_65_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;