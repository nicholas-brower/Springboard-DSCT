
CREATE VIEW test_data_D_55_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_55 
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
    WHERE D_55 IS NOT NULL
    GROUP BY customer_ID
),
first_D_55 AS
(
    SELECT
        f.customer_ID, s.D_55 AS D_55_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_55 AS
(
    SELECT
        f.customer_ID, s.D_55 AS D_55_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_55_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_55_span
    FROM
        first_last
),
D_55_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_55,
        s.D_55 - LAG(s.D_55, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_55_delta
    FROM
        subset s
),
D_55_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_55_delta
    FROM
        D_55_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_55_delta_per_day AS
(
    SELECT
        customer_ID,
        D_55_delta / date_delta AS D_55_delta_per_day
    FROM
        D_55_delta
),
D_55_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_55_delta_per_day) AS D_55_delta_pd
    FROM
        D_55_delta_per_day
    GROUP BY
        customer_ID
),      
D_55_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_55_delta) AS D_55_delta_mean,
        MAX(D_55_delta) AS D_55_delta_max,
        MIN(D_55_delta) AS D_55_delta_min
    FROM
        D_55_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_55) AS D_55_mean,
        MIN(D_55) AS D_55_min, 
        MAX(D_55) AS D_55_max, 
        SUM(D_55) AS D_55_sum,
        COUNT(D_55) AS D_55_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_55_mean,
        a.D_55_min, 
        a.D_55_max, 
        a.D_55_sum,
        a.D_55_max - a.D_55_min AS D_55_range,
        a.D_55_count,
        f.D_55_first,
        l.D_55_last,
        d.D_55_delta_mean,
        d.D_55_delta_max,
        d.D_55_delta_min,
        pd.D_55_delta_pd,
        cs.D_55_span
    FROM
        aggs a
        LEFT JOIN first_D_55 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_55 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_55_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_55_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_55_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_55_mean, 
    v.D_55_min,
    v.D_55_max, 
    v.D_55_range,
    v.D_55_sum,
    ISNULL(v.D_55_count, 0) AS D_55_count,
    v.D_55_first, 
    v.D_55_last,
    v.D_55_delta_mean,
    v.D_55_delta_max,
    v.D_55_delta_min,
    v.D_55_delta_pd,
    v.D_55_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;