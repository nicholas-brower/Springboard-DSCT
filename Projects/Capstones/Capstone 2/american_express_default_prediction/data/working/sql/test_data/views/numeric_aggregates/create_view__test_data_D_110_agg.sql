
CREATE VIEW test_data_D_110_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_110 
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
    WHERE D_110 IS NOT NULL
    GROUP BY customer_ID
),
first_D_110 AS
(
    SELECT
        f.customer_ID, s.D_110 AS D_110_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_110 AS
(
    SELECT
        f.customer_ID, s.D_110 AS D_110_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_110_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_110_span
    FROM
        first_last
),
D_110_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_110,
        s.D_110 - LAG(s.D_110, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_110_delta
    FROM
        subset s
),
D_110_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_110_delta
    FROM
        D_110_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_110_delta_per_day AS
(
    SELECT
        customer_ID,
        D_110_delta / date_delta AS D_110_delta_per_day
    FROM
        D_110_delta
),
D_110_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_110_delta_per_day) AS D_110_delta_pd
    FROM
        D_110_delta_per_day
    GROUP BY
        customer_ID
),      
D_110_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_110_delta) AS D_110_delta_mean,
        MAX(D_110_delta) AS D_110_delta_max,
        MIN(D_110_delta) AS D_110_delta_min
    FROM
        D_110_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_110) AS D_110_mean,
        MIN(D_110) AS D_110_min, 
        MAX(D_110) AS D_110_max, 
        SUM(D_110) AS D_110_sum,
        COUNT(D_110) AS D_110_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_110_mean,
        a.D_110_min, 
        a.D_110_max, 
        a.D_110_sum,
        a.D_110_max - a.D_110_min AS D_110_range,
        a.D_110_count,
        f.D_110_first,
        l.D_110_last,
        d.D_110_delta_mean,
        d.D_110_delta_max,
        d.D_110_delta_min,
        pd.D_110_delta_pd,
        cs.D_110_span
    FROM
        aggs a
        LEFT JOIN first_D_110 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_110 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_110_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_110_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_110_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_110_mean, 
    v.D_110_min,
    v.D_110_max, 
    v.D_110_range,
    v.D_110_sum,
    ISNULL(v.D_110_count, 0) AS D_110_count,
    v.D_110_first, 
    v.D_110_last,
    v.D_110_delta_mean,
    v.D_110_delta_max,
    v.D_110_delta_min,
    v.D_110_delta_pd,
    v.D_110_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;