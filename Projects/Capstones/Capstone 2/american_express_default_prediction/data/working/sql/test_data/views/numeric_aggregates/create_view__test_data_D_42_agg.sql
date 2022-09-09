
CREATE VIEW test_data_D_42_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_42 
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
    WHERE D_42 IS NOT NULL
    GROUP BY customer_ID
),
first_D_42 AS
(
    SELECT
        f.customer_ID, s.D_42 AS D_42_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_42 AS
(
    SELECT
        f.customer_ID, s.D_42 AS D_42_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_42_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_42_span
    FROM
        first_last
),
D_42_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_42,
        s.D_42 - LAG(s.D_42, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_42_delta
    FROM
        subset s
),
D_42_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_42_delta
    FROM
        D_42_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_42_delta_per_day AS
(
    SELECT
        customer_ID,
        D_42_delta / date_delta AS D_42_delta_per_day
    FROM
        D_42_delta
),
D_42_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_42_delta_per_day) AS D_42_delta_pd
    FROM
        D_42_delta_per_day
    GROUP BY
        customer_ID
),      
D_42_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_42_delta) AS D_42_delta_mean,
        MAX(D_42_delta) AS D_42_delta_max,
        MIN(D_42_delta) AS D_42_delta_min
    FROM
        D_42_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_42) AS D_42_mean,
        MIN(D_42) AS D_42_min, 
        MAX(D_42) AS D_42_max, 
        SUM(D_42) AS D_42_sum,
        COUNT(D_42) AS D_42_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_42_mean,
        a.D_42_min, 
        a.D_42_max, 
        a.D_42_sum,
        a.D_42_max - a.D_42_min AS D_42_range,
        a.D_42_count,
        f.D_42_first,
        l.D_42_last,
        d.D_42_delta_mean,
        d.D_42_delta_max,
        d.D_42_delta_min,
        pd.D_42_delta_pd,
        cs.D_42_span
    FROM
        aggs a
        LEFT JOIN first_D_42 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_42 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_42_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_42_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_42_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_42_mean, 
    v.D_42_min,
    v.D_42_max, 
    v.D_42_range,
    v.D_42_sum,
    ISNULL(v.D_42_count, 0) AS D_42_count,
    v.D_42_first, 
    v.D_42_last,
    v.D_42_delta_mean,
    v.D_42_delta_max,
    v.D_42_delta_min,
    v.D_42_delta_pd,
    v.D_42_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;