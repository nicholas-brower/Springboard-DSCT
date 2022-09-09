
CREATE VIEW test_data_D_105_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_105 
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
    WHERE D_105 IS NOT NULL
    GROUP BY customer_ID
),
first_D_105 AS
(
    SELECT
        f.customer_ID, s.D_105 AS D_105_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_105 AS
(
    SELECT
        f.customer_ID, s.D_105 AS D_105_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_105_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_105_span
    FROM
        first_last
),
D_105_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_105,
        s.D_105 - LAG(s.D_105, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_105_delta
    FROM
        subset s
),
D_105_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_105_delta
    FROM
        D_105_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_105_delta_per_day AS
(
    SELECT
        customer_ID,
        D_105_delta / date_delta AS D_105_delta_per_day
    FROM
        D_105_delta
),
D_105_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_105_delta_per_day) AS D_105_delta_pd
    FROM
        D_105_delta_per_day
    GROUP BY
        customer_ID
),      
D_105_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_105_delta) AS D_105_delta_mean,
        MAX(D_105_delta) AS D_105_delta_max,
        MIN(D_105_delta) AS D_105_delta_min
    FROM
        D_105_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_105) AS D_105_mean,
        MIN(D_105) AS D_105_min, 
        MAX(D_105) AS D_105_max, 
        SUM(D_105) AS D_105_sum,
        COUNT(D_105) AS D_105_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_105_mean,
        a.D_105_min, 
        a.D_105_max, 
        a.D_105_sum,
        a.D_105_max - a.D_105_min AS D_105_range,
        a.D_105_count,
        f.D_105_first,
        l.D_105_last,
        d.D_105_delta_mean,
        d.D_105_delta_max,
        d.D_105_delta_min,
        pd.D_105_delta_pd,
        cs.D_105_span
    FROM
        aggs a
        LEFT JOIN first_D_105 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_105 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_105_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_105_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_105_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_105_mean, 
    v.D_105_min,
    v.D_105_max, 
    v.D_105_range,
    v.D_105_sum,
    ISNULL(v.D_105_count, 0) AS D_105_count,
    v.D_105_first, 
    v.D_105_last,
    v.D_105_delta_mean,
    v.D_105_delta_max,
    v.D_105_delta_min,
    v.D_105_delta_pd,
    v.D_105_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;