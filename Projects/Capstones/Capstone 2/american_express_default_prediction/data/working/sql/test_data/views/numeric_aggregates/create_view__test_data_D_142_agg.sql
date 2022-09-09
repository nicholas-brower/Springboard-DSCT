
CREATE VIEW test_data_D_142_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_142 
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
    WHERE D_142 IS NOT NULL
    GROUP BY customer_ID
),
first_D_142 AS
(
    SELECT
        f.customer_ID, s.D_142 AS D_142_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_142 AS
(
    SELECT
        f.customer_ID, s.D_142 AS D_142_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_142_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_142_span
    FROM
        first_last
),
D_142_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_142,
        s.D_142 - LAG(s.D_142, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_142_delta
    FROM
        subset s
),
D_142_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_142_delta
    FROM
        D_142_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_142_delta_per_day AS
(
    SELECT
        customer_ID,
        D_142_delta / date_delta AS D_142_delta_per_day
    FROM
        D_142_delta
),
D_142_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_142_delta_per_day) AS D_142_delta_pd
    FROM
        D_142_delta_per_day
    GROUP BY
        customer_ID
),      
D_142_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_142_delta) AS D_142_delta_mean,
        MAX(D_142_delta) AS D_142_delta_max,
        MIN(D_142_delta) AS D_142_delta_min
    FROM
        D_142_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_142) AS D_142_mean,
        MIN(D_142) AS D_142_min, 
        MAX(D_142) AS D_142_max, 
        SUM(D_142) AS D_142_sum,
        COUNT(D_142) AS D_142_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_142_mean,
        a.D_142_min, 
        a.D_142_max, 
        a.D_142_sum,
        a.D_142_max - a.D_142_min AS D_142_range,
        a.D_142_count,
        f.D_142_first,
        l.D_142_last,
        d.D_142_delta_mean,
        d.D_142_delta_max,
        d.D_142_delta_min,
        pd.D_142_delta_pd,
        cs.D_142_span
    FROM
        aggs a
        LEFT JOIN first_D_142 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_142 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_142_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_142_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_142_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_142_mean, 
    v.D_142_min,
    v.D_142_max, 
    v.D_142_range,
    v.D_142_sum,
    ISNULL(v.D_142_count, 0) AS D_142_count,
    v.D_142_first, 
    v.D_142_last,
    v.D_142_delta_mean,
    v.D_142_delta_max,
    v.D_142_delta_min,
    v.D_142_delta_pd,
    v.D_142_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;