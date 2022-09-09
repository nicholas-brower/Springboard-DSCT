
CREATE VIEW test_data_D_140_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_140 
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
    WHERE D_140 IS NOT NULL
    GROUP BY customer_ID
),
first_D_140 AS
(
    SELECT
        f.customer_ID, s.D_140 AS D_140_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_140 AS
(
    SELECT
        f.customer_ID, s.D_140 AS D_140_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_140_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_140_span
    FROM
        first_last
),
D_140_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_140,
        s.D_140 - LAG(s.D_140, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_140_delta
    FROM
        subset s
),
D_140_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_140_delta
    FROM
        D_140_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_140_delta_per_day AS
(
    SELECT
        customer_ID,
        D_140_delta / date_delta AS D_140_delta_per_day
    FROM
        D_140_delta
),
D_140_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_140_delta_per_day) AS D_140_delta_pd
    FROM
        D_140_delta_per_day
    GROUP BY
        customer_ID
),      
D_140_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_140_delta) AS D_140_delta_mean,
        MAX(D_140_delta) AS D_140_delta_max,
        MIN(D_140_delta) AS D_140_delta_min
    FROM
        D_140_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_140) AS D_140_mean,
        MIN(D_140) AS D_140_min, 
        MAX(D_140) AS D_140_max, 
        SUM(D_140) AS D_140_sum,
        COUNT(D_140) AS D_140_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_140_mean,
        a.D_140_min, 
        a.D_140_max, 
        a.D_140_sum,
        a.D_140_max - a.D_140_min AS D_140_range,
        a.D_140_count,
        f.D_140_first,
        l.D_140_last,
        d.D_140_delta_mean,
        d.D_140_delta_max,
        d.D_140_delta_min,
        pd.D_140_delta_pd,
        cs.D_140_span
    FROM
        aggs a
        LEFT JOIN first_D_140 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_140 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_140_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_140_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_140_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_140_mean, 
    v.D_140_min,
    v.D_140_max, 
    v.D_140_range,
    v.D_140_sum,
    ISNULL(v.D_140_count, 0) AS D_140_count,
    v.D_140_first, 
    v.D_140_last,
    v.D_140_delta_mean,
    v.D_140_delta_max,
    v.D_140_delta_min,
    v.D_140_delta_pd,
    v.D_140_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;