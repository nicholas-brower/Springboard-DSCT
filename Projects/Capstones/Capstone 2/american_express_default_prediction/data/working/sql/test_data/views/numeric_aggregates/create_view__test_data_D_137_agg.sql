
CREATE VIEW test_data_D_137_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_137 
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
    WHERE D_137 IS NOT NULL
    GROUP BY customer_ID
),
first_D_137 AS
(
    SELECT
        f.customer_ID, s.D_137 AS D_137_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_137 AS
(
    SELECT
        f.customer_ID, s.D_137 AS D_137_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_137_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_137_span
    FROM
        first_last
),
D_137_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_137,
        s.D_137 - LAG(s.D_137, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_137_delta
    FROM
        subset s
),
D_137_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_137_delta
    FROM
        D_137_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_137_delta_per_day AS
(
    SELECT
        customer_ID,
        D_137_delta / date_delta AS D_137_delta_per_day
    FROM
        D_137_delta
),
D_137_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_137_delta_per_day) AS D_137_delta_pd
    FROM
        D_137_delta_per_day
    GROUP BY
        customer_ID
),      
D_137_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_137_delta) AS D_137_delta_mean,
        MAX(D_137_delta) AS D_137_delta_max,
        MIN(D_137_delta) AS D_137_delta_min
    FROM
        D_137_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_137) AS D_137_mean,
        MIN(D_137) AS D_137_min, 
        MAX(D_137) AS D_137_max, 
        SUM(D_137) AS D_137_sum,
        COUNT(D_137) AS D_137_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_137_mean,
        a.D_137_min, 
        a.D_137_max, 
        a.D_137_sum,
        a.D_137_max - a.D_137_min AS D_137_range,
        a.D_137_count,
        f.D_137_first,
        l.D_137_last,
        d.D_137_delta_mean,
        d.D_137_delta_max,
        d.D_137_delta_min,
        pd.D_137_delta_pd,
        cs.D_137_span
    FROM
        aggs a
        LEFT JOIN first_D_137 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_137 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_137_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_137_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_137_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_137_mean, 
    v.D_137_min,
    v.D_137_max, 
    v.D_137_range,
    v.D_137_sum,
    ISNULL(v.D_137_count, 0) AS D_137_count,
    v.D_137_first, 
    v.D_137_last,
    v.D_137_delta_mean,
    v.D_137_delta_max,
    v.D_137_delta_min,
    v.D_137_delta_pd,
    v.D_137_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;