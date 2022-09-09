
CREATE VIEW train_data_D_128_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_128 
    FROM
        train_data td 
        
),
first_last AS
(
    SELECT 
        customer_ID, 
        MIN(S_2) AS first_dt, 
        MAX(S_2) AS last_dt
    FROM subset
    WHERE D_128 IS NOT NULL
    GROUP BY customer_ID
),
first_D_128 AS
(
    SELECT
        f.customer_ID, s.D_128 AS D_128_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_128 AS
(
    SELECT
        f.customer_ID, s.D_128 AS D_128_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_128_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_128_span
    FROM
        first_last
),
D_128_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_128,
        s.D_128 - LAG(s.D_128, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_128_delta
    FROM
        subset s
),
D_128_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_128_delta
    FROM
        D_128_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_128_delta_per_day AS
(
    SELECT
        customer_ID,
        D_128_delta / date_delta AS D_128_delta_per_day
    FROM
        D_128_delta
),
D_128_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_128_delta_per_day) AS D_128_delta_pd
    FROM
        D_128_delta_per_day
    GROUP BY
        customer_ID
),      
D_128_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_128_delta) AS D_128_delta_mean,
        MAX(D_128_delta) AS D_128_delta_max,
        MIN(D_128_delta) AS D_128_delta_min
    FROM
        D_128_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_128) AS D_128_mean,
        MIN(D_128) AS D_128_min, 
        MAX(D_128) AS D_128_max, 
        SUM(D_128) AS D_128_sum,
        COUNT(D_128) AS D_128_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_128_mean,
        a.D_128_min, 
        a.D_128_max, 
        a.D_128_sum,
        a.D_128_max - a.D_128_min AS D_128_range,
        a.D_128_count,
        f.D_128_first,
        l.D_128_last,
        d.D_128_delta_mean,
        d.D_128_delta_max,
        d.D_128_delta_min,
        pd.D_128_delta_pd,
        cs.D_128_span
    FROM
        aggs a
        LEFT JOIN first_D_128 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_128 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_128_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_128_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_128_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_128_mean, 
    v.D_128_min,
    v.D_128_max, 
    v.D_128_range,
    v.D_128_sum,
    ISNULL(v.D_128_count, 0) AS D_128_count,
    v.D_128_first, 
    v.D_128_last,
    v.D_128_delta_mean,
    v.D_128_delta_max,
    v.D_128_delta_min,
    v.D_128_delta_pd,
    v.D_128_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;