
CREATE VIEW train_data_D_133_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_133 
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
    WHERE D_133 IS NOT NULL
    GROUP BY customer_ID
),
first_D_133 AS
(
    SELECT
        f.customer_ID, s.D_133 AS D_133_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_133 AS
(
    SELECT
        f.customer_ID, s.D_133 AS D_133_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_133_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_133_span
    FROM
        first_last
),
D_133_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_133,
        s.D_133 - LAG(s.D_133, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_133_delta
    FROM
        subset s
),
D_133_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_133_delta
    FROM
        D_133_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_133_delta_per_day AS
(
    SELECT
        customer_ID,
        D_133_delta / date_delta AS D_133_delta_per_day
    FROM
        D_133_delta
),
D_133_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_133_delta_per_day) AS D_133_delta_pd
    FROM
        D_133_delta_per_day
    GROUP BY
        customer_ID
),      
D_133_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_133_delta) AS D_133_delta_mean,
        MAX(D_133_delta) AS D_133_delta_max,
        MIN(D_133_delta) AS D_133_delta_min
    FROM
        D_133_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_133) AS D_133_mean,
        MIN(D_133) AS D_133_min, 
        MAX(D_133) AS D_133_max, 
        SUM(D_133) AS D_133_sum,
        COUNT(D_133) AS D_133_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_133_mean,
        a.D_133_min, 
        a.D_133_max, 
        a.D_133_sum,
        a.D_133_max - a.D_133_min AS D_133_range,
        a.D_133_count,
        f.D_133_first,
        l.D_133_last,
        d.D_133_delta_mean,
        d.D_133_delta_max,
        d.D_133_delta_min,
        pd.D_133_delta_pd,
        cs.D_133_span
    FROM
        aggs a
        LEFT JOIN first_D_133 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_133 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_133_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_133_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_133_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_133_mean, 
    v.D_133_min,
    v.D_133_max, 
    v.D_133_range,
    v.D_133_sum,
    ISNULL(v.D_133_count, 0) AS D_133_count,
    v.D_133_first, 
    v.D_133_last,
    v.D_133_delta_mean,
    v.D_133_delta_max,
    v.D_133_delta_min,
    v.D_133_delta_pd,
    v.D_133_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;