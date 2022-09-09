
CREATE VIEW train_data_D_106_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_106 
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
    WHERE D_106 IS NOT NULL
    GROUP BY customer_ID
),
first_D_106 AS
(
    SELECT
        f.customer_ID, s.D_106 AS D_106_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_106 AS
(
    SELECT
        f.customer_ID, s.D_106 AS D_106_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_106_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_106_span
    FROM
        first_last
),
D_106_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_106,
        s.D_106 - LAG(s.D_106, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_106_delta
    FROM
        subset s
),
D_106_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_106_delta
    FROM
        D_106_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_106_delta_per_day AS
(
    SELECT
        customer_ID,
        D_106_delta / date_delta AS D_106_delta_per_day
    FROM
        D_106_delta
),
D_106_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_106_delta_per_day) AS D_106_delta_pd
    FROM
        D_106_delta_per_day
    GROUP BY
        customer_ID
),      
D_106_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_106_delta) AS D_106_delta_mean,
        MAX(D_106_delta) AS D_106_delta_max,
        MIN(D_106_delta) AS D_106_delta_min
    FROM
        D_106_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_106) AS D_106_mean,
        MIN(D_106) AS D_106_min, 
        MAX(D_106) AS D_106_max, 
        SUM(D_106) AS D_106_sum,
        COUNT(D_106) AS D_106_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_106_mean,
        a.D_106_min, 
        a.D_106_max, 
        a.D_106_sum,
        a.D_106_max - a.D_106_min AS D_106_range,
        a.D_106_count,
        f.D_106_first,
        l.D_106_last,
        d.D_106_delta_mean,
        d.D_106_delta_max,
        d.D_106_delta_min,
        pd.D_106_delta_pd,
        cs.D_106_span
    FROM
        aggs a
        LEFT JOIN first_D_106 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_106 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_106_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_106_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_106_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_106_mean, 
    v.D_106_min,
    v.D_106_max, 
    v.D_106_range,
    v.D_106_sum,
    ISNULL(v.D_106_count, 0) AS D_106_count,
    v.D_106_first, 
    v.D_106_last,
    v.D_106_delta_mean,
    v.D_106_delta_max,
    v.D_106_delta_min,
    v.D_106_delta_pd,
    v.D_106_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;