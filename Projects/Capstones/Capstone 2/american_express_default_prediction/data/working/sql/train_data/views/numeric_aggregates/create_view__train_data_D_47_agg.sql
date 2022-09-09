
CREATE VIEW train_data_D_47_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_47 
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
    WHERE D_47 IS NOT NULL
    GROUP BY customer_ID
),
first_D_47 AS
(
    SELECT
        f.customer_ID, s.D_47 AS D_47_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_47 AS
(
    SELECT
        f.customer_ID, s.D_47 AS D_47_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_47_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_47_span
    FROM
        first_last
),
D_47_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_47,
        s.D_47 - LAG(s.D_47, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_47_delta
    FROM
        subset s
),
D_47_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_47_delta
    FROM
        D_47_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_47_delta_per_day AS
(
    SELECT
        customer_ID,
        D_47_delta / date_delta AS D_47_delta_per_day
    FROM
        D_47_delta
),
D_47_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_47_delta_per_day) AS D_47_delta_pd
    FROM
        D_47_delta_per_day
    GROUP BY
        customer_ID
),      
D_47_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_47_delta) AS D_47_delta_mean,
        MAX(D_47_delta) AS D_47_delta_max,
        MIN(D_47_delta) AS D_47_delta_min
    FROM
        D_47_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_47) AS D_47_mean,
        MIN(D_47) AS D_47_min, 
        MAX(D_47) AS D_47_max, 
        SUM(D_47) AS D_47_sum,
        COUNT(D_47) AS D_47_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_47_mean,
        a.D_47_min, 
        a.D_47_max, 
        a.D_47_sum,
        a.D_47_max - a.D_47_min AS D_47_range,
        a.D_47_count,
        f.D_47_first,
        l.D_47_last,
        d.D_47_delta_mean,
        d.D_47_delta_max,
        d.D_47_delta_min,
        pd.D_47_delta_pd,
        cs.D_47_span
    FROM
        aggs a
        LEFT JOIN first_D_47 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_47 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_47_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_47_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_47_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_47_mean, 
    v.D_47_min,
    v.D_47_max, 
    v.D_47_range,
    v.D_47_sum,
    ISNULL(v.D_47_count, 0) AS D_47_count,
    v.D_47_first, 
    v.D_47_last,
    v.D_47_delta_mean,
    v.D_47_delta_max,
    v.D_47_delta_min,
    v.D_47_delta_pd,
    v.D_47_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;