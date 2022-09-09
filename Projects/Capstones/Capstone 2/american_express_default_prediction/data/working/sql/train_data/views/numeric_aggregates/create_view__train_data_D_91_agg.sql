
CREATE VIEW train_data_D_91_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_91 
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
    WHERE D_91 IS NOT NULL
    GROUP BY customer_ID
),
first_D_91 AS
(
    SELECT
        f.customer_ID, s.D_91 AS D_91_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_91 AS
(
    SELECT
        f.customer_ID, s.D_91 AS D_91_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_91_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_91_span
    FROM
        first_last
),
D_91_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_91,
        s.D_91 - LAG(s.D_91, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_91_delta
    FROM
        subset s
),
D_91_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_91_delta
    FROM
        D_91_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_91_delta_per_day AS
(
    SELECT
        customer_ID,
        D_91_delta / date_delta AS D_91_delta_per_day
    FROM
        D_91_delta
),
D_91_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_91_delta_per_day) AS D_91_delta_pd
    FROM
        D_91_delta_per_day
    GROUP BY
        customer_ID
),      
D_91_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_91_delta) AS D_91_delta_mean,
        MAX(D_91_delta) AS D_91_delta_max,
        MIN(D_91_delta) AS D_91_delta_min
    FROM
        D_91_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_91) AS D_91_mean,
        MIN(D_91) AS D_91_min, 
        MAX(D_91) AS D_91_max, 
        SUM(D_91) AS D_91_sum,
        COUNT(D_91) AS D_91_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_91_mean,
        a.D_91_min, 
        a.D_91_max, 
        a.D_91_sum,
        a.D_91_max - a.D_91_min AS D_91_range,
        a.D_91_count,
        f.D_91_first,
        l.D_91_last,
        d.D_91_delta_mean,
        d.D_91_delta_max,
        d.D_91_delta_min,
        pd.D_91_delta_pd,
        cs.D_91_span
    FROM
        aggs a
        LEFT JOIN first_D_91 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_91 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_91_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_91_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_91_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_91_mean, 
    v.D_91_min,
    v.D_91_max, 
    v.D_91_range,
    v.D_91_sum,
    ISNULL(v.D_91_count, 0) AS D_91_count,
    v.D_91_first, 
    v.D_91_last,
    v.D_91_delta_mean,
    v.D_91_delta_max,
    v.D_91_delta_min,
    v.D_91_delta_pd,
    v.D_91_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;