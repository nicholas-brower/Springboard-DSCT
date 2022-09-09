
CREATE VIEW train_data_D_39_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_39 
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
    WHERE D_39 IS NOT NULL
    GROUP BY customer_ID
),
first_D_39 AS
(
    SELECT
        f.customer_ID, s.D_39 AS D_39_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_39 AS
(
    SELECT
        f.customer_ID, s.D_39 AS D_39_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_39_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_39_span
    FROM
        first_last
),
D_39_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_39,
        s.D_39 - LAG(s.D_39, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_39_delta
    FROM
        subset s
),
D_39_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_39_delta
    FROM
        D_39_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_39_delta_per_day AS
(
    SELECT
        customer_ID,
        D_39_delta / date_delta AS D_39_delta_per_day
    FROM
        D_39_delta
),
D_39_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_39_delta_per_day) AS D_39_delta_pd
    FROM
        D_39_delta_per_day
    GROUP BY
        customer_ID
),      
D_39_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_39_delta) AS D_39_delta_mean,
        MAX(D_39_delta) AS D_39_delta_max,
        MIN(D_39_delta) AS D_39_delta_min
    FROM
        D_39_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_39) AS D_39_mean,
        MIN(D_39) AS D_39_min, 
        MAX(D_39) AS D_39_max, 
        SUM(D_39) AS D_39_sum,
        COUNT(D_39) AS D_39_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_39_mean,
        a.D_39_min, 
        a.D_39_max, 
        a.D_39_sum,
        a.D_39_max - a.D_39_min AS D_39_range,
        a.D_39_count,
        f.D_39_first,
        l.D_39_last,
        d.D_39_delta_mean,
        d.D_39_delta_max,
        d.D_39_delta_min,
        pd.D_39_delta_pd,
        cs.D_39_span
    FROM
        aggs a
        LEFT JOIN first_D_39 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_39 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_39_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_39_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_39_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_39_mean, 
    v.D_39_min,
    v.D_39_max, 
    v.D_39_range,
    v.D_39_sum,
    ISNULL(v.D_39_count, 0) AS D_39_count,
    v.D_39_first, 
    v.D_39_last,
    v.D_39_delta_mean,
    v.D_39_delta_max,
    v.D_39_delta_min,
    v.D_39_delta_pd,
    v.D_39_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;