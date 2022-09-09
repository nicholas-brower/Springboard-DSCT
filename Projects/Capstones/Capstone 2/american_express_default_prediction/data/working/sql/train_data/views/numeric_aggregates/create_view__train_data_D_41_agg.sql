
CREATE VIEW train_data_D_41_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_41 
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
    WHERE D_41 IS NOT NULL
    GROUP BY customer_ID
),
first_D_41 AS
(
    SELECT
        f.customer_ID, s.D_41 AS D_41_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_41 AS
(
    SELECT
        f.customer_ID, s.D_41 AS D_41_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_41_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_41_span
    FROM
        first_last
),
D_41_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_41,
        s.D_41 - LAG(s.D_41, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_41_delta
    FROM
        subset s
),
D_41_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_41_delta
    FROM
        D_41_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_41_delta_per_day AS
(
    SELECT
        customer_ID,
        D_41_delta / date_delta AS D_41_delta_per_day
    FROM
        D_41_delta
),
D_41_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_41_delta_per_day) AS D_41_delta_pd
    FROM
        D_41_delta_per_day
    GROUP BY
        customer_ID
),      
D_41_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_41_delta) AS D_41_delta_mean,
        MAX(D_41_delta) AS D_41_delta_max,
        MIN(D_41_delta) AS D_41_delta_min
    FROM
        D_41_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_41) AS D_41_mean,
        MIN(D_41) AS D_41_min, 
        MAX(D_41) AS D_41_max, 
        SUM(D_41) AS D_41_sum,
        COUNT(D_41) AS D_41_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_41_mean,
        a.D_41_min, 
        a.D_41_max, 
        a.D_41_sum,
        a.D_41_max - a.D_41_min AS D_41_range,
        a.D_41_count,
        f.D_41_first,
        l.D_41_last,
        d.D_41_delta_mean,
        d.D_41_delta_max,
        d.D_41_delta_min,
        pd.D_41_delta_pd,
        cs.D_41_span
    FROM
        aggs a
        LEFT JOIN first_D_41 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_41 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_41_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_41_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_41_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_41_mean, 
    v.D_41_min,
    v.D_41_max, 
    v.D_41_range,
    v.D_41_sum,
    ISNULL(v.D_41_count, 0) AS D_41_count,
    v.D_41_first, 
    v.D_41_last,
    v.D_41_delta_mean,
    v.D_41_delta_max,
    v.D_41_delta_min,
    v.D_41_delta_pd,
    v.D_41_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;