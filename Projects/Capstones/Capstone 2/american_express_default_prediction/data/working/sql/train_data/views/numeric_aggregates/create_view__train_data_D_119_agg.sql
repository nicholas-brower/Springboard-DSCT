
CREATE VIEW train_data_D_119_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_119 
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
    WHERE D_119 IS NOT NULL
    GROUP BY customer_ID
),
first_D_119 AS
(
    SELECT
        f.customer_ID, s.D_119 AS D_119_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_119 AS
(
    SELECT
        f.customer_ID, s.D_119 AS D_119_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_119_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_119_span
    FROM
        first_last
),
D_119_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_119,
        s.D_119 - LAG(s.D_119, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_119_delta
    FROM
        subset s
),
D_119_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_119_delta
    FROM
        D_119_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_119_delta_per_day AS
(
    SELECT
        customer_ID,
        D_119_delta / date_delta AS D_119_delta_per_day
    FROM
        D_119_delta
),
D_119_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_119_delta_per_day) AS D_119_delta_pd
    FROM
        D_119_delta_per_day
    GROUP BY
        customer_ID
),      
D_119_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_119_delta) AS D_119_delta_mean,
        MAX(D_119_delta) AS D_119_delta_max,
        MIN(D_119_delta) AS D_119_delta_min
    FROM
        D_119_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_119) AS D_119_mean,
        MIN(D_119) AS D_119_min, 
        MAX(D_119) AS D_119_max, 
        SUM(D_119) AS D_119_sum,
        COUNT(D_119) AS D_119_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_119_mean,
        a.D_119_min, 
        a.D_119_max, 
        a.D_119_sum,
        a.D_119_max - a.D_119_min AS D_119_range,
        a.D_119_count,
        f.D_119_first,
        l.D_119_last,
        d.D_119_delta_mean,
        d.D_119_delta_max,
        d.D_119_delta_min,
        pd.D_119_delta_pd,
        cs.D_119_span
    FROM
        aggs a
        LEFT JOIN first_D_119 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_119 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_119_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_119_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_119_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_119_mean, 
    v.D_119_min,
    v.D_119_max, 
    v.D_119_range,
    v.D_119_sum,
    ISNULL(v.D_119_count, 0) AS D_119_count,
    v.D_119_first, 
    v.D_119_last,
    v.D_119_delta_mean,
    v.D_119_delta_max,
    v.D_119_delta_min,
    v.D_119_delta_pd,
    v.D_119_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;