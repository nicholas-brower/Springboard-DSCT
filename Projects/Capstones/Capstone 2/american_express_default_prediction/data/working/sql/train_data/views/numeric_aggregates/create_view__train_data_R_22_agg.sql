
CREATE VIEW train_data_R_22_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_22 
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
    WHERE R_22 IS NOT NULL
    GROUP BY customer_ID
),
first_R_22 AS
(
    SELECT
        f.customer_ID, s.R_22 AS R_22_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_22 AS
(
    SELECT
        f.customer_ID, s.R_22 AS R_22_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_22_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_22_span
    FROM
        first_last
),
R_22_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_22,
        s.R_22 - LAG(s.R_22, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_22_delta
    FROM
        subset s
),
R_22_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_22_delta
    FROM
        R_22_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_22_delta_per_day AS
(
    SELECT
        customer_ID,
        R_22_delta / date_delta AS R_22_delta_per_day
    FROM
        R_22_delta
),
R_22_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_22_delta_per_day) AS R_22_delta_pd
    FROM
        R_22_delta_per_day
    GROUP BY
        customer_ID
),      
R_22_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_22_delta) AS R_22_delta_mean,
        MAX(R_22_delta) AS R_22_delta_max,
        MIN(R_22_delta) AS R_22_delta_min
    FROM
        R_22_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_22) AS R_22_mean,
        MIN(R_22) AS R_22_min, 
        MAX(R_22) AS R_22_max, 
        SUM(R_22) AS R_22_sum,
        COUNT(R_22) AS R_22_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_22_mean,
        a.R_22_min, 
        a.R_22_max, 
        a.R_22_sum,
        a.R_22_max - a.R_22_min AS R_22_range,
        a.R_22_count,
        f.R_22_first,
        l.R_22_last,
        d.R_22_delta_mean,
        d.R_22_delta_max,
        d.R_22_delta_min,
        pd.R_22_delta_pd,
        cs.R_22_span
    FROM
        aggs a
        LEFT JOIN first_R_22 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_22 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_22_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_22_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_22_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_22_mean, 
    v.R_22_min,
    v.R_22_max, 
    v.R_22_range,
    v.R_22_sum,
    ISNULL(v.R_22_count, 0) AS R_22_count,
    v.R_22_first, 
    v.R_22_last,
    v.R_22_delta_mean,
    v.R_22_delta_max,
    v.R_22_delta_min,
    v.R_22_delta_pd,
    v.R_22_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;