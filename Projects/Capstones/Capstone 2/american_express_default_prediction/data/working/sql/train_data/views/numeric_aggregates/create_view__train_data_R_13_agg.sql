
CREATE VIEW train_data_R_13_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_13 
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
    WHERE R_13 IS NOT NULL
    GROUP BY customer_ID
),
first_R_13 AS
(
    SELECT
        f.customer_ID, s.R_13 AS R_13_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_13 AS
(
    SELECT
        f.customer_ID, s.R_13 AS R_13_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_13_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_13_span
    FROM
        first_last
),
R_13_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_13,
        s.R_13 - LAG(s.R_13, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_13_delta
    FROM
        subset s
),
R_13_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_13_delta
    FROM
        R_13_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_13_delta_per_day AS
(
    SELECT
        customer_ID,
        R_13_delta / date_delta AS R_13_delta_per_day
    FROM
        R_13_delta
),
R_13_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_13_delta_per_day) AS R_13_delta_pd
    FROM
        R_13_delta_per_day
    GROUP BY
        customer_ID
),      
R_13_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_13_delta) AS R_13_delta_mean,
        MAX(R_13_delta) AS R_13_delta_max,
        MIN(R_13_delta) AS R_13_delta_min
    FROM
        R_13_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_13) AS R_13_mean,
        MIN(R_13) AS R_13_min, 
        MAX(R_13) AS R_13_max, 
        SUM(R_13) AS R_13_sum,
        COUNT(R_13) AS R_13_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_13_mean,
        a.R_13_min, 
        a.R_13_max, 
        a.R_13_sum,
        a.R_13_max - a.R_13_min AS R_13_range,
        a.R_13_count,
        f.R_13_first,
        l.R_13_last,
        d.R_13_delta_mean,
        d.R_13_delta_max,
        d.R_13_delta_min,
        pd.R_13_delta_pd,
        cs.R_13_span
    FROM
        aggs a
        LEFT JOIN first_R_13 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_13 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_13_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_13_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_13_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_13_mean, 
    v.R_13_min,
    v.R_13_max, 
    v.R_13_range,
    v.R_13_sum,
    ISNULL(v.R_13_count, 0) AS R_13_count,
    v.R_13_first, 
    v.R_13_last,
    v.R_13_delta_mean,
    v.R_13_delta_max,
    v.R_13_delta_min,
    v.R_13_delta_pd,
    v.R_13_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;