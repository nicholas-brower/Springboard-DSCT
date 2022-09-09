
CREATE VIEW train_data_R_2_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_2 
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
    WHERE R_2 IS NOT NULL
    GROUP BY customer_ID
),
first_R_2 AS
(
    SELECT
        f.customer_ID, s.R_2 AS R_2_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_2 AS
(
    SELECT
        f.customer_ID, s.R_2 AS R_2_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_2_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_2_span
    FROM
        first_last
),
R_2_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_2,
        s.R_2 - LAG(s.R_2, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_2_delta
    FROM
        subset s
),
R_2_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_2_delta
    FROM
        R_2_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_2_delta_per_day AS
(
    SELECT
        customer_ID,
        R_2_delta / date_delta AS R_2_delta_per_day
    FROM
        R_2_delta
),
R_2_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_2_delta_per_day) AS R_2_delta_pd
    FROM
        R_2_delta_per_day
    GROUP BY
        customer_ID
),      
R_2_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_2_delta) AS R_2_delta_mean,
        MAX(R_2_delta) AS R_2_delta_max,
        MIN(R_2_delta) AS R_2_delta_min
    FROM
        R_2_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_2) AS R_2_mean,
        MIN(R_2) AS R_2_min, 
        MAX(R_2) AS R_2_max, 
        SUM(R_2) AS R_2_sum,
        COUNT(R_2) AS R_2_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_2_mean,
        a.R_2_min, 
        a.R_2_max, 
        a.R_2_sum,
        a.R_2_max - a.R_2_min AS R_2_range,
        a.R_2_count,
        f.R_2_first,
        l.R_2_last,
        d.R_2_delta_mean,
        d.R_2_delta_max,
        d.R_2_delta_min,
        pd.R_2_delta_pd,
        cs.R_2_span
    FROM
        aggs a
        LEFT JOIN first_R_2 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_2 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_2_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_2_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_2_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_2_mean, 
    v.R_2_min,
    v.R_2_max, 
    v.R_2_range,
    v.R_2_sum,
    ISNULL(v.R_2_count, 0) AS R_2_count,
    v.R_2_first, 
    v.R_2_last,
    v.R_2_delta_mean,
    v.R_2_delta_max,
    v.R_2_delta_min,
    v.R_2_delta_pd,
    v.R_2_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;