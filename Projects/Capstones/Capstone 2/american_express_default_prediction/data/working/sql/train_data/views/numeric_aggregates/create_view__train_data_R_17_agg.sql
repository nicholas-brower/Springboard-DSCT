
CREATE VIEW train_data_R_17_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_17 
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
    WHERE R_17 IS NOT NULL
    GROUP BY customer_ID
),
first_R_17 AS
(
    SELECT
        f.customer_ID, s.R_17 AS R_17_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_17 AS
(
    SELECT
        f.customer_ID, s.R_17 AS R_17_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_17_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_17_span
    FROM
        first_last
),
R_17_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_17,
        s.R_17 - LAG(s.R_17, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_17_delta
    FROM
        subset s
),
R_17_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_17_delta
    FROM
        R_17_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_17_delta_per_day AS
(
    SELECT
        customer_ID,
        R_17_delta / date_delta AS R_17_delta_per_day
    FROM
        R_17_delta
),
R_17_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_17_delta_per_day) AS R_17_delta_pd
    FROM
        R_17_delta_per_day
    GROUP BY
        customer_ID
),      
R_17_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_17_delta) AS R_17_delta_mean,
        MAX(R_17_delta) AS R_17_delta_max,
        MIN(R_17_delta) AS R_17_delta_min
    FROM
        R_17_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_17) AS R_17_mean,
        MIN(R_17) AS R_17_min, 
        MAX(R_17) AS R_17_max, 
        SUM(R_17) AS R_17_sum,
        COUNT(R_17) AS R_17_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_17_mean,
        a.R_17_min, 
        a.R_17_max, 
        a.R_17_sum,
        a.R_17_max - a.R_17_min AS R_17_range,
        a.R_17_count,
        f.R_17_first,
        l.R_17_last,
        d.R_17_delta_mean,
        d.R_17_delta_max,
        d.R_17_delta_min,
        pd.R_17_delta_pd,
        cs.R_17_span
    FROM
        aggs a
        LEFT JOIN first_R_17 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_17 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_17_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_17_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_17_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_17_mean, 
    v.R_17_min,
    v.R_17_max, 
    v.R_17_range,
    v.R_17_sum,
    ISNULL(v.R_17_count, 0) AS R_17_count,
    v.R_17_first, 
    v.R_17_last,
    v.R_17_delta_mean,
    v.R_17_delta_max,
    v.R_17_delta_min,
    v.R_17_delta_pd,
    v.R_17_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;