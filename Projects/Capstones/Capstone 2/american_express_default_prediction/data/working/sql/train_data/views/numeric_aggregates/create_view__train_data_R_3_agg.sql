
CREATE VIEW train_data_R_3_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_3 
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
    WHERE R_3 IS NOT NULL
    GROUP BY customer_ID
),
first_R_3 AS
(
    SELECT
        f.customer_ID, s.R_3 AS R_3_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_3 AS
(
    SELECT
        f.customer_ID, s.R_3 AS R_3_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_3_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_3_span
    FROM
        first_last
),
R_3_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_3,
        s.R_3 - LAG(s.R_3, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_3_delta
    FROM
        subset s
),
R_3_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_3_delta
    FROM
        R_3_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_3_delta_per_day AS
(
    SELECT
        customer_ID,
        R_3_delta / date_delta AS R_3_delta_per_day
    FROM
        R_3_delta
),
R_3_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_3_delta_per_day) AS R_3_delta_pd
    FROM
        R_3_delta_per_day
    GROUP BY
        customer_ID
),      
R_3_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_3_delta) AS R_3_delta_mean,
        MAX(R_3_delta) AS R_3_delta_max,
        MIN(R_3_delta) AS R_3_delta_min
    FROM
        R_3_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_3) AS R_3_mean,
        MIN(R_3) AS R_3_min, 
        MAX(R_3) AS R_3_max, 
        SUM(R_3) AS R_3_sum,
        COUNT(R_3) AS R_3_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_3_mean,
        a.R_3_min, 
        a.R_3_max, 
        a.R_3_sum,
        a.R_3_max - a.R_3_min AS R_3_range,
        a.R_3_count,
        f.R_3_first,
        l.R_3_last,
        d.R_3_delta_mean,
        d.R_3_delta_max,
        d.R_3_delta_min,
        pd.R_3_delta_pd,
        cs.R_3_span
    FROM
        aggs a
        LEFT JOIN first_R_3 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_3 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_3_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_3_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_3_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_3_mean, 
    v.R_3_min,
    v.R_3_max, 
    v.R_3_range,
    v.R_3_sum,
    ISNULL(v.R_3_count, 0) AS R_3_count,
    v.R_3_first, 
    v.R_3_last,
    v.R_3_delta_mean,
    v.R_3_delta_max,
    v.R_3_delta_min,
    v.R_3_delta_pd,
    v.R_3_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;