
CREATE VIEW test_data_R_9_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_9 
    FROM
        test_data td 
        
),
first_last AS
(
    SELECT 
        customer_ID, 
        MIN(S_2) AS first_dt, 
        MAX(S_2) AS last_dt
    FROM subset
    WHERE R_9 IS NOT NULL
    GROUP BY customer_ID
),
first_R_9 AS
(
    SELECT
        f.customer_ID, s.R_9 AS R_9_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_9 AS
(
    SELECT
        f.customer_ID, s.R_9 AS R_9_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_9_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_9_span
    FROM
        first_last
),
R_9_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_9,
        s.R_9 - LAG(s.R_9, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_9_delta
    FROM
        subset s
),
R_9_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_9_delta
    FROM
        R_9_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_9_delta_per_day AS
(
    SELECT
        customer_ID,
        R_9_delta / date_delta AS R_9_delta_per_day
    FROM
        R_9_delta
),
R_9_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_9_delta_per_day) AS R_9_delta_pd
    FROM
        R_9_delta_per_day
    GROUP BY
        customer_ID
),      
R_9_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_9_delta) AS R_9_delta_mean,
        MAX(R_9_delta) AS R_9_delta_max,
        MIN(R_9_delta) AS R_9_delta_min
    FROM
        R_9_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_9) AS R_9_mean,
        MIN(R_9) AS R_9_min, 
        MAX(R_9) AS R_9_max, 
        SUM(R_9) AS R_9_sum,
        COUNT(R_9) AS R_9_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_9_mean,
        a.R_9_min, 
        a.R_9_max, 
        a.R_9_sum,
        a.R_9_max - a.R_9_min AS R_9_range,
        a.R_9_count,
        f.R_9_first,
        l.R_9_last,
        d.R_9_delta_mean,
        d.R_9_delta_max,
        d.R_9_delta_min,
        pd.R_9_delta_pd,
        cs.R_9_span
    FROM
        aggs a
        LEFT JOIN first_R_9 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_9 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_9_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_9_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_9_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_9_mean, 
    v.R_9_min,
    v.R_9_max, 
    v.R_9_range,
    v.R_9_sum,
    ISNULL(v.R_9_count, 0) AS R_9_count,
    v.R_9_first, 
    v.R_9_last,
    v.R_9_delta_mean,
    v.R_9_delta_max,
    v.R_9_delta_min,
    v.R_9_delta_pd,
    v.R_9_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;