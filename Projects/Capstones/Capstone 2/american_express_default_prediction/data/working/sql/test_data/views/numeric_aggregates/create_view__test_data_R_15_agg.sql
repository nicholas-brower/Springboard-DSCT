
CREATE VIEW test_data_R_15_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_15 
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
    WHERE R_15 IS NOT NULL
    GROUP BY customer_ID
),
first_R_15 AS
(
    SELECT
        f.customer_ID, s.R_15 AS R_15_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_15 AS
(
    SELECT
        f.customer_ID, s.R_15 AS R_15_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_15_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_15_span
    FROM
        first_last
),
R_15_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_15,
        s.R_15 - LAG(s.R_15, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_15_delta
    FROM
        subset s
),
R_15_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_15_delta
    FROM
        R_15_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_15_delta_per_day AS
(
    SELECT
        customer_ID,
        R_15_delta / date_delta AS R_15_delta_per_day
    FROM
        R_15_delta
),
R_15_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_15_delta_per_day) AS R_15_delta_pd
    FROM
        R_15_delta_per_day
    GROUP BY
        customer_ID
),      
R_15_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_15_delta) AS R_15_delta_mean,
        MAX(R_15_delta) AS R_15_delta_max,
        MIN(R_15_delta) AS R_15_delta_min
    FROM
        R_15_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_15) AS R_15_mean,
        MIN(R_15) AS R_15_min, 
        MAX(R_15) AS R_15_max, 
        SUM(R_15) AS R_15_sum,
        COUNT(R_15) AS R_15_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_15_mean,
        a.R_15_min, 
        a.R_15_max, 
        a.R_15_sum,
        a.R_15_max - a.R_15_min AS R_15_range,
        a.R_15_count,
        f.R_15_first,
        l.R_15_last,
        d.R_15_delta_mean,
        d.R_15_delta_max,
        d.R_15_delta_min,
        pd.R_15_delta_pd,
        cs.R_15_span
    FROM
        aggs a
        LEFT JOIN first_R_15 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_15 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_15_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_15_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_15_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_15_mean, 
    v.R_15_min,
    v.R_15_max, 
    v.R_15_range,
    v.R_15_sum,
    ISNULL(v.R_15_count, 0) AS R_15_count,
    v.R_15_first, 
    v.R_15_last,
    v.R_15_delta_mean,
    v.R_15_delta_max,
    v.R_15_delta_min,
    v.R_15_delta_pd,
    v.R_15_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;