
CREATE VIEW test_data_R_10_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_10 
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
    WHERE R_10 IS NOT NULL
    GROUP BY customer_ID
),
first_R_10 AS
(
    SELECT
        f.customer_ID, s.R_10 AS R_10_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_10 AS
(
    SELECT
        f.customer_ID, s.R_10 AS R_10_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_10_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_10_span
    FROM
        first_last
),
R_10_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_10,
        s.R_10 - LAG(s.R_10, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_10_delta
    FROM
        subset s
),
R_10_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_10_delta
    FROM
        R_10_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_10_delta_per_day AS
(
    SELECT
        customer_ID,
        R_10_delta / date_delta AS R_10_delta_per_day
    FROM
        R_10_delta
),
R_10_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_10_delta_per_day) AS R_10_delta_pd
    FROM
        R_10_delta_per_day
    GROUP BY
        customer_ID
),      
R_10_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_10_delta) AS R_10_delta_mean,
        MAX(R_10_delta) AS R_10_delta_max,
        MIN(R_10_delta) AS R_10_delta_min
    FROM
        R_10_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_10) AS R_10_mean,
        MIN(R_10) AS R_10_min, 
        MAX(R_10) AS R_10_max, 
        SUM(R_10) AS R_10_sum,
        COUNT(R_10) AS R_10_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_10_mean,
        a.R_10_min, 
        a.R_10_max, 
        a.R_10_sum,
        a.R_10_max - a.R_10_min AS R_10_range,
        a.R_10_count,
        f.R_10_first,
        l.R_10_last,
        d.R_10_delta_mean,
        d.R_10_delta_max,
        d.R_10_delta_min,
        pd.R_10_delta_pd,
        cs.R_10_span
    FROM
        aggs a
        LEFT JOIN first_R_10 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_10 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_10_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_10_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_10_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_10_mean, 
    v.R_10_min,
    v.R_10_max, 
    v.R_10_range,
    v.R_10_sum,
    ISNULL(v.R_10_count, 0) AS R_10_count,
    v.R_10_first, 
    v.R_10_last,
    v.R_10_delta_mean,
    v.R_10_delta_max,
    v.R_10_delta_min,
    v.R_10_delta_pd,
    v.R_10_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;