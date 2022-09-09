
CREATE VIEW test_data_R_18_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_18 
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
    WHERE R_18 IS NOT NULL
    GROUP BY customer_ID
),
first_R_18 AS
(
    SELECT
        f.customer_ID, s.R_18 AS R_18_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_18 AS
(
    SELECT
        f.customer_ID, s.R_18 AS R_18_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_18_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_18_span
    FROM
        first_last
),
R_18_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_18,
        s.R_18 - LAG(s.R_18, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_18_delta
    FROM
        subset s
),
R_18_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_18_delta
    FROM
        R_18_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_18_delta_per_day AS
(
    SELECT
        customer_ID,
        R_18_delta / date_delta AS R_18_delta_per_day
    FROM
        R_18_delta
),
R_18_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_18_delta_per_day) AS R_18_delta_pd
    FROM
        R_18_delta_per_day
    GROUP BY
        customer_ID
),      
R_18_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_18_delta) AS R_18_delta_mean,
        MAX(R_18_delta) AS R_18_delta_max,
        MIN(R_18_delta) AS R_18_delta_min
    FROM
        R_18_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_18) AS R_18_mean,
        MIN(R_18) AS R_18_min, 
        MAX(R_18) AS R_18_max, 
        SUM(R_18) AS R_18_sum,
        COUNT(R_18) AS R_18_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_18_mean,
        a.R_18_min, 
        a.R_18_max, 
        a.R_18_sum,
        a.R_18_max - a.R_18_min AS R_18_range,
        a.R_18_count,
        f.R_18_first,
        l.R_18_last,
        d.R_18_delta_mean,
        d.R_18_delta_max,
        d.R_18_delta_min,
        pd.R_18_delta_pd,
        cs.R_18_span
    FROM
        aggs a
        LEFT JOIN first_R_18 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_18 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_18_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_18_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_18_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_18_mean, 
    v.R_18_min,
    v.R_18_max, 
    v.R_18_range,
    v.R_18_sum,
    ISNULL(v.R_18_count, 0) AS R_18_count,
    v.R_18_first, 
    v.R_18_last,
    v.R_18_delta_mean,
    v.R_18_delta_max,
    v.R_18_delta_min,
    v.R_18_delta_pd,
    v.R_18_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;