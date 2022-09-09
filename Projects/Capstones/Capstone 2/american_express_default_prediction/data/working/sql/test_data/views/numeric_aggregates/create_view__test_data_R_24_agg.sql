
CREATE VIEW test_data_R_24_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_24 
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
    WHERE R_24 IS NOT NULL
    GROUP BY customer_ID
),
first_R_24 AS
(
    SELECT
        f.customer_ID, s.R_24 AS R_24_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_24 AS
(
    SELECT
        f.customer_ID, s.R_24 AS R_24_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_24_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_24_span
    FROM
        first_last
),
R_24_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_24,
        s.R_24 - LAG(s.R_24, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_24_delta
    FROM
        subset s
),
R_24_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_24_delta
    FROM
        R_24_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_24_delta_per_day AS
(
    SELECT
        customer_ID,
        R_24_delta / date_delta AS R_24_delta_per_day
    FROM
        R_24_delta
),
R_24_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_24_delta_per_day) AS R_24_delta_pd
    FROM
        R_24_delta_per_day
    GROUP BY
        customer_ID
),      
R_24_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_24_delta) AS R_24_delta_mean,
        MAX(R_24_delta) AS R_24_delta_max,
        MIN(R_24_delta) AS R_24_delta_min
    FROM
        R_24_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_24) AS R_24_mean,
        MIN(R_24) AS R_24_min, 
        MAX(R_24) AS R_24_max, 
        SUM(R_24) AS R_24_sum,
        COUNT(R_24) AS R_24_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_24_mean,
        a.R_24_min, 
        a.R_24_max, 
        a.R_24_sum,
        a.R_24_max - a.R_24_min AS R_24_range,
        a.R_24_count,
        f.R_24_first,
        l.R_24_last,
        d.R_24_delta_mean,
        d.R_24_delta_max,
        d.R_24_delta_min,
        pd.R_24_delta_pd,
        cs.R_24_span
    FROM
        aggs a
        LEFT JOIN first_R_24 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_24 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_24_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_24_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_24_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_24_mean, 
    v.R_24_min,
    v.R_24_max, 
    v.R_24_range,
    v.R_24_sum,
    ISNULL(v.R_24_count, 0) AS R_24_count,
    v.R_24_first, 
    v.R_24_last,
    v.R_24_delta_mean,
    v.R_24_delta_max,
    v.R_24_delta_min,
    v.R_24_delta_pd,
    v.R_24_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;