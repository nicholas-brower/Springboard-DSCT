
CREATE VIEW test_data_R_8_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_8 
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
    WHERE R_8 IS NOT NULL
    GROUP BY customer_ID
),
first_R_8 AS
(
    SELECT
        f.customer_ID, s.R_8 AS R_8_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_8 AS
(
    SELECT
        f.customer_ID, s.R_8 AS R_8_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_8_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_8_span
    FROM
        first_last
),
R_8_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_8,
        s.R_8 - LAG(s.R_8, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_8_delta
    FROM
        subset s
),
R_8_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_8_delta
    FROM
        R_8_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_8_delta_per_day AS
(
    SELECT
        customer_ID,
        R_8_delta / date_delta AS R_8_delta_per_day
    FROM
        R_8_delta
),
R_8_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_8_delta_per_day) AS R_8_delta_pd
    FROM
        R_8_delta_per_day
    GROUP BY
        customer_ID
),      
R_8_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_8_delta) AS R_8_delta_mean,
        MAX(R_8_delta) AS R_8_delta_max,
        MIN(R_8_delta) AS R_8_delta_min
    FROM
        R_8_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_8) AS R_8_mean,
        MIN(R_8) AS R_8_min, 
        MAX(R_8) AS R_8_max, 
        SUM(R_8) AS R_8_sum,
        COUNT(R_8) AS R_8_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_8_mean,
        a.R_8_min, 
        a.R_8_max, 
        a.R_8_sum,
        a.R_8_max - a.R_8_min AS R_8_range,
        a.R_8_count,
        f.R_8_first,
        l.R_8_last,
        d.R_8_delta_mean,
        d.R_8_delta_max,
        d.R_8_delta_min,
        pd.R_8_delta_pd,
        cs.R_8_span
    FROM
        aggs a
        LEFT JOIN first_R_8 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_8 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_8_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_8_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_8_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_8_mean, 
    v.R_8_min,
    v.R_8_max, 
    v.R_8_range,
    v.R_8_sum,
    ISNULL(v.R_8_count, 0) AS R_8_count,
    v.R_8_first, 
    v.R_8_last,
    v.R_8_delta_mean,
    v.R_8_delta_max,
    v.R_8_delta_min,
    v.R_8_delta_pd,
    v.R_8_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;