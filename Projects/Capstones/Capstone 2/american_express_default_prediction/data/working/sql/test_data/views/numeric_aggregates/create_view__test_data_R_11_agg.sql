
CREATE VIEW test_data_R_11_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_11 
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
    WHERE R_11 IS NOT NULL
    GROUP BY customer_ID
),
first_R_11 AS
(
    SELECT
        f.customer_ID, s.R_11 AS R_11_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_11 AS
(
    SELECT
        f.customer_ID, s.R_11 AS R_11_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_11_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_11_span
    FROM
        first_last
),
R_11_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_11,
        s.R_11 - LAG(s.R_11, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_11_delta
    FROM
        subset s
),
R_11_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_11_delta
    FROM
        R_11_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_11_delta_per_day AS
(
    SELECT
        customer_ID,
        R_11_delta / date_delta AS R_11_delta_per_day
    FROM
        R_11_delta
),
R_11_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_11_delta_per_day) AS R_11_delta_pd
    FROM
        R_11_delta_per_day
    GROUP BY
        customer_ID
),      
R_11_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_11_delta) AS R_11_delta_mean,
        MAX(R_11_delta) AS R_11_delta_max,
        MIN(R_11_delta) AS R_11_delta_min
    FROM
        R_11_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_11) AS R_11_mean,
        MIN(R_11) AS R_11_min, 
        MAX(R_11) AS R_11_max, 
        SUM(R_11) AS R_11_sum,
        COUNT(R_11) AS R_11_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_11_mean,
        a.R_11_min, 
        a.R_11_max, 
        a.R_11_sum,
        a.R_11_max - a.R_11_min AS R_11_range,
        a.R_11_count,
        f.R_11_first,
        l.R_11_last,
        d.R_11_delta_mean,
        d.R_11_delta_max,
        d.R_11_delta_min,
        pd.R_11_delta_pd,
        cs.R_11_span
    FROM
        aggs a
        LEFT JOIN first_R_11 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_11 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_11_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_11_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_11_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_11_mean, 
    v.R_11_min,
    v.R_11_max, 
    v.R_11_range,
    v.R_11_sum,
    ISNULL(v.R_11_count, 0) AS R_11_count,
    v.R_11_first, 
    v.R_11_last,
    v.R_11_delta_mean,
    v.R_11_delta_max,
    v.R_11_delta_min,
    v.R_11_delta_pd,
    v.R_11_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;