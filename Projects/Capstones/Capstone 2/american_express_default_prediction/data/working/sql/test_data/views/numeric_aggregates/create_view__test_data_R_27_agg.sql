
CREATE VIEW test_data_R_27_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_27 
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
    WHERE R_27 IS NOT NULL
    GROUP BY customer_ID
),
first_R_27 AS
(
    SELECT
        f.customer_ID, s.R_27 AS R_27_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_27 AS
(
    SELECT
        f.customer_ID, s.R_27 AS R_27_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_27_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_27_span
    FROM
        first_last
),
R_27_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_27,
        s.R_27 - LAG(s.R_27, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_27_delta
    FROM
        subset s
),
R_27_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_27_delta
    FROM
        R_27_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_27_delta_per_day AS
(
    SELECT
        customer_ID,
        R_27_delta / date_delta AS R_27_delta_per_day
    FROM
        R_27_delta
),
R_27_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_27_delta_per_day) AS R_27_delta_pd
    FROM
        R_27_delta_per_day
    GROUP BY
        customer_ID
),      
R_27_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_27_delta) AS R_27_delta_mean,
        MAX(R_27_delta) AS R_27_delta_max,
        MIN(R_27_delta) AS R_27_delta_min
    FROM
        R_27_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_27) AS R_27_mean,
        MIN(R_27) AS R_27_min, 
        MAX(R_27) AS R_27_max, 
        SUM(R_27) AS R_27_sum,
        COUNT(R_27) AS R_27_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_27_mean,
        a.R_27_min, 
        a.R_27_max, 
        a.R_27_sum,
        a.R_27_max - a.R_27_min AS R_27_range,
        a.R_27_count,
        f.R_27_first,
        l.R_27_last,
        d.R_27_delta_mean,
        d.R_27_delta_max,
        d.R_27_delta_min,
        pd.R_27_delta_pd,
        cs.R_27_span
    FROM
        aggs a
        LEFT JOIN first_R_27 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_27 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_27_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_27_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_27_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_27_mean, 
    v.R_27_min,
    v.R_27_max, 
    v.R_27_range,
    v.R_27_sum,
    ISNULL(v.R_27_count, 0) AS R_27_count,
    v.R_27_first, 
    v.R_27_last,
    v.R_27_delta_mean,
    v.R_27_delta_max,
    v.R_27_delta_min,
    v.R_27_delta_pd,
    v.R_27_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;