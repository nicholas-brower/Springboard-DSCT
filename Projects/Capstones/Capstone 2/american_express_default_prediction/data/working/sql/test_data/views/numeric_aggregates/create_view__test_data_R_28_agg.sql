
CREATE VIEW test_data_R_28_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_28 
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
    WHERE R_28 IS NOT NULL
    GROUP BY customer_ID
),
first_R_28 AS
(
    SELECT
        f.customer_ID, s.R_28 AS R_28_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_28 AS
(
    SELECT
        f.customer_ID, s.R_28 AS R_28_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_28_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_28_span
    FROM
        first_last
),
R_28_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_28,
        s.R_28 - LAG(s.R_28, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_28_delta
    FROM
        subset s
),
R_28_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_28_delta
    FROM
        R_28_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_28_delta_per_day AS
(
    SELECT
        customer_ID,
        R_28_delta / date_delta AS R_28_delta_per_day
    FROM
        R_28_delta
),
R_28_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_28_delta_per_day) AS R_28_delta_pd
    FROM
        R_28_delta_per_day
    GROUP BY
        customer_ID
),      
R_28_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_28_delta) AS R_28_delta_mean,
        MAX(R_28_delta) AS R_28_delta_max,
        MIN(R_28_delta) AS R_28_delta_min
    FROM
        R_28_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_28) AS R_28_mean,
        MIN(R_28) AS R_28_min, 
        MAX(R_28) AS R_28_max, 
        SUM(R_28) AS R_28_sum,
        COUNT(R_28) AS R_28_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_28_mean,
        a.R_28_min, 
        a.R_28_max, 
        a.R_28_sum,
        a.R_28_max - a.R_28_min AS R_28_range,
        a.R_28_count,
        f.R_28_first,
        l.R_28_last,
        d.R_28_delta_mean,
        d.R_28_delta_max,
        d.R_28_delta_min,
        pd.R_28_delta_pd,
        cs.R_28_span
    FROM
        aggs a
        LEFT JOIN first_R_28 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_28 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_28_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_28_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_28_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_28_mean, 
    v.R_28_min,
    v.R_28_max, 
    v.R_28_range,
    v.R_28_sum,
    ISNULL(v.R_28_count, 0) AS R_28_count,
    v.R_28_first, 
    v.R_28_last,
    v.R_28_delta_mean,
    v.R_28_delta_max,
    v.R_28_delta_min,
    v.R_28_delta_pd,
    v.R_28_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;