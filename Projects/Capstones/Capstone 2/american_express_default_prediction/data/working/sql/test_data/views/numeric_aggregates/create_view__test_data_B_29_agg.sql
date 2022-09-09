
CREATE VIEW test_data_B_29_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_29 
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
    WHERE B_29 IS NOT NULL
    GROUP BY customer_ID
),
first_B_29 AS
(
    SELECT
        f.customer_ID, s.B_29 AS B_29_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_29 AS
(
    SELECT
        f.customer_ID, s.B_29 AS B_29_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_29_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_29_span
    FROM
        first_last
),
B_29_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_29,
        s.B_29 - LAG(s.B_29, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_29_delta
    FROM
        subset s
),
B_29_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_29_delta
    FROM
        B_29_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_29_delta_per_day AS
(
    SELECT
        customer_ID,
        B_29_delta / date_delta AS B_29_delta_per_day
    FROM
        B_29_delta
),
B_29_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_29_delta_per_day) AS B_29_delta_pd
    FROM
        B_29_delta_per_day
    GROUP BY
        customer_ID
),      
B_29_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_29_delta) AS B_29_delta_mean,
        MAX(B_29_delta) AS B_29_delta_max,
        MIN(B_29_delta) AS B_29_delta_min
    FROM
        B_29_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_29) AS B_29_mean,
        MIN(B_29) AS B_29_min, 
        MAX(B_29) AS B_29_max, 
        SUM(B_29) AS B_29_sum,
        COUNT(B_29) AS B_29_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_29_mean,
        a.B_29_min, 
        a.B_29_max, 
        a.B_29_sum,
        a.B_29_max - a.B_29_min AS B_29_range,
        a.B_29_count,
        f.B_29_first,
        l.B_29_last,
        d.B_29_delta_mean,
        d.B_29_delta_max,
        d.B_29_delta_min,
        pd.B_29_delta_pd,
        cs.B_29_span
    FROM
        aggs a
        LEFT JOIN first_B_29 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_29 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_29_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_29_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_29_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_29_mean, 
    v.B_29_min,
    v.B_29_max, 
    v.B_29_range,
    v.B_29_sum,
    ISNULL(v.B_29_count, 0) AS B_29_count,
    v.B_29_first, 
    v.B_29_last,
    v.B_29_delta_mean,
    v.B_29_delta_max,
    v.B_29_delta_min,
    v.B_29_delta_pd,
    v.B_29_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;