
CREATE VIEW test_data_B_23_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_23 
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
    WHERE B_23 IS NOT NULL
    GROUP BY customer_ID
),
first_B_23 AS
(
    SELECT
        f.customer_ID, s.B_23 AS B_23_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_23 AS
(
    SELECT
        f.customer_ID, s.B_23 AS B_23_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_23_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_23_span
    FROM
        first_last
),
B_23_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_23,
        s.B_23 - LAG(s.B_23, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_23_delta
    FROM
        subset s
),
B_23_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_23_delta
    FROM
        B_23_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_23_delta_per_day AS
(
    SELECT
        customer_ID,
        B_23_delta / date_delta AS B_23_delta_per_day
    FROM
        B_23_delta
),
B_23_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_23_delta_per_day) AS B_23_delta_pd
    FROM
        B_23_delta_per_day
    GROUP BY
        customer_ID
),      
B_23_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_23_delta) AS B_23_delta_mean,
        MAX(B_23_delta) AS B_23_delta_max,
        MIN(B_23_delta) AS B_23_delta_min
    FROM
        B_23_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_23) AS B_23_mean,
        MIN(B_23) AS B_23_min, 
        MAX(B_23) AS B_23_max, 
        SUM(B_23) AS B_23_sum,
        COUNT(B_23) AS B_23_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_23_mean,
        a.B_23_min, 
        a.B_23_max, 
        a.B_23_sum,
        a.B_23_max - a.B_23_min AS B_23_range,
        a.B_23_count,
        f.B_23_first,
        l.B_23_last,
        d.B_23_delta_mean,
        d.B_23_delta_max,
        d.B_23_delta_min,
        pd.B_23_delta_pd,
        cs.B_23_span
    FROM
        aggs a
        LEFT JOIN first_B_23 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_23 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_23_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_23_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_23_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_23_mean, 
    v.B_23_min,
    v.B_23_max, 
    v.B_23_range,
    v.B_23_sum,
    ISNULL(v.B_23_count, 0) AS B_23_count,
    v.B_23_first, 
    v.B_23_last,
    v.B_23_delta_mean,
    v.B_23_delta_max,
    v.B_23_delta_min,
    v.B_23_delta_pd,
    v.B_23_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;