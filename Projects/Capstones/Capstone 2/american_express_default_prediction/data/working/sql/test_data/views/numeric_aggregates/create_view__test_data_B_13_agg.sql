
CREATE VIEW test_data_B_13_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_13 
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
    WHERE B_13 IS NOT NULL
    GROUP BY customer_ID
),
first_B_13 AS
(
    SELECT
        f.customer_ID, s.B_13 AS B_13_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_13 AS
(
    SELECT
        f.customer_ID, s.B_13 AS B_13_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_13_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_13_span
    FROM
        first_last
),
B_13_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_13,
        s.B_13 - LAG(s.B_13, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_13_delta
    FROM
        subset s
),
B_13_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_13_delta
    FROM
        B_13_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_13_delta_per_day AS
(
    SELECT
        customer_ID,
        B_13_delta / date_delta AS B_13_delta_per_day
    FROM
        B_13_delta
),
B_13_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_13_delta_per_day) AS B_13_delta_pd
    FROM
        B_13_delta_per_day
    GROUP BY
        customer_ID
),      
B_13_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_13_delta) AS B_13_delta_mean,
        MAX(B_13_delta) AS B_13_delta_max,
        MIN(B_13_delta) AS B_13_delta_min
    FROM
        B_13_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_13) AS B_13_mean,
        MIN(B_13) AS B_13_min, 
        MAX(B_13) AS B_13_max, 
        SUM(B_13) AS B_13_sum,
        COUNT(B_13) AS B_13_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_13_mean,
        a.B_13_min, 
        a.B_13_max, 
        a.B_13_sum,
        a.B_13_max - a.B_13_min AS B_13_range,
        a.B_13_count,
        f.B_13_first,
        l.B_13_last,
        d.B_13_delta_mean,
        d.B_13_delta_max,
        d.B_13_delta_min,
        pd.B_13_delta_pd,
        cs.B_13_span
    FROM
        aggs a
        LEFT JOIN first_B_13 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_13 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_13_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_13_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_13_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_13_mean, 
    v.B_13_min,
    v.B_13_max, 
    v.B_13_range,
    v.B_13_sum,
    ISNULL(v.B_13_count, 0) AS B_13_count,
    v.B_13_first, 
    v.B_13_last,
    v.B_13_delta_mean,
    v.B_13_delta_max,
    v.B_13_delta_min,
    v.B_13_delta_pd,
    v.B_13_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;