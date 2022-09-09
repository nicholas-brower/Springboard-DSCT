
CREATE VIEW test_data_B_3_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_3 
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
    WHERE B_3 IS NOT NULL
    GROUP BY customer_ID
),
first_B_3 AS
(
    SELECT
        f.customer_ID, s.B_3 AS B_3_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_3 AS
(
    SELECT
        f.customer_ID, s.B_3 AS B_3_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_3_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_3_span
    FROM
        first_last
),
B_3_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_3,
        s.B_3 - LAG(s.B_3, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_3_delta
    FROM
        subset s
),
B_3_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_3_delta
    FROM
        B_3_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_3_delta_per_day AS
(
    SELECT
        customer_ID,
        B_3_delta / date_delta AS B_3_delta_per_day
    FROM
        B_3_delta
),
B_3_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_3_delta_per_day) AS B_3_delta_pd
    FROM
        B_3_delta_per_day
    GROUP BY
        customer_ID
),      
B_3_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_3_delta) AS B_3_delta_mean,
        MAX(B_3_delta) AS B_3_delta_max,
        MIN(B_3_delta) AS B_3_delta_min
    FROM
        B_3_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_3) AS B_3_mean,
        MIN(B_3) AS B_3_min, 
        MAX(B_3) AS B_3_max, 
        SUM(B_3) AS B_3_sum,
        COUNT(B_3) AS B_3_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_3_mean,
        a.B_3_min, 
        a.B_3_max, 
        a.B_3_sum,
        a.B_3_max - a.B_3_min AS B_3_range,
        a.B_3_count,
        f.B_3_first,
        l.B_3_last,
        d.B_3_delta_mean,
        d.B_3_delta_max,
        d.B_3_delta_min,
        pd.B_3_delta_pd,
        cs.B_3_span
    FROM
        aggs a
        LEFT JOIN first_B_3 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_3 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_3_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_3_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_3_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_3_mean, 
    v.B_3_min,
    v.B_3_max, 
    v.B_3_range,
    v.B_3_sum,
    ISNULL(v.B_3_count, 0) AS B_3_count,
    v.B_3_first, 
    v.B_3_last,
    v.B_3_delta_mean,
    v.B_3_delta_max,
    v.B_3_delta_min,
    v.B_3_delta_pd,
    v.B_3_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;