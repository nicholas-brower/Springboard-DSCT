
CREATE VIEW test_data_B_25_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_25 
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
    WHERE B_25 IS NOT NULL
    GROUP BY customer_ID
),
first_B_25 AS
(
    SELECT
        f.customer_ID, s.B_25 AS B_25_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_25 AS
(
    SELECT
        f.customer_ID, s.B_25 AS B_25_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_25_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_25_span
    FROM
        first_last
),
B_25_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_25,
        s.B_25 - LAG(s.B_25, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_25_delta
    FROM
        subset s
),
B_25_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_25_delta
    FROM
        B_25_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_25_delta_per_day AS
(
    SELECT
        customer_ID,
        B_25_delta / date_delta AS B_25_delta_per_day
    FROM
        B_25_delta
),
B_25_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_25_delta_per_day) AS B_25_delta_pd
    FROM
        B_25_delta_per_day
    GROUP BY
        customer_ID
),      
B_25_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_25_delta) AS B_25_delta_mean,
        MAX(B_25_delta) AS B_25_delta_max,
        MIN(B_25_delta) AS B_25_delta_min
    FROM
        B_25_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_25) AS B_25_mean,
        MIN(B_25) AS B_25_min, 
        MAX(B_25) AS B_25_max, 
        SUM(B_25) AS B_25_sum,
        COUNT(B_25) AS B_25_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_25_mean,
        a.B_25_min, 
        a.B_25_max, 
        a.B_25_sum,
        a.B_25_max - a.B_25_min AS B_25_range,
        a.B_25_count,
        f.B_25_first,
        l.B_25_last,
        d.B_25_delta_mean,
        d.B_25_delta_max,
        d.B_25_delta_min,
        pd.B_25_delta_pd,
        cs.B_25_span
    FROM
        aggs a
        LEFT JOIN first_B_25 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_25 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_25_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_25_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_25_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_25_mean, 
    v.B_25_min,
    v.B_25_max, 
    v.B_25_range,
    v.B_25_sum,
    ISNULL(v.B_25_count, 0) AS B_25_count,
    v.B_25_first, 
    v.B_25_last,
    v.B_25_delta_mean,
    v.B_25_delta_max,
    v.B_25_delta_min,
    v.B_25_delta_pd,
    v.B_25_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;