
CREATE VIEW test_data_B_24_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_24 
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
    WHERE B_24 IS NOT NULL
    GROUP BY customer_ID
),
first_B_24 AS
(
    SELECT
        f.customer_ID, s.B_24 AS B_24_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_24 AS
(
    SELECT
        f.customer_ID, s.B_24 AS B_24_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_24_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_24_span
    FROM
        first_last
),
B_24_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_24,
        s.B_24 - LAG(s.B_24, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_24_delta
    FROM
        subset s
),
B_24_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_24_delta
    FROM
        B_24_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_24_delta_per_day AS
(
    SELECT
        customer_ID,
        B_24_delta / date_delta AS B_24_delta_per_day
    FROM
        B_24_delta
),
B_24_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_24_delta_per_day) AS B_24_delta_pd
    FROM
        B_24_delta_per_day
    GROUP BY
        customer_ID
),      
B_24_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_24_delta) AS B_24_delta_mean,
        MAX(B_24_delta) AS B_24_delta_max,
        MIN(B_24_delta) AS B_24_delta_min
    FROM
        B_24_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_24) AS B_24_mean,
        MIN(B_24) AS B_24_min, 
        MAX(B_24) AS B_24_max, 
        SUM(B_24) AS B_24_sum,
        COUNT(B_24) AS B_24_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_24_mean,
        a.B_24_min, 
        a.B_24_max, 
        a.B_24_sum,
        a.B_24_max - a.B_24_min AS B_24_range,
        a.B_24_count,
        f.B_24_first,
        l.B_24_last,
        d.B_24_delta_mean,
        d.B_24_delta_max,
        d.B_24_delta_min,
        pd.B_24_delta_pd,
        cs.B_24_span
    FROM
        aggs a
        LEFT JOIN first_B_24 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_24 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_24_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_24_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_24_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_24_mean, 
    v.B_24_min,
    v.B_24_max, 
    v.B_24_range,
    v.B_24_sum,
    ISNULL(v.B_24_count, 0) AS B_24_count,
    v.B_24_first, 
    v.B_24_last,
    v.B_24_delta_mean,
    v.B_24_delta_max,
    v.B_24_delta_min,
    v.B_24_delta_pd,
    v.B_24_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;