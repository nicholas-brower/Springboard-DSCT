
CREATE VIEW test_data_B_4_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_4 
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
    WHERE B_4 IS NOT NULL
    GROUP BY customer_ID
),
first_B_4 AS
(
    SELECT
        f.customer_ID, s.B_4 AS B_4_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_4 AS
(
    SELECT
        f.customer_ID, s.B_4 AS B_4_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_4_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_4_span
    FROM
        first_last
),
B_4_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_4,
        s.B_4 - LAG(s.B_4, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_4_delta
    FROM
        subset s
),
B_4_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_4_delta
    FROM
        B_4_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_4_delta_per_day AS
(
    SELECT
        customer_ID,
        B_4_delta / date_delta AS B_4_delta_per_day
    FROM
        B_4_delta
),
B_4_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_4_delta_per_day) AS B_4_delta_pd
    FROM
        B_4_delta_per_day
    GROUP BY
        customer_ID
),      
B_4_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_4_delta) AS B_4_delta_mean,
        MAX(B_4_delta) AS B_4_delta_max,
        MIN(B_4_delta) AS B_4_delta_min
    FROM
        B_4_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_4) AS B_4_mean,
        MIN(B_4) AS B_4_min, 
        MAX(B_4) AS B_4_max, 
        SUM(B_4) AS B_4_sum,
        COUNT(B_4) AS B_4_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_4_mean,
        a.B_4_min, 
        a.B_4_max, 
        a.B_4_sum,
        a.B_4_max - a.B_4_min AS B_4_range,
        a.B_4_count,
        f.B_4_first,
        l.B_4_last,
        d.B_4_delta_mean,
        d.B_4_delta_max,
        d.B_4_delta_min,
        pd.B_4_delta_pd,
        cs.B_4_span
    FROM
        aggs a
        LEFT JOIN first_B_4 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_4 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_4_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_4_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_4_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_4_mean, 
    v.B_4_min,
    v.B_4_max, 
    v.B_4_range,
    v.B_4_sum,
    ISNULL(v.B_4_count, 0) AS B_4_count,
    v.B_4_first, 
    v.B_4_last,
    v.B_4_delta_mean,
    v.B_4_delta_max,
    v.B_4_delta_min,
    v.B_4_delta_pd,
    v.B_4_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;