
CREATE VIEW test_data_B_39_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_39 
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
    WHERE B_39 IS NOT NULL
    GROUP BY customer_ID
),
first_B_39 AS
(
    SELECT
        f.customer_ID, s.B_39 AS B_39_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_39 AS
(
    SELECT
        f.customer_ID, s.B_39 AS B_39_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_39_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_39_span
    FROM
        first_last
),
B_39_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_39,
        s.B_39 - LAG(s.B_39, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_39_delta
    FROM
        subset s
),
B_39_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_39_delta
    FROM
        B_39_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_39_delta_per_day AS
(
    SELECT
        customer_ID,
        B_39_delta / date_delta AS B_39_delta_per_day
    FROM
        B_39_delta
),
B_39_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_39_delta_per_day) AS B_39_delta_pd
    FROM
        B_39_delta_per_day
    GROUP BY
        customer_ID
),      
B_39_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_39_delta) AS B_39_delta_mean,
        MAX(B_39_delta) AS B_39_delta_max,
        MIN(B_39_delta) AS B_39_delta_min
    FROM
        B_39_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_39) AS B_39_mean,
        MIN(B_39) AS B_39_min, 
        MAX(B_39) AS B_39_max, 
        SUM(B_39) AS B_39_sum,
        COUNT(B_39) AS B_39_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_39_mean,
        a.B_39_min, 
        a.B_39_max, 
        a.B_39_sum,
        a.B_39_max - a.B_39_min AS B_39_range,
        a.B_39_count,
        f.B_39_first,
        l.B_39_last,
        d.B_39_delta_mean,
        d.B_39_delta_max,
        d.B_39_delta_min,
        pd.B_39_delta_pd,
        cs.B_39_span
    FROM
        aggs a
        LEFT JOIN first_B_39 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_39 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_39_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_39_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_39_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_39_mean, 
    v.B_39_min,
    v.B_39_max, 
    v.B_39_range,
    v.B_39_sum,
    ISNULL(v.B_39_count, 0) AS B_39_count,
    v.B_39_first, 
    v.B_39_last,
    v.B_39_delta_mean,
    v.B_39_delta_max,
    v.B_39_delta_min,
    v.B_39_delta_pd,
    v.B_39_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;