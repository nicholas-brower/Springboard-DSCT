
CREATE VIEW train_data_B_8_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_8 
    FROM
        train_data td 
        
),
first_last AS
(
    SELECT 
        customer_ID, 
        MIN(S_2) AS first_dt, 
        MAX(S_2) AS last_dt
    FROM subset
    WHERE B_8 IS NOT NULL
    GROUP BY customer_ID
),
first_B_8 AS
(
    SELECT
        f.customer_ID, s.B_8 AS B_8_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_8 AS
(
    SELECT
        f.customer_ID, s.B_8 AS B_8_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_8_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_8_span
    FROM
        first_last
),
B_8_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_8,
        s.B_8 - LAG(s.B_8, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_8_delta
    FROM
        subset s
),
B_8_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_8_delta
    FROM
        B_8_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_8_delta_per_day AS
(
    SELECT
        customer_ID,
        B_8_delta / date_delta AS B_8_delta_per_day
    FROM
        B_8_delta
),
B_8_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_8_delta_per_day) AS B_8_delta_pd
    FROM
        B_8_delta_per_day
    GROUP BY
        customer_ID
),      
B_8_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_8_delta) AS B_8_delta_mean,
        MAX(B_8_delta) AS B_8_delta_max,
        MIN(B_8_delta) AS B_8_delta_min
    FROM
        B_8_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_8) AS B_8_mean,
        MIN(B_8) AS B_8_min, 
        MAX(B_8) AS B_8_max, 
        SUM(B_8) AS B_8_sum,
        COUNT(B_8) AS B_8_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_8_mean,
        a.B_8_min, 
        a.B_8_max, 
        a.B_8_sum,
        a.B_8_max - a.B_8_min AS B_8_range,
        a.B_8_count,
        f.B_8_first,
        l.B_8_last,
        d.B_8_delta_mean,
        d.B_8_delta_max,
        d.B_8_delta_min,
        pd.B_8_delta_pd,
        cs.B_8_span
    FROM
        aggs a
        LEFT JOIN first_B_8 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_8 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_8_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_8_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_8_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_8_mean, 
    v.B_8_min,
    v.B_8_max, 
    v.B_8_range,
    v.B_8_sum,
    ISNULL(v.B_8_count, 0) AS B_8_count,
    v.B_8_first, 
    v.B_8_last,
    v.B_8_delta_mean,
    v.B_8_delta_max,
    v.B_8_delta_min,
    v.B_8_delta_pd,
    v.B_8_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;