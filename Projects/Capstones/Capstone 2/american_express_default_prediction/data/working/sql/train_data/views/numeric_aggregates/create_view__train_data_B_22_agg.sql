
CREATE VIEW train_data_B_22_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_22 
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
    WHERE B_22 IS NOT NULL
    GROUP BY customer_ID
),
first_B_22 AS
(
    SELECT
        f.customer_ID, s.B_22 AS B_22_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_22 AS
(
    SELECT
        f.customer_ID, s.B_22 AS B_22_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_22_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_22_span
    FROM
        first_last
),
B_22_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_22,
        s.B_22 - LAG(s.B_22, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_22_delta
    FROM
        subset s
),
B_22_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_22_delta
    FROM
        B_22_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_22_delta_per_day AS
(
    SELECT
        customer_ID,
        B_22_delta / date_delta AS B_22_delta_per_day
    FROM
        B_22_delta
),
B_22_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_22_delta_per_day) AS B_22_delta_pd
    FROM
        B_22_delta_per_day
    GROUP BY
        customer_ID
),      
B_22_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_22_delta) AS B_22_delta_mean,
        MAX(B_22_delta) AS B_22_delta_max,
        MIN(B_22_delta) AS B_22_delta_min
    FROM
        B_22_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_22) AS B_22_mean,
        MIN(B_22) AS B_22_min, 
        MAX(B_22) AS B_22_max, 
        SUM(B_22) AS B_22_sum,
        COUNT(B_22) AS B_22_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_22_mean,
        a.B_22_min, 
        a.B_22_max, 
        a.B_22_sum,
        a.B_22_max - a.B_22_min AS B_22_range,
        a.B_22_count,
        f.B_22_first,
        l.B_22_last,
        d.B_22_delta_mean,
        d.B_22_delta_max,
        d.B_22_delta_min,
        pd.B_22_delta_pd,
        cs.B_22_span
    FROM
        aggs a
        LEFT JOIN first_B_22 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_22 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_22_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_22_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_22_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_22_mean, 
    v.B_22_min,
    v.B_22_max, 
    v.B_22_range,
    v.B_22_sum,
    ISNULL(v.B_22_count, 0) AS B_22_count,
    v.B_22_first, 
    v.B_22_last,
    v.B_22_delta_mean,
    v.B_22_delta_max,
    v.B_22_delta_min,
    v.B_22_delta_pd,
    v.B_22_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;