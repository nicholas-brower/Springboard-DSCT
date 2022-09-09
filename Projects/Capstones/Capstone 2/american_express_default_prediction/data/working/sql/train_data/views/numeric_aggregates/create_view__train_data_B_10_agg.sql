
CREATE VIEW train_data_B_10_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_10 
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
    WHERE B_10 IS NOT NULL
    GROUP BY customer_ID
),
first_B_10 AS
(
    SELECT
        f.customer_ID, s.B_10 AS B_10_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_10 AS
(
    SELECT
        f.customer_ID, s.B_10 AS B_10_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_10_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_10_span
    FROM
        first_last
),
B_10_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_10,
        s.B_10 - LAG(s.B_10, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_10_delta
    FROM
        subset s
),
B_10_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_10_delta
    FROM
        B_10_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_10_delta_per_day AS
(
    SELECT
        customer_ID,
        B_10_delta / date_delta AS B_10_delta_per_day
    FROM
        B_10_delta
),
B_10_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_10_delta_per_day) AS B_10_delta_pd
    FROM
        B_10_delta_per_day
    GROUP BY
        customer_ID
),      
B_10_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_10_delta) AS B_10_delta_mean,
        MAX(B_10_delta) AS B_10_delta_max,
        MIN(B_10_delta) AS B_10_delta_min
    FROM
        B_10_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_10) AS B_10_mean,
        MIN(B_10) AS B_10_min, 
        MAX(B_10) AS B_10_max, 
        SUM(B_10) AS B_10_sum,
        COUNT(B_10) AS B_10_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_10_mean,
        a.B_10_min, 
        a.B_10_max, 
        a.B_10_sum,
        a.B_10_max - a.B_10_min AS B_10_range,
        a.B_10_count,
        f.B_10_first,
        l.B_10_last,
        d.B_10_delta_mean,
        d.B_10_delta_max,
        d.B_10_delta_min,
        pd.B_10_delta_pd,
        cs.B_10_span
    FROM
        aggs a
        LEFT JOIN first_B_10 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_10 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_10_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_10_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_10_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_10_mean, 
    v.B_10_min,
    v.B_10_max, 
    v.B_10_range,
    v.B_10_sum,
    ISNULL(v.B_10_count, 0) AS B_10_count,
    v.B_10_first, 
    v.B_10_last,
    v.B_10_delta_mean,
    v.B_10_delta_max,
    v.B_10_delta_min,
    v.B_10_delta_pd,
    v.B_10_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;