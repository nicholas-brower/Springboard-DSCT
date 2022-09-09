
CREATE VIEW train_data_B_17_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_17 
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
    WHERE B_17 IS NOT NULL
    GROUP BY customer_ID
),
first_B_17 AS
(
    SELECT
        f.customer_ID, s.B_17 AS B_17_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_17 AS
(
    SELECT
        f.customer_ID, s.B_17 AS B_17_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_17_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_17_span
    FROM
        first_last
),
B_17_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_17,
        s.B_17 - LAG(s.B_17, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_17_delta
    FROM
        subset s
),
B_17_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_17_delta
    FROM
        B_17_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_17_delta_per_day AS
(
    SELECT
        customer_ID,
        B_17_delta / date_delta AS B_17_delta_per_day
    FROM
        B_17_delta
),
B_17_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_17_delta_per_day) AS B_17_delta_pd
    FROM
        B_17_delta_per_day
    GROUP BY
        customer_ID
),      
B_17_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_17_delta) AS B_17_delta_mean,
        MAX(B_17_delta) AS B_17_delta_max,
        MIN(B_17_delta) AS B_17_delta_min
    FROM
        B_17_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_17) AS B_17_mean,
        MIN(B_17) AS B_17_min, 
        MAX(B_17) AS B_17_max, 
        SUM(B_17) AS B_17_sum,
        COUNT(B_17) AS B_17_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_17_mean,
        a.B_17_min, 
        a.B_17_max, 
        a.B_17_sum,
        a.B_17_max - a.B_17_min AS B_17_range,
        a.B_17_count,
        f.B_17_first,
        l.B_17_last,
        d.B_17_delta_mean,
        d.B_17_delta_max,
        d.B_17_delta_min,
        pd.B_17_delta_pd,
        cs.B_17_span
    FROM
        aggs a
        LEFT JOIN first_B_17 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_17 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_17_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_17_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_17_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_17_mean, 
    v.B_17_min,
    v.B_17_max, 
    v.B_17_range,
    v.B_17_sum,
    ISNULL(v.B_17_count, 0) AS B_17_count,
    v.B_17_first, 
    v.B_17_last,
    v.B_17_delta_mean,
    v.B_17_delta_max,
    v.B_17_delta_min,
    v.B_17_delta_pd,
    v.B_17_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;