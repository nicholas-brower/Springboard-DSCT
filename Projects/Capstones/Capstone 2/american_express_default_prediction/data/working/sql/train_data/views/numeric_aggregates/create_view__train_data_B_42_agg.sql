
CREATE VIEW train_data_B_42_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_42 
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
    WHERE B_42 IS NOT NULL
    GROUP BY customer_ID
),
first_B_42 AS
(
    SELECT
        f.customer_ID, s.B_42 AS B_42_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_42 AS
(
    SELECT
        f.customer_ID, s.B_42 AS B_42_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_42_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_42_span
    FROM
        first_last
),
B_42_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_42,
        s.B_42 - LAG(s.B_42, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_42_delta
    FROM
        subset s
),
B_42_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_42_delta
    FROM
        B_42_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_42_delta_per_day AS
(
    SELECT
        customer_ID,
        B_42_delta / date_delta AS B_42_delta_per_day
    FROM
        B_42_delta
),
B_42_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_42_delta_per_day) AS B_42_delta_pd
    FROM
        B_42_delta_per_day
    GROUP BY
        customer_ID
),      
B_42_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_42_delta) AS B_42_delta_mean,
        MAX(B_42_delta) AS B_42_delta_max,
        MIN(B_42_delta) AS B_42_delta_min
    FROM
        B_42_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_42) AS B_42_mean,
        MIN(B_42) AS B_42_min, 
        MAX(B_42) AS B_42_max, 
        SUM(B_42) AS B_42_sum,
        COUNT(B_42) AS B_42_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_42_mean,
        a.B_42_min, 
        a.B_42_max, 
        a.B_42_sum,
        a.B_42_max - a.B_42_min AS B_42_range,
        a.B_42_count,
        f.B_42_first,
        l.B_42_last,
        d.B_42_delta_mean,
        d.B_42_delta_max,
        d.B_42_delta_min,
        pd.B_42_delta_pd,
        cs.B_42_span
    FROM
        aggs a
        LEFT JOIN first_B_42 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_42 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_42_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_42_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_42_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_42_mean, 
    v.B_42_min,
    v.B_42_max, 
    v.B_42_range,
    v.B_42_sum,
    ISNULL(v.B_42_count, 0) AS B_42_count,
    v.B_42_first, 
    v.B_42_last,
    v.B_42_delta_mean,
    v.B_42_delta_max,
    v.B_42_delta_min,
    v.B_42_delta_pd,
    v.B_42_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;