
CREATE VIEW train_data_B_33_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_33 
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
    WHERE B_33 IS NOT NULL
    GROUP BY customer_ID
),
first_B_33 AS
(
    SELECT
        f.customer_ID, s.B_33 AS B_33_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_33 AS
(
    SELECT
        f.customer_ID, s.B_33 AS B_33_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_33_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_33_span
    FROM
        first_last
),
B_33_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_33,
        s.B_33 - LAG(s.B_33, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_33_delta
    FROM
        subset s
),
B_33_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_33_delta
    FROM
        B_33_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_33_delta_per_day AS
(
    SELECT
        customer_ID,
        B_33_delta / date_delta AS B_33_delta_per_day
    FROM
        B_33_delta
),
B_33_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_33_delta_per_day) AS B_33_delta_pd
    FROM
        B_33_delta_per_day
    GROUP BY
        customer_ID
),      
B_33_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_33_delta) AS B_33_delta_mean,
        MAX(B_33_delta) AS B_33_delta_max,
        MIN(B_33_delta) AS B_33_delta_min
    FROM
        B_33_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_33) AS B_33_mean,
        MIN(B_33) AS B_33_min, 
        MAX(B_33) AS B_33_max, 
        SUM(B_33) AS B_33_sum,
        COUNT(B_33) AS B_33_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_33_mean,
        a.B_33_min, 
        a.B_33_max, 
        a.B_33_sum,
        a.B_33_max - a.B_33_min AS B_33_range,
        a.B_33_count,
        f.B_33_first,
        l.B_33_last,
        d.B_33_delta_mean,
        d.B_33_delta_max,
        d.B_33_delta_min,
        pd.B_33_delta_pd,
        cs.B_33_span
    FROM
        aggs a
        LEFT JOIN first_B_33 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_33 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_33_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_33_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_33_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_33_mean, 
    v.B_33_min,
    v.B_33_max, 
    v.B_33_range,
    v.B_33_sum,
    ISNULL(v.B_33_count, 0) AS B_33_count,
    v.B_33_first, 
    v.B_33_last,
    v.B_33_delta_mean,
    v.B_33_delta_max,
    v.B_33_delta_min,
    v.B_33_delta_pd,
    v.B_33_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;