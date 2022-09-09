
CREATE VIEW train_data_B_16_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_16 
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
    WHERE B_16 IS NOT NULL
    GROUP BY customer_ID
),
first_B_16 AS
(
    SELECT
        f.customer_ID, s.B_16 AS B_16_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_16 AS
(
    SELECT
        f.customer_ID, s.B_16 AS B_16_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_16_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_16_span
    FROM
        first_last
),
B_16_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_16,
        s.B_16 - LAG(s.B_16, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_16_delta
    FROM
        subset s
),
B_16_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_16_delta
    FROM
        B_16_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_16_delta_per_day AS
(
    SELECT
        customer_ID,
        B_16_delta / date_delta AS B_16_delta_per_day
    FROM
        B_16_delta
),
B_16_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_16_delta_per_day) AS B_16_delta_pd
    FROM
        B_16_delta_per_day
    GROUP BY
        customer_ID
),      
B_16_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_16_delta) AS B_16_delta_mean,
        MAX(B_16_delta) AS B_16_delta_max,
        MIN(B_16_delta) AS B_16_delta_min
    FROM
        B_16_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_16) AS B_16_mean,
        MIN(B_16) AS B_16_min, 
        MAX(B_16) AS B_16_max, 
        SUM(B_16) AS B_16_sum,
        COUNT(B_16) AS B_16_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_16_mean,
        a.B_16_min, 
        a.B_16_max, 
        a.B_16_sum,
        a.B_16_max - a.B_16_min AS B_16_range,
        a.B_16_count,
        f.B_16_first,
        l.B_16_last,
        d.B_16_delta_mean,
        d.B_16_delta_max,
        d.B_16_delta_min,
        pd.B_16_delta_pd,
        cs.B_16_span
    FROM
        aggs a
        LEFT JOIN first_B_16 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_16 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_16_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_16_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_16_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_16_mean, 
    v.B_16_min,
    v.B_16_max, 
    v.B_16_range,
    v.B_16_sum,
    ISNULL(v.B_16_count, 0) AS B_16_count,
    v.B_16_first, 
    v.B_16_last,
    v.B_16_delta_mean,
    v.B_16_delta_max,
    v.B_16_delta_min,
    v.B_16_delta_pd,
    v.B_16_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;