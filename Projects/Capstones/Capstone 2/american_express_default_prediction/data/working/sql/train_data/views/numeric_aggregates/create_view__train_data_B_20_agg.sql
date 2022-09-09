
CREATE VIEW train_data_B_20_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_20 
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
    WHERE B_20 IS NOT NULL
    GROUP BY customer_ID
),
first_B_20 AS
(
    SELECT
        f.customer_ID, s.B_20 AS B_20_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_20 AS
(
    SELECT
        f.customer_ID, s.B_20 AS B_20_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_20_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_20_span
    FROM
        first_last
),
B_20_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_20,
        s.B_20 - LAG(s.B_20, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_20_delta
    FROM
        subset s
),
B_20_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_20_delta
    FROM
        B_20_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_20_delta_per_day AS
(
    SELECT
        customer_ID,
        B_20_delta / date_delta AS B_20_delta_per_day
    FROM
        B_20_delta
),
B_20_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_20_delta_per_day) AS B_20_delta_pd
    FROM
        B_20_delta_per_day
    GROUP BY
        customer_ID
),      
B_20_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_20_delta) AS B_20_delta_mean,
        MAX(B_20_delta) AS B_20_delta_max,
        MIN(B_20_delta) AS B_20_delta_min
    FROM
        B_20_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_20) AS B_20_mean,
        MIN(B_20) AS B_20_min, 
        MAX(B_20) AS B_20_max, 
        SUM(B_20) AS B_20_sum,
        COUNT(B_20) AS B_20_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_20_mean,
        a.B_20_min, 
        a.B_20_max, 
        a.B_20_sum,
        a.B_20_max - a.B_20_min AS B_20_range,
        a.B_20_count,
        f.B_20_first,
        l.B_20_last,
        d.B_20_delta_mean,
        d.B_20_delta_max,
        d.B_20_delta_min,
        pd.B_20_delta_pd,
        cs.B_20_span
    FROM
        aggs a
        LEFT JOIN first_B_20 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_20 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_20_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_20_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_20_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_20_mean, 
    v.B_20_min,
    v.B_20_max, 
    v.B_20_range,
    v.B_20_sum,
    ISNULL(v.B_20_count, 0) AS B_20_count,
    v.B_20_first, 
    v.B_20_last,
    v.B_20_delta_mean,
    v.B_20_delta_max,
    v.B_20_delta_min,
    v.B_20_delta_pd,
    v.B_20_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;