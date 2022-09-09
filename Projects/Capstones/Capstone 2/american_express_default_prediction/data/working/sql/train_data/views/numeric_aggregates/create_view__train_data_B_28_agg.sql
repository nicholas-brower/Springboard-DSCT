
CREATE VIEW train_data_B_28_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_28 
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
    WHERE B_28 IS NOT NULL
    GROUP BY customer_ID
),
first_B_28 AS
(
    SELECT
        f.customer_ID, s.B_28 AS B_28_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_28 AS
(
    SELECT
        f.customer_ID, s.B_28 AS B_28_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_28_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_28_span
    FROM
        first_last
),
B_28_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_28,
        s.B_28 - LAG(s.B_28, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_28_delta
    FROM
        subset s
),
B_28_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_28_delta
    FROM
        B_28_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_28_delta_per_day AS
(
    SELECT
        customer_ID,
        B_28_delta / date_delta AS B_28_delta_per_day
    FROM
        B_28_delta
),
B_28_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_28_delta_per_day) AS B_28_delta_pd
    FROM
        B_28_delta_per_day
    GROUP BY
        customer_ID
),      
B_28_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_28_delta) AS B_28_delta_mean,
        MAX(B_28_delta) AS B_28_delta_max,
        MIN(B_28_delta) AS B_28_delta_min
    FROM
        B_28_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_28) AS B_28_mean,
        MIN(B_28) AS B_28_min, 
        MAX(B_28) AS B_28_max, 
        SUM(B_28) AS B_28_sum,
        COUNT(B_28) AS B_28_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_28_mean,
        a.B_28_min, 
        a.B_28_max, 
        a.B_28_sum,
        a.B_28_max - a.B_28_min AS B_28_range,
        a.B_28_count,
        f.B_28_first,
        l.B_28_last,
        d.B_28_delta_mean,
        d.B_28_delta_max,
        d.B_28_delta_min,
        pd.B_28_delta_pd,
        cs.B_28_span
    FROM
        aggs a
        LEFT JOIN first_B_28 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_28 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_28_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_28_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_28_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_28_mean, 
    v.B_28_min,
    v.B_28_max, 
    v.B_28_range,
    v.B_28_sum,
    ISNULL(v.B_28_count, 0) AS B_28_count,
    v.B_28_first, 
    v.B_28_last,
    v.B_28_delta_mean,
    v.B_28_delta_max,
    v.B_28_delta_min,
    v.B_28_delta_pd,
    v.B_28_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;