
CREATE VIEW test_data_D_129_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_129 
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
    WHERE D_129 IS NOT NULL
    GROUP BY customer_ID
),
first_D_129 AS
(
    SELECT
        f.customer_ID, s.D_129 AS D_129_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_129 AS
(
    SELECT
        f.customer_ID, s.D_129 AS D_129_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_129_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_129_span
    FROM
        first_last
),
D_129_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_129,
        s.D_129 - LAG(s.D_129, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_129_delta
    FROM
        subset s
),
D_129_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_129_delta
    FROM
        D_129_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_129_delta_per_day AS
(
    SELECT
        customer_ID,
        D_129_delta / date_delta AS D_129_delta_per_day
    FROM
        D_129_delta
),
D_129_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_129_delta_per_day) AS D_129_delta_pd
    FROM
        D_129_delta_per_day
    GROUP BY
        customer_ID
),      
D_129_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_129_delta) AS D_129_delta_mean,
        MAX(D_129_delta) AS D_129_delta_max,
        MIN(D_129_delta) AS D_129_delta_min
    FROM
        D_129_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_129) AS D_129_mean,
        MIN(D_129) AS D_129_min, 
        MAX(D_129) AS D_129_max, 
        SUM(D_129) AS D_129_sum,
        COUNT(D_129) AS D_129_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_129_mean,
        a.D_129_min, 
        a.D_129_max, 
        a.D_129_sum,
        a.D_129_max - a.D_129_min AS D_129_range,
        a.D_129_count,
        f.D_129_first,
        l.D_129_last,
        d.D_129_delta_mean,
        d.D_129_delta_max,
        d.D_129_delta_min,
        pd.D_129_delta_pd,
        cs.D_129_span
    FROM
        aggs a
        LEFT JOIN first_D_129 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_129 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_129_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_129_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_129_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_129_mean, 
    v.D_129_min,
    v.D_129_max, 
    v.D_129_range,
    v.D_129_sum,
    ISNULL(v.D_129_count, 0) AS D_129_count,
    v.D_129_first, 
    v.D_129_last,
    v.D_129_delta_mean,
    v.D_129_delta_max,
    v.D_129_delta_min,
    v.D_129_delta_pd,
    v.D_129_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;