
GO

CREATE VIEW test_data_P_2_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.P_2 
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
    WHERE P_2 IS NOT NULL
    GROUP BY customer_ID
),
first_P_2 AS
(
    SELECT
        f.customer_ID, s.P_2 AS P_2_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_P_2 AS
(
    SELECT
        f.customer_ID, s.P_2 AS P_2_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
P_2_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS P_2_span
    FROM
        first_last
),
P_2_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.P_2,
        s.P_2 - LAG(s.P_2, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS P_2_delta
    FROM
        subset s
),
P_2_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.P_2_delta
    FROM
        P_2_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
P_2_delta_per_day AS
(
    SELECT
        customer_ID,
        P_2_delta / date_delta AS P_2_delta_per_day
    FROM
        P_2_delta
),
P_2_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(P_2_delta_per_day) AS P_2_delta_pd
    FROM
        P_2_delta_per_day
    GROUP BY
        customer_ID
),      
P_2_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(P_2_delta) AS P_2_delta_mean,
        MAX(P_2_delta) AS P_2_delta_max,
        MIN(P_2_delta) AS P_2_delta_min
    FROM
        P_2_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(P_2) AS P_2_mean,
        MIN(P_2) AS P_2_min, 
        MAX(P_2) AS P_2_max, 
        SUM(P_2) AS P_2_sum,
        COUNT(P_2) AS P_2_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.P_2_mean,
        a.P_2_min, 
        a.P_2_max, 
        a.P_2_sum,
        a.P_2_max - a.P_2_min AS P_2_range,
        a.P_2_count,
        f.P_2_first,
        l.P_2_last,
        d.P_2_delta_mean,
        d.P_2_delta_max,
        d.P_2_delta_min,
        pd.P_2_delta_pd,
        cs.P_2_span
    FROM
        aggs a
        LEFT JOIN first_P_2 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_P_2 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN P_2_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN P_2_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN P_2_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.P_2_mean, 
    v.P_2_min,
    v.P_2_max, 
    v.P_2_range,
    v.P_2_sum,
    ISNULL(v.P_2_count, 0) AS P_2_count,
    v.P_2_first, 
    v.P_2_last,
    v.P_2_delta_mean,
    v.P_2_delta_max,
    v.P_2_delta_min,
    v.P_2_delta_pd,
    v.P_2_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_39_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_39 
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
    WHERE D_39 IS NOT NULL
    GROUP BY customer_ID
),
first_D_39 AS
(
    SELECT
        f.customer_ID, s.D_39 AS D_39_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_39 AS
(
    SELECT
        f.customer_ID, s.D_39 AS D_39_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_39_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_39_span
    FROM
        first_last
),
D_39_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_39,
        s.D_39 - LAG(s.D_39, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_39_delta
    FROM
        subset s
),
D_39_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_39_delta
    FROM
        D_39_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_39_delta_per_day AS
(
    SELECT
        customer_ID,
        D_39_delta / date_delta AS D_39_delta_per_day
    FROM
        D_39_delta
),
D_39_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_39_delta_per_day) AS D_39_delta_pd
    FROM
        D_39_delta_per_day
    GROUP BY
        customer_ID
),      
D_39_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_39_delta) AS D_39_delta_mean,
        MAX(D_39_delta) AS D_39_delta_max,
        MIN(D_39_delta) AS D_39_delta_min
    FROM
        D_39_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_39) AS D_39_mean,
        MIN(D_39) AS D_39_min, 
        MAX(D_39) AS D_39_max, 
        SUM(D_39) AS D_39_sum,
        COUNT(D_39) AS D_39_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_39_mean,
        a.D_39_min, 
        a.D_39_max, 
        a.D_39_sum,
        a.D_39_max - a.D_39_min AS D_39_range,
        a.D_39_count,
        f.D_39_first,
        l.D_39_last,
        d.D_39_delta_mean,
        d.D_39_delta_max,
        d.D_39_delta_min,
        pd.D_39_delta_pd,
        cs.D_39_span
    FROM
        aggs a
        LEFT JOIN first_D_39 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_39 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_39_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_39_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_39_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_39_mean, 
    v.D_39_min,
    v.D_39_max, 
    v.D_39_range,
    v.D_39_sum,
    ISNULL(v.D_39_count, 0) AS D_39_count,
    v.D_39_first, 
    v.D_39_last,
    v.D_39_delta_mean,
    v.D_39_delta_max,
    v.D_39_delta_min,
    v.D_39_delta_pd,
    v.D_39_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_1_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_1 
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
    WHERE B_1 IS NOT NULL
    GROUP BY customer_ID
),
first_B_1 AS
(
    SELECT
        f.customer_ID, s.B_1 AS B_1_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_1 AS
(
    SELECT
        f.customer_ID, s.B_1 AS B_1_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_1_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_1_span
    FROM
        first_last
),
B_1_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_1,
        s.B_1 - LAG(s.B_1, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_1_delta
    FROM
        subset s
),
B_1_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_1_delta
    FROM
        B_1_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_1_delta_per_day AS
(
    SELECT
        customer_ID,
        B_1_delta / date_delta AS B_1_delta_per_day
    FROM
        B_1_delta
),
B_1_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_1_delta_per_day) AS B_1_delta_pd
    FROM
        B_1_delta_per_day
    GROUP BY
        customer_ID
),      
B_1_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_1_delta) AS B_1_delta_mean,
        MAX(B_1_delta) AS B_1_delta_max,
        MIN(B_1_delta) AS B_1_delta_min
    FROM
        B_1_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_1) AS B_1_mean,
        MIN(B_1) AS B_1_min, 
        MAX(B_1) AS B_1_max, 
        SUM(B_1) AS B_1_sum,
        COUNT(B_1) AS B_1_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_1_mean,
        a.B_1_min, 
        a.B_1_max, 
        a.B_1_sum,
        a.B_1_max - a.B_1_min AS B_1_range,
        a.B_1_count,
        f.B_1_first,
        l.B_1_last,
        d.B_1_delta_mean,
        d.B_1_delta_max,
        d.B_1_delta_min,
        pd.B_1_delta_pd,
        cs.B_1_span
    FROM
        aggs a
        LEFT JOIN first_B_1 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_1 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_1_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_1_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_1_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_1_mean, 
    v.B_1_min,
    v.B_1_max, 
    v.B_1_range,
    v.B_1_sum,
    ISNULL(v.B_1_count, 0) AS B_1_count,
    v.B_1_first, 
    v.B_1_last,
    v.B_1_delta_mean,
    v.B_1_delta_max,
    v.B_1_delta_min,
    v.B_1_delta_pd,
    v.B_1_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_2_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_2 
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
    WHERE B_2 IS NOT NULL
    GROUP BY customer_ID
),
first_B_2 AS
(
    SELECT
        f.customer_ID, s.B_2 AS B_2_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_2 AS
(
    SELECT
        f.customer_ID, s.B_2 AS B_2_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_2_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_2_span
    FROM
        first_last
),
B_2_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_2,
        s.B_2 - LAG(s.B_2, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_2_delta
    FROM
        subset s
),
B_2_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_2_delta
    FROM
        B_2_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_2_delta_per_day AS
(
    SELECT
        customer_ID,
        B_2_delta / date_delta AS B_2_delta_per_day
    FROM
        B_2_delta
),
B_2_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_2_delta_per_day) AS B_2_delta_pd
    FROM
        B_2_delta_per_day
    GROUP BY
        customer_ID
),      
B_2_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_2_delta) AS B_2_delta_mean,
        MAX(B_2_delta) AS B_2_delta_max,
        MIN(B_2_delta) AS B_2_delta_min
    FROM
        B_2_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_2) AS B_2_mean,
        MIN(B_2) AS B_2_min, 
        MAX(B_2) AS B_2_max, 
        SUM(B_2) AS B_2_sum,
        COUNT(B_2) AS B_2_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_2_mean,
        a.B_2_min, 
        a.B_2_max, 
        a.B_2_sum,
        a.B_2_max - a.B_2_min AS B_2_range,
        a.B_2_count,
        f.B_2_first,
        l.B_2_last,
        d.B_2_delta_mean,
        d.B_2_delta_max,
        d.B_2_delta_min,
        pd.B_2_delta_pd,
        cs.B_2_span
    FROM
        aggs a
        LEFT JOIN first_B_2 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_2 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_2_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_2_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_2_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_2_mean, 
    v.B_2_min,
    v.B_2_max, 
    v.B_2_range,
    v.B_2_sum,
    ISNULL(v.B_2_count, 0) AS B_2_count,
    v.B_2_first, 
    v.B_2_last,
    v.B_2_delta_mean,
    v.B_2_delta_max,
    v.B_2_delta_min,
    v.B_2_delta_pd,
    v.B_2_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_1_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_1 
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
    WHERE R_1 IS NOT NULL
    GROUP BY customer_ID
),
first_R_1 AS
(
    SELECT
        f.customer_ID, s.R_1 AS R_1_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_1 AS
(
    SELECT
        f.customer_ID, s.R_1 AS R_1_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_1_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_1_span
    FROM
        first_last
),
R_1_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_1,
        s.R_1 - LAG(s.R_1, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_1_delta
    FROM
        subset s
),
R_1_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_1_delta
    FROM
        R_1_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_1_delta_per_day AS
(
    SELECT
        customer_ID,
        R_1_delta / date_delta AS R_1_delta_per_day
    FROM
        R_1_delta
),
R_1_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_1_delta_per_day) AS R_1_delta_pd
    FROM
        R_1_delta_per_day
    GROUP BY
        customer_ID
),      
R_1_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_1_delta) AS R_1_delta_mean,
        MAX(R_1_delta) AS R_1_delta_max,
        MIN(R_1_delta) AS R_1_delta_min
    FROM
        R_1_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_1) AS R_1_mean,
        MIN(R_1) AS R_1_min, 
        MAX(R_1) AS R_1_max, 
        SUM(R_1) AS R_1_sum,
        COUNT(R_1) AS R_1_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_1_mean,
        a.R_1_min, 
        a.R_1_max, 
        a.R_1_sum,
        a.R_1_max - a.R_1_min AS R_1_range,
        a.R_1_count,
        f.R_1_first,
        l.R_1_last,
        d.R_1_delta_mean,
        d.R_1_delta_max,
        d.R_1_delta_min,
        pd.R_1_delta_pd,
        cs.R_1_span
    FROM
        aggs a
        LEFT JOIN first_R_1 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_1 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_1_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_1_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_1_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_1_mean, 
    v.R_1_min,
    v.R_1_max, 
    v.R_1_range,
    v.R_1_sum,
    ISNULL(v.R_1_count, 0) AS R_1_count,
    v.R_1_first, 
    v.R_1_last,
    v.R_1_delta_mean,
    v.R_1_delta_max,
    v.R_1_delta_min,
    v.R_1_delta_pd,
    v.R_1_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_3_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_3 
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
    WHERE S_3 IS NOT NULL
    GROUP BY customer_ID
),
first_S_3 AS
(
    SELECT
        f.customer_ID, s.S_3 AS S_3_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_3 AS
(
    SELECT
        f.customer_ID, s.S_3 AS S_3_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_3_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_3_span
    FROM
        first_last
),
S_3_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_3,
        s.S_3 - LAG(s.S_3, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_3_delta
    FROM
        subset s
),
S_3_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_3_delta
    FROM
        S_3_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_3_delta_per_day AS
(
    SELECT
        customer_ID,
        S_3_delta / date_delta AS S_3_delta_per_day
    FROM
        S_3_delta
),
S_3_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_3_delta_per_day) AS S_3_delta_pd
    FROM
        S_3_delta_per_day
    GROUP BY
        customer_ID
),      
S_3_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_3_delta) AS S_3_delta_mean,
        MAX(S_3_delta) AS S_3_delta_max,
        MIN(S_3_delta) AS S_3_delta_min
    FROM
        S_3_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_3) AS S_3_mean,
        MIN(S_3) AS S_3_min, 
        MAX(S_3) AS S_3_max, 
        SUM(S_3) AS S_3_sum,
        COUNT(S_3) AS S_3_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_3_mean,
        a.S_3_min, 
        a.S_3_max, 
        a.S_3_sum,
        a.S_3_max - a.S_3_min AS S_3_range,
        a.S_3_count,
        f.S_3_first,
        l.S_3_last,
        d.S_3_delta_mean,
        d.S_3_delta_max,
        d.S_3_delta_min,
        pd.S_3_delta_pd,
        cs.S_3_span
    FROM
        aggs a
        LEFT JOIN first_S_3 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_3 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_3_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_3_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_3_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_3_mean, 
    v.S_3_min,
    v.S_3_max, 
    v.S_3_range,
    v.S_3_sum,
    ISNULL(v.S_3_count, 0) AS S_3_count,
    v.S_3_first, 
    v.S_3_last,
    v.S_3_delta_mean,
    v.S_3_delta_max,
    v.S_3_delta_min,
    v.S_3_delta_pd,
    v.S_3_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_41_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_41 
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
    WHERE D_41 IS NOT NULL
    GROUP BY customer_ID
),
first_D_41 AS
(
    SELECT
        f.customer_ID, s.D_41 AS D_41_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_41 AS
(
    SELECT
        f.customer_ID, s.D_41 AS D_41_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_41_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_41_span
    FROM
        first_last
),
D_41_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_41,
        s.D_41 - LAG(s.D_41, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_41_delta
    FROM
        subset s
),
D_41_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_41_delta
    FROM
        D_41_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_41_delta_per_day AS
(
    SELECT
        customer_ID,
        D_41_delta / date_delta AS D_41_delta_per_day
    FROM
        D_41_delta
),
D_41_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_41_delta_per_day) AS D_41_delta_pd
    FROM
        D_41_delta_per_day
    GROUP BY
        customer_ID
),      
D_41_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_41_delta) AS D_41_delta_mean,
        MAX(D_41_delta) AS D_41_delta_max,
        MIN(D_41_delta) AS D_41_delta_min
    FROM
        D_41_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_41) AS D_41_mean,
        MIN(D_41) AS D_41_min, 
        MAX(D_41) AS D_41_max, 
        SUM(D_41) AS D_41_sum,
        COUNT(D_41) AS D_41_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_41_mean,
        a.D_41_min, 
        a.D_41_max, 
        a.D_41_sum,
        a.D_41_max - a.D_41_min AS D_41_range,
        a.D_41_count,
        f.D_41_first,
        l.D_41_last,
        d.D_41_delta_mean,
        d.D_41_delta_max,
        d.D_41_delta_min,
        pd.D_41_delta_pd,
        cs.D_41_span
    FROM
        aggs a
        LEFT JOIN first_D_41 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_41 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_41_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_41_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_41_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_41_mean, 
    v.D_41_min,
    v.D_41_max, 
    v.D_41_range,
    v.D_41_sum,
    ISNULL(v.D_41_count, 0) AS D_41_count,
    v.D_41_first, 
    v.D_41_last,
    v.D_41_delta_mean,
    v.D_41_delta_max,
    v.D_41_delta_min,
    v.D_41_delta_pd,
    v.D_41_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_3_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_3 
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
    WHERE B_3 IS NOT NULL
    GROUP BY customer_ID
),
first_B_3 AS
(
    SELECT
        f.customer_ID, s.B_3 AS B_3_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_3 AS
(
    SELECT
        f.customer_ID, s.B_3 AS B_3_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_3_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_3_span
    FROM
        first_last
),
B_3_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_3,
        s.B_3 - LAG(s.B_3, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_3_delta
    FROM
        subset s
),
B_3_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_3_delta
    FROM
        B_3_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_3_delta_per_day AS
(
    SELECT
        customer_ID,
        B_3_delta / date_delta AS B_3_delta_per_day
    FROM
        B_3_delta
),
B_3_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_3_delta_per_day) AS B_3_delta_pd
    FROM
        B_3_delta_per_day
    GROUP BY
        customer_ID
),      
B_3_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_3_delta) AS B_3_delta_mean,
        MAX(B_3_delta) AS B_3_delta_max,
        MIN(B_3_delta) AS B_3_delta_min
    FROM
        B_3_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_3) AS B_3_mean,
        MIN(B_3) AS B_3_min, 
        MAX(B_3) AS B_3_max, 
        SUM(B_3) AS B_3_sum,
        COUNT(B_3) AS B_3_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_3_mean,
        a.B_3_min, 
        a.B_3_max, 
        a.B_3_sum,
        a.B_3_max - a.B_3_min AS B_3_range,
        a.B_3_count,
        f.B_3_first,
        l.B_3_last,
        d.B_3_delta_mean,
        d.B_3_delta_max,
        d.B_3_delta_min,
        pd.B_3_delta_pd,
        cs.B_3_span
    FROM
        aggs a
        LEFT JOIN first_B_3 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_3 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_3_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_3_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_3_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_3_mean, 
    v.B_3_min,
    v.B_3_max, 
    v.B_3_range,
    v.B_3_sum,
    ISNULL(v.B_3_count, 0) AS B_3_count,
    v.B_3_first, 
    v.B_3_last,
    v.B_3_delta_mean,
    v.B_3_delta_max,
    v.B_3_delta_min,
    v.B_3_delta_pd,
    v.B_3_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_42_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_42 
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
    WHERE D_42 IS NOT NULL
    GROUP BY customer_ID
),
first_D_42 AS
(
    SELECT
        f.customer_ID, s.D_42 AS D_42_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_42 AS
(
    SELECT
        f.customer_ID, s.D_42 AS D_42_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_42_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_42_span
    FROM
        first_last
),
D_42_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_42,
        s.D_42 - LAG(s.D_42, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_42_delta
    FROM
        subset s
),
D_42_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_42_delta
    FROM
        D_42_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_42_delta_per_day AS
(
    SELECT
        customer_ID,
        D_42_delta / date_delta AS D_42_delta_per_day
    FROM
        D_42_delta
),
D_42_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_42_delta_per_day) AS D_42_delta_pd
    FROM
        D_42_delta_per_day
    GROUP BY
        customer_ID
),      
D_42_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_42_delta) AS D_42_delta_mean,
        MAX(D_42_delta) AS D_42_delta_max,
        MIN(D_42_delta) AS D_42_delta_min
    FROM
        D_42_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_42) AS D_42_mean,
        MIN(D_42) AS D_42_min, 
        MAX(D_42) AS D_42_max, 
        SUM(D_42) AS D_42_sum,
        COUNT(D_42) AS D_42_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_42_mean,
        a.D_42_min, 
        a.D_42_max, 
        a.D_42_sum,
        a.D_42_max - a.D_42_min AS D_42_range,
        a.D_42_count,
        f.D_42_first,
        l.D_42_last,
        d.D_42_delta_mean,
        d.D_42_delta_max,
        d.D_42_delta_min,
        pd.D_42_delta_pd,
        cs.D_42_span
    FROM
        aggs a
        LEFT JOIN first_D_42 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_42 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_42_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_42_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_42_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_42_mean, 
    v.D_42_min,
    v.D_42_max, 
    v.D_42_range,
    v.D_42_sum,
    ISNULL(v.D_42_count, 0) AS D_42_count,
    v.D_42_first, 
    v.D_42_last,
    v.D_42_delta_mean,
    v.D_42_delta_max,
    v.D_42_delta_min,
    v.D_42_delta_pd,
    v.D_42_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_43_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_43 
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
    WHERE D_43 IS NOT NULL
    GROUP BY customer_ID
),
first_D_43 AS
(
    SELECT
        f.customer_ID, s.D_43 AS D_43_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_43 AS
(
    SELECT
        f.customer_ID, s.D_43 AS D_43_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_43_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_43_span
    FROM
        first_last
),
D_43_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_43,
        s.D_43 - LAG(s.D_43, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_43_delta
    FROM
        subset s
),
D_43_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_43_delta
    FROM
        D_43_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_43_delta_per_day AS
(
    SELECT
        customer_ID,
        D_43_delta / date_delta AS D_43_delta_per_day
    FROM
        D_43_delta
),
D_43_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_43_delta_per_day) AS D_43_delta_pd
    FROM
        D_43_delta_per_day
    GROUP BY
        customer_ID
),      
D_43_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_43_delta) AS D_43_delta_mean,
        MAX(D_43_delta) AS D_43_delta_max,
        MIN(D_43_delta) AS D_43_delta_min
    FROM
        D_43_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_43) AS D_43_mean,
        MIN(D_43) AS D_43_min, 
        MAX(D_43) AS D_43_max, 
        SUM(D_43) AS D_43_sum,
        COUNT(D_43) AS D_43_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_43_mean,
        a.D_43_min, 
        a.D_43_max, 
        a.D_43_sum,
        a.D_43_max - a.D_43_min AS D_43_range,
        a.D_43_count,
        f.D_43_first,
        l.D_43_last,
        d.D_43_delta_mean,
        d.D_43_delta_max,
        d.D_43_delta_min,
        pd.D_43_delta_pd,
        cs.D_43_span
    FROM
        aggs a
        LEFT JOIN first_D_43 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_43 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_43_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_43_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_43_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_43_mean, 
    v.D_43_min,
    v.D_43_max, 
    v.D_43_range,
    v.D_43_sum,
    ISNULL(v.D_43_count, 0) AS D_43_count,
    v.D_43_first, 
    v.D_43_last,
    v.D_43_delta_mean,
    v.D_43_delta_max,
    v.D_43_delta_min,
    v.D_43_delta_pd,
    v.D_43_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_44_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_44 
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
    WHERE D_44 IS NOT NULL
    GROUP BY customer_ID
),
first_D_44 AS
(
    SELECT
        f.customer_ID, s.D_44 AS D_44_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_44 AS
(
    SELECT
        f.customer_ID, s.D_44 AS D_44_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_44_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_44_span
    FROM
        first_last
),
D_44_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_44,
        s.D_44 - LAG(s.D_44, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_44_delta
    FROM
        subset s
),
D_44_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_44_delta
    FROM
        D_44_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_44_delta_per_day AS
(
    SELECT
        customer_ID,
        D_44_delta / date_delta AS D_44_delta_per_day
    FROM
        D_44_delta
),
D_44_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_44_delta_per_day) AS D_44_delta_pd
    FROM
        D_44_delta_per_day
    GROUP BY
        customer_ID
),      
D_44_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_44_delta) AS D_44_delta_mean,
        MAX(D_44_delta) AS D_44_delta_max,
        MIN(D_44_delta) AS D_44_delta_min
    FROM
        D_44_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_44) AS D_44_mean,
        MIN(D_44) AS D_44_min, 
        MAX(D_44) AS D_44_max, 
        SUM(D_44) AS D_44_sum,
        COUNT(D_44) AS D_44_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_44_mean,
        a.D_44_min, 
        a.D_44_max, 
        a.D_44_sum,
        a.D_44_max - a.D_44_min AS D_44_range,
        a.D_44_count,
        f.D_44_first,
        l.D_44_last,
        d.D_44_delta_mean,
        d.D_44_delta_max,
        d.D_44_delta_min,
        pd.D_44_delta_pd,
        cs.D_44_span
    FROM
        aggs a
        LEFT JOIN first_D_44 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_44 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_44_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_44_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_44_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_44_mean, 
    v.D_44_min,
    v.D_44_max, 
    v.D_44_range,
    v.D_44_sum,
    ISNULL(v.D_44_count, 0) AS D_44_count,
    v.D_44_first, 
    v.D_44_last,
    v.D_44_delta_mean,
    v.D_44_delta_max,
    v.D_44_delta_min,
    v.D_44_delta_pd,
    v.D_44_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_4_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_4 
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
    WHERE B_4 IS NOT NULL
    GROUP BY customer_ID
),
first_B_4 AS
(
    SELECT
        f.customer_ID, s.B_4 AS B_4_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_4 AS
(
    SELECT
        f.customer_ID, s.B_4 AS B_4_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_4_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_4_span
    FROM
        first_last
),
B_4_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_4,
        s.B_4 - LAG(s.B_4, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_4_delta
    FROM
        subset s
),
B_4_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_4_delta
    FROM
        B_4_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_4_delta_per_day AS
(
    SELECT
        customer_ID,
        B_4_delta / date_delta AS B_4_delta_per_day
    FROM
        B_4_delta
),
B_4_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_4_delta_per_day) AS B_4_delta_pd
    FROM
        B_4_delta_per_day
    GROUP BY
        customer_ID
),      
B_4_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_4_delta) AS B_4_delta_mean,
        MAX(B_4_delta) AS B_4_delta_max,
        MIN(B_4_delta) AS B_4_delta_min
    FROM
        B_4_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_4) AS B_4_mean,
        MIN(B_4) AS B_4_min, 
        MAX(B_4) AS B_4_max, 
        SUM(B_4) AS B_4_sum,
        COUNT(B_4) AS B_4_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_4_mean,
        a.B_4_min, 
        a.B_4_max, 
        a.B_4_sum,
        a.B_4_max - a.B_4_min AS B_4_range,
        a.B_4_count,
        f.B_4_first,
        l.B_4_last,
        d.B_4_delta_mean,
        d.B_4_delta_max,
        d.B_4_delta_min,
        pd.B_4_delta_pd,
        cs.B_4_span
    FROM
        aggs a
        LEFT JOIN first_B_4 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_4 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_4_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_4_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_4_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_4_mean, 
    v.B_4_min,
    v.B_4_max, 
    v.B_4_range,
    v.B_4_sum,
    ISNULL(v.B_4_count, 0) AS B_4_count,
    v.B_4_first, 
    v.B_4_last,
    v.B_4_delta_mean,
    v.B_4_delta_max,
    v.B_4_delta_min,
    v.B_4_delta_pd,
    v.B_4_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_45_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_45 
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
    WHERE D_45 IS NOT NULL
    GROUP BY customer_ID
),
first_D_45 AS
(
    SELECT
        f.customer_ID, s.D_45 AS D_45_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_45 AS
(
    SELECT
        f.customer_ID, s.D_45 AS D_45_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_45_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_45_span
    FROM
        first_last
),
D_45_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_45,
        s.D_45 - LAG(s.D_45, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_45_delta
    FROM
        subset s
),
D_45_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_45_delta
    FROM
        D_45_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_45_delta_per_day AS
(
    SELECT
        customer_ID,
        D_45_delta / date_delta AS D_45_delta_per_day
    FROM
        D_45_delta
),
D_45_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_45_delta_per_day) AS D_45_delta_pd
    FROM
        D_45_delta_per_day
    GROUP BY
        customer_ID
),      
D_45_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_45_delta) AS D_45_delta_mean,
        MAX(D_45_delta) AS D_45_delta_max,
        MIN(D_45_delta) AS D_45_delta_min
    FROM
        D_45_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_45) AS D_45_mean,
        MIN(D_45) AS D_45_min, 
        MAX(D_45) AS D_45_max, 
        SUM(D_45) AS D_45_sum,
        COUNT(D_45) AS D_45_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_45_mean,
        a.D_45_min, 
        a.D_45_max, 
        a.D_45_sum,
        a.D_45_max - a.D_45_min AS D_45_range,
        a.D_45_count,
        f.D_45_first,
        l.D_45_last,
        d.D_45_delta_mean,
        d.D_45_delta_max,
        d.D_45_delta_min,
        pd.D_45_delta_pd,
        cs.D_45_span
    FROM
        aggs a
        LEFT JOIN first_D_45 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_45 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_45_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_45_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_45_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_45_mean, 
    v.D_45_min,
    v.D_45_max, 
    v.D_45_range,
    v.D_45_sum,
    ISNULL(v.D_45_count, 0) AS D_45_count,
    v.D_45_first, 
    v.D_45_last,
    v.D_45_delta_mean,
    v.D_45_delta_max,
    v.D_45_delta_min,
    v.D_45_delta_pd,
    v.D_45_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_5_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_5 
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
    WHERE B_5 IS NOT NULL
    GROUP BY customer_ID
),
first_B_5 AS
(
    SELECT
        f.customer_ID, s.B_5 AS B_5_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_5 AS
(
    SELECT
        f.customer_ID, s.B_5 AS B_5_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_5_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_5_span
    FROM
        first_last
),
B_5_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_5,
        s.B_5 - LAG(s.B_5, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_5_delta
    FROM
        subset s
),
B_5_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_5_delta
    FROM
        B_5_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_5_delta_per_day AS
(
    SELECT
        customer_ID,
        B_5_delta / date_delta AS B_5_delta_per_day
    FROM
        B_5_delta
),
B_5_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_5_delta_per_day) AS B_5_delta_pd
    FROM
        B_5_delta_per_day
    GROUP BY
        customer_ID
),      
B_5_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_5_delta) AS B_5_delta_mean,
        MAX(B_5_delta) AS B_5_delta_max,
        MIN(B_5_delta) AS B_5_delta_min
    FROM
        B_5_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_5) AS B_5_mean,
        MIN(B_5) AS B_5_min, 
        MAX(B_5) AS B_5_max, 
        SUM(B_5) AS B_5_sum,
        COUNT(B_5) AS B_5_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_5_mean,
        a.B_5_min, 
        a.B_5_max, 
        a.B_5_sum,
        a.B_5_max - a.B_5_min AS B_5_range,
        a.B_5_count,
        f.B_5_first,
        l.B_5_last,
        d.B_5_delta_mean,
        d.B_5_delta_max,
        d.B_5_delta_min,
        pd.B_5_delta_pd,
        cs.B_5_span
    FROM
        aggs a
        LEFT JOIN first_B_5 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_5 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_5_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_5_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_5_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_5_mean, 
    v.B_5_min,
    v.B_5_max, 
    v.B_5_range,
    v.B_5_sum,
    ISNULL(v.B_5_count, 0) AS B_5_count,
    v.B_5_first, 
    v.B_5_last,
    v.B_5_delta_mean,
    v.B_5_delta_max,
    v.B_5_delta_min,
    v.B_5_delta_pd,
    v.B_5_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_2_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_2 
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
    WHERE R_2 IS NOT NULL
    GROUP BY customer_ID
),
first_R_2 AS
(
    SELECT
        f.customer_ID, s.R_2 AS R_2_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_2 AS
(
    SELECT
        f.customer_ID, s.R_2 AS R_2_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_2_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_2_span
    FROM
        first_last
),
R_2_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_2,
        s.R_2 - LAG(s.R_2, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_2_delta
    FROM
        subset s
),
R_2_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_2_delta
    FROM
        R_2_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_2_delta_per_day AS
(
    SELECT
        customer_ID,
        R_2_delta / date_delta AS R_2_delta_per_day
    FROM
        R_2_delta
),
R_2_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_2_delta_per_day) AS R_2_delta_pd
    FROM
        R_2_delta_per_day
    GROUP BY
        customer_ID
),      
R_2_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_2_delta) AS R_2_delta_mean,
        MAX(R_2_delta) AS R_2_delta_max,
        MIN(R_2_delta) AS R_2_delta_min
    FROM
        R_2_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_2) AS R_2_mean,
        MIN(R_2) AS R_2_min, 
        MAX(R_2) AS R_2_max, 
        SUM(R_2) AS R_2_sum,
        COUNT(R_2) AS R_2_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_2_mean,
        a.R_2_min, 
        a.R_2_max, 
        a.R_2_sum,
        a.R_2_max - a.R_2_min AS R_2_range,
        a.R_2_count,
        f.R_2_first,
        l.R_2_last,
        d.R_2_delta_mean,
        d.R_2_delta_max,
        d.R_2_delta_min,
        pd.R_2_delta_pd,
        cs.R_2_span
    FROM
        aggs a
        LEFT JOIN first_R_2 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_2 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_2_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_2_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_2_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_2_mean, 
    v.R_2_min,
    v.R_2_max, 
    v.R_2_range,
    v.R_2_sum,
    ISNULL(v.R_2_count, 0) AS R_2_count,
    v.R_2_first, 
    v.R_2_last,
    v.R_2_delta_mean,
    v.R_2_delta_max,
    v.R_2_delta_min,
    v.R_2_delta_pd,
    v.R_2_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_46_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_46 
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
    WHERE D_46 IS NOT NULL
    GROUP BY customer_ID
),
first_D_46 AS
(
    SELECT
        f.customer_ID, s.D_46 AS D_46_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_46 AS
(
    SELECT
        f.customer_ID, s.D_46 AS D_46_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_46_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_46_span
    FROM
        first_last
),
D_46_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_46,
        s.D_46 - LAG(s.D_46, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_46_delta
    FROM
        subset s
),
D_46_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_46_delta
    FROM
        D_46_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_46_delta_per_day AS
(
    SELECT
        customer_ID,
        D_46_delta / date_delta AS D_46_delta_per_day
    FROM
        D_46_delta
),
D_46_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_46_delta_per_day) AS D_46_delta_pd
    FROM
        D_46_delta_per_day
    GROUP BY
        customer_ID
),      
D_46_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_46_delta) AS D_46_delta_mean,
        MAX(D_46_delta) AS D_46_delta_max,
        MIN(D_46_delta) AS D_46_delta_min
    FROM
        D_46_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_46) AS D_46_mean,
        MIN(D_46) AS D_46_min, 
        MAX(D_46) AS D_46_max, 
        SUM(D_46) AS D_46_sum,
        COUNT(D_46) AS D_46_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_46_mean,
        a.D_46_min, 
        a.D_46_max, 
        a.D_46_sum,
        a.D_46_max - a.D_46_min AS D_46_range,
        a.D_46_count,
        f.D_46_first,
        l.D_46_last,
        d.D_46_delta_mean,
        d.D_46_delta_max,
        d.D_46_delta_min,
        pd.D_46_delta_pd,
        cs.D_46_span
    FROM
        aggs a
        LEFT JOIN first_D_46 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_46 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_46_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_46_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_46_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_46_mean, 
    v.D_46_min,
    v.D_46_max, 
    v.D_46_range,
    v.D_46_sum,
    ISNULL(v.D_46_count, 0) AS D_46_count,
    v.D_46_first, 
    v.D_46_last,
    v.D_46_delta_mean,
    v.D_46_delta_max,
    v.D_46_delta_min,
    v.D_46_delta_pd,
    v.D_46_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_47_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_47 
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
    WHERE D_47 IS NOT NULL
    GROUP BY customer_ID
),
first_D_47 AS
(
    SELECT
        f.customer_ID, s.D_47 AS D_47_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_47 AS
(
    SELECT
        f.customer_ID, s.D_47 AS D_47_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_47_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_47_span
    FROM
        first_last
),
D_47_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_47,
        s.D_47 - LAG(s.D_47, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_47_delta
    FROM
        subset s
),
D_47_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_47_delta
    FROM
        D_47_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_47_delta_per_day AS
(
    SELECT
        customer_ID,
        D_47_delta / date_delta AS D_47_delta_per_day
    FROM
        D_47_delta
),
D_47_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_47_delta_per_day) AS D_47_delta_pd
    FROM
        D_47_delta_per_day
    GROUP BY
        customer_ID
),      
D_47_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_47_delta) AS D_47_delta_mean,
        MAX(D_47_delta) AS D_47_delta_max,
        MIN(D_47_delta) AS D_47_delta_min
    FROM
        D_47_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_47) AS D_47_mean,
        MIN(D_47) AS D_47_min, 
        MAX(D_47) AS D_47_max, 
        SUM(D_47) AS D_47_sum,
        COUNT(D_47) AS D_47_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_47_mean,
        a.D_47_min, 
        a.D_47_max, 
        a.D_47_sum,
        a.D_47_max - a.D_47_min AS D_47_range,
        a.D_47_count,
        f.D_47_first,
        l.D_47_last,
        d.D_47_delta_mean,
        d.D_47_delta_max,
        d.D_47_delta_min,
        pd.D_47_delta_pd,
        cs.D_47_span
    FROM
        aggs a
        LEFT JOIN first_D_47 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_47 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_47_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_47_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_47_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_47_mean, 
    v.D_47_min,
    v.D_47_max, 
    v.D_47_range,
    v.D_47_sum,
    ISNULL(v.D_47_count, 0) AS D_47_count,
    v.D_47_first, 
    v.D_47_last,
    v.D_47_delta_mean,
    v.D_47_delta_max,
    v.D_47_delta_min,
    v.D_47_delta_pd,
    v.D_47_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_48_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_48 
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
    WHERE D_48 IS NOT NULL
    GROUP BY customer_ID
),
first_D_48 AS
(
    SELECT
        f.customer_ID, s.D_48 AS D_48_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_48 AS
(
    SELECT
        f.customer_ID, s.D_48 AS D_48_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_48_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_48_span
    FROM
        first_last
),
D_48_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_48,
        s.D_48 - LAG(s.D_48, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_48_delta
    FROM
        subset s
),
D_48_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_48_delta
    FROM
        D_48_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_48_delta_per_day AS
(
    SELECT
        customer_ID,
        D_48_delta / date_delta AS D_48_delta_per_day
    FROM
        D_48_delta
),
D_48_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_48_delta_per_day) AS D_48_delta_pd
    FROM
        D_48_delta_per_day
    GROUP BY
        customer_ID
),      
D_48_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_48_delta) AS D_48_delta_mean,
        MAX(D_48_delta) AS D_48_delta_max,
        MIN(D_48_delta) AS D_48_delta_min
    FROM
        D_48_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_48) AS D_48_mean,
        MIN(D_48) AS D_48_min, 
        MAX(D_48) AS D_48_max, 
        SUM(D_48) AS D_48_sum,
        COUNT(D_48) AS D_48_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_48_mean,
        a.D_48_min, 
        a.D_48_max, 
        a.D_48_sum,
        a.D_48_max - a.D_48_min AS D_48_range,
        a.D_48_count,
        f.D_48_first,
        l.D_48_last,
        d.D_48_delta_mean,
        d.D_48_delta_max,
        d.D_48_delta_min,
        pd.D_48_delta_pd,
        cs.D_48_span
    FROM
        aggs a
        LEFT JOIN first_D_48 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_48 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_48_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_48_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_48_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_48_mean, 
    v.D_48_min,
    v.D_48_max, 
    v.D_48_range,
    v.D_48_sum,
    ISNULL(v.D_48_count, 0) AS D_48_count,
    v.D_48_first, 
    v.D_48_last,
    v.D_48_delta_mean,
    v.D_48_delta_max,
    v.D_48_delta_min,
    v.D_48_delta_pd,
    v.D_48_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_49_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_49 
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
    WHERE D_49 IS NOT NULL
    GROUP BY customer_ID
),
first_D_49 AS
(
    SELECT
        f.customer_ID, s.D_49 AS D_49_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_49 AS
(
    SELECT
        f.customer_ID, s.D_49 AS D_49_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_49_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_49_span
    FROM
        first_last
),
D_49_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_49,
        s.D_49 - LAG(s.D_49, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_49_delta
    FROM
        subset s
),
D_49_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_49_delta
    FROM
        D_49_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_49_delta_per_day AS
(
    SELECT
        customer_ID,
        D_49_delta / date_delta AS D_49_delta_per_day
    FROM
        D_49_delta
),
D_49_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_49_delta_per_day) AS D_49_delta_pd
    FROM
        D_49_delta_per_day
    GROUP BY
        customer_ID
),      
D_49_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_49_delta) AS D_49_delta_mean,
        MAX(D_49_delta) AS D_49_delta_max,
        MIN(D_49_delta) AS D_49_delta_min
    FROM
        D_49_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_49) AS D_49_mean,
        MIN(D_49) AS D_49_min, 
        MAX(D_49) AS D_49_max, 
        SUM(D_49) AS D_49_sum,
        COUNT(D_49) AS D_49_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_49_mean,
        a.D_49_min, 
        a.D_49_max, 
        a.D_49_sum,
        a.D_49_max - a.D_49_min AS D_49_range,
        a.D_49_count,
        f.D_49_first,
        l.D_49_last,
        d.D_49_delta_mean,
        d.D_49_delta_max,
        d.D_49_delta_min,
        pd.D_49_delta_pd,
        cs.D_49_span
    FROM
        aggs a
        LEFT JOIN first_D_49 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_49 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_49_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_49_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_49_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_49_mean, 
    v.D_49_min,
    v.D_49_max, 
    v.D_49_range,
    v.D_49_sum,
    ISNULL(v.D_49_count, 0) AS D_49_count,
    v.D_49_first, 
    v.D_49_last,
    v.D_49_delta_mean,
    v.D_49_delta_max,
    v.D_49_delta_min,
    v.D_49_delta_pd,
    v.D_49_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_6_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_6 
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
    WHERE B_6 IS NOT NULL
    GROUP BY customer_ID
),
first_B_6 AS
(
    SELECT
        f.customer_ID, s.B_6 AS B_6_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_6 AS
(
    SELECT
        f.customer_ID, s.B_6 AS B_6_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_6_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_6_span
    FROM
        first_last
),
B_6_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_6,
        s.B_6 - LAG(s.B_6, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_6_delta
    FROM
        subset s
),
B_6_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_6_delta
    FROM
        B_6_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_6_delta_per_day AS
(
    SELECT
        customer_ID,
        B_6_delta / date_delta AS B_6_delta_per_day
    FROM
        B_6_delta
),
B_6_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_6_delta_per_day) AS B_6_delta_pd
    FROM
        B_6_delta_per_day
    GROUP BY
        customer_ID
),      
B_6_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_6_delta) AS B_6_delta_mean,
        MAX(B_6_delta) AS B_6_delta_max,
        MIN(B_6_delta) AS B_6_delta_min
    FROM
        B_6_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_6) AS B_6_mean,
        MIN(B_6) AS B_6_min, 
        MAX(B_6) AS B_6_max, 
        SUM(B_6) AS B_6_sum,
        COUNT(B_6) AS B_6_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_6_mean,
        a.B_6_min, 
        a.B_6_max, 
        a.B_6_sum,
        a.B_6_max - a.B_6_min AS B_6_range,
        a.B_6_count,
        f.B_6_first,
        l.B_6_last,
        d.B_6_delta_mean,
        d.B_6_delta_max,
        d.B_6_delta_min,
        pd.B_6_delta_pd,
        cs.B_6_span
    FROM
        aggs a
        LEFT JOIN first_B_6 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_6 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_6_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_6_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_6_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_6_mean, 
    v.B_6_min,
    v.B_6_max, 
    v.B_6_range,
    v.B_6_sum,
    ISNULL(v.B_6_count, 0) AS B_6_count,
    v.B_6_first, 
    v.B_6_last,
    v.B_6_delta_mean,
    v.B_6_delta_max,
    v.B_6_delta_min,
    v.B_6_delta_pd,
    v.B_6_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_7_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_7 
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
    WHERE B_7 IS NOT NULL
    GROUP BY customer_ID
),
first_B_7 AS
(
    SELECT
        f.customer_ID, s.B_7 AS B_7_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_7 AS
(
    SELECT
        f.customer_ID, s.B_7 AS B_7_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_7_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_7_span
    FROM
        first_last
),
B_7_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_7,
        s.B_7 - LAG(s.B_7, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_7_delta
    FROM
        subset s
),
B_7_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_7_delta
    FROM
        B_7_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_7_delta_per_day AS
(
    SELECT
        customer_ID,
        B_7_delta / date_delta AS B_7_delta_per_day
    FROM
        B_7_delta
),
B_7_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_7_delta_per_day) AS B_7_delta_pd
    FROM
        B_7_delta_per_day
    GROUP BY
        customer_ID
),      
B_7_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_7_delta) AS B_7_delta_mean,
        MAX(B_7_delta) AS B_7_delta_max,
        MIN(B_7_delta) AS B_7_delta_min
    FROM
        B_7_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_7) AS B_7_mean,
        MIN(B_7) AS B_7_min, 
        MAX(B_7) AS B_7_max, 
        SUM(B_7) AS B_7_sum,
        COUNT(B_7) AS B_7_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_7_mean,
        a.B_7_min, 
        a.B_7_max, 
        a.B_7_sum,
        a.B_7_max - a.B_7_min AS B_7_range,
        a.B_7_count,
        f.B_7_first,
        l.B_7_last,
        d.B_7_delta_mean,
        d.B_7_delta_max,
        d.B_7_delta_min,
        pd.B_7_delta_pd,
        cs.B_7_span
    FROM
        aggs a
        LEFT JOIN first_B_7 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_7 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_7_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_7_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_7_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_7_mean, 
    v.B_7_min,
    v.B_7_max, 
    v.B_7_range,
    v.B_7_sum,
    ISNULL(v.B_7_count, 0) AS B_7_count,
    v.B_7_first, 
    v.B_7_last,
    v.B_7_delta_mean,
    v.B_7_delta_max,
    v.B_7_delta_min,
    v.B_7_delta_pd,
    v.B_7_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_8_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_8 
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
GO

CREATE VIEW test_data_D_50_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_50 
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
    WHERE D_50 IS NOT NULL
    GROUP BY customer_ID
),
first_D_50 AS
(
    SELECT
        f.customer_ID, s.D_50 AS D_50_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_50 AS
(
    SELECT
        f.customer_ID, s.D_50 AS D_50_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_50_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_50_span
    FROM
        first_last
),
D_50_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_50,
        s.D_50 - LAG(s.D_50, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_50_delta
    FROM
        subset s
),
D_50_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_50_delta
    FROM
        D_50_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_50_delta_per_day AS
(
    SELECT
        customer_ID,
        D_50_delta / date_delta AS D_50_delta_per_day
    FROM
        D_50_delta
),
D_50_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_50_delta_per_day) AS D_50_delta_pd
    FROM
        D_50_delta_per_day
    GROUP BY
        customer_ID
),      
D_50_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_50_delta) AS D_50_delta_mean,
        MAX(D_50_delta) AS D_50_delta_max,
        MIN(D_50_delta) AS D_50_delta_min
    FROM
        D_50_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_50) AS D_50_mean,
        MIN(D_50) AS D_50_min, 
        MAX(D_50) AS D_50_max, 
        SUM(D_50) AS D_50_sum,
        COUNT(D_50) AS D_50_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_50_mean,
        a.D_50_min, 
        a.D_50_max, 
        a.D_50_sum,
        a.D_50_max - a.D_50_min AS D_50_range,
        a.D_50_count,
        f.D_50_first,
        l.D_50_last,
        d.D_50_delta_mean,
        d.D_50_delta_max,
        d.D_50_delta_min,
        pd.D_50_delta_pd,
        cs.D_50_span
    FROM
        aggs a
        LEFT JOIN first_D_50 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_50 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_50_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_50_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_50_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_50_mean, 
    v.D_50_min,
    v.D_50_max, 
    v.D_50_range,
    v.D_50_sum,
    ISNULL(v.D_50_count, 0) AS D_50_count,
    v.D_50_first, 
    v.D_50_last,
    v.D_50_delta_mean,
    v.D_50_delta_max,
    v.D_50_delta_min,
    v.D_50_delta_pd,
    v.D_50_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_51_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_51 
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
    WHERE D_51 IS NOT NULL
    GROUP BY customer_ID
),
first_D_51 AS
(
    SELECT
        f.customer_ID, s.D_51 AS D_51_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_51 AS
(
    SELECT
        f.customer_ID, s.D_51 AS D_51_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_51_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_51_span
    FROM
        first_last
),
D_51_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_51,
        s.D_51 - LAG(s.D_51, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_51_delta
    FROM
        subset s
),
D_51_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_51_delta
    FROM
        D_51_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_51_delta_per_day AS
(
    SELECT
        customer_ID,
        D_51_delta / date_delta AS D_51_delta_per_day
    FROM
        D_51_delta
),
D_51_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_51_delta_per_day) AS D_51_delta_pd
    FROM
        D_51_delta_per_day
    GROUP BY
        customer_ID
),      
D_51_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_51_delta) AS D_51_delta_mean,
        MAX(D_51_delta) AS D_51_delta_max,
        MIN(D_51_delta) AS D_51_delta_min
    FROM
        D_51_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_51) AS D_51_mean,
        MIN(D_51) AS D_51_min, 
        MAX(D_51) AS D_51_max, 
        SUM(D_51) AS D_51_sum,
        COUNT(D_51) AS D_51_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_51_mean,
        a.D_51_min, 
        a.D_51_max, 
        a.D_51_sum,
        a.D_51_max - a.D_51_min AS D_51_range,
        a.D_51_count,
        f.D_51_first,
        l.D_51_last,
        d.D_51_delta_mean,
        d.D_51_delta_max,
        d.D_51_delta_min,
        pd.D_51_delta_pd,
        cs.D_51_span
    FROM
        aggs a
        LEFT JOIN first_D_51 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_51 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_51_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_51_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_51_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_51_mean, 
    v.D_51_min,
    v.D_51_max, 
    v.D_51_range,
    v.D_51_sum,
    ISNULL(v.D_51_count, 0) AS D_51_count,
    v.D_51_first, 
    v.D_51_last,
    v.D_51_delta_mean,
    v.D_51_delta_max,
    v.D_51_delta_min,
    v.D_51_delta_pd,
    v.D_51_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_9_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_9 
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
    WHERE B_9 IS NOT NULL
    GROUP BY customer_ID
),
first_B_9 AS
(
    SELECT
        f.customer_ID, s.B_9 AS B_9_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_9 AS
(
    SELECT
        f.customer_ID, s.B_9 AS B_9_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_9_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_9_span
    FROM
        first_last
),
B_9_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_9,
        s.B_9 - LAG(s.B_9, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_9_delta
    FROM
        subset s
),
B_9_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_9_delta
    FROM
        B_9_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_9_delta_per_day AS
(
    SELECT
        customer_ID,
        B_9_delta / date_delta AS B_9_delta_per_day
    FROM
        B_9_delta
),
B_9_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_9_delta_per_day) AS B_9_delta_pd
    FROM
        B_9_delta_per_day
    GROUP BY
        customer_ID
),      
B_9_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_9_delta) AS B_9_delta_mean,
        MAX(B_9_delta) AS B_9_delta_max,
        MIN(B_9_delta) AS B_9_delta_min
    FROM
        B_9_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_9) AS B_9_mean,
        MIN(B_9) AS B_9_min, 
        MAX(B_9) AS B_9_max, 
        SUM(B_9) AS B_9_sum,
        COUNT(B_9) AS B_9_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_9_mean,
        a.B_9_min, 
        a.B_9_max, 
        a.B_9_sum,
        a.B_9_max - a.B_9_min AS B_9_range,
        a.B_9_count,
        f.B_9_first,
        l.B_9_last,
        d.B_9_delta_mean,
        d.B_9_delta_max,
        d.B_9_delta_min,
        pd.B_9_delta_pd,
        cs.B_9_span
    FROM
        aggs a
        LEFT JOIN first_B_9 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_9 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_9_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_9_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_9_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_9_mean, 
    v.B_9_min,
    v.B_9_max, 
    v.B_9_range,
    v.B_9_sum,
    ISNULL(v.B_9_count, 0) AS B_9_count,
    v.B_9_first, 
    v.B_9_last,
    v.B_9_delta_mean,
    v.B_9_delta_max,
    v.B_9_delta_min,
    v.B_9_delta_pd,
    v.B_9_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_3_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_3 
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
    WHERE R_3 IS NOT NULL
    GROUP BY customer_ID
),
first_R_3 AS
(
    SELECT
        f.customer_ID, s.R_3 AS R_3_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_3 AS
(
    SELECT
        f.customer_ID, s.R_3 AS R_3_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_3_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_3_span
    FROM
        first_last
),
R_3_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_3,
        s.R_3 - LAG(s.R_3, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_3_delta
    FROM
        subset s
),
R_3_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_3_delta
    FROM
        R_3_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_3_delta_per_day AS
(
    SELECT
        customer_ID,
        R_3_delta / date_delta AS R_3_delta_per_day
    FROM
        R_3_delta
),
R_3_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_3_delta_per_day) AS R_3_delta_pd
    FROM
        R_3_delta_per_day
    GROUP BY
        customer_ID
),      
R_3_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_3_delta) AS R_3_delta_mean,
        MAX(R_3_delta) AS R_3_delta_max,
        MIN(R_3_delta) AS R_3_delta_min
    FROM
        R_3_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_3) AS R_3_mean,
        MIN(R_3) AS R_3_min, 
        MAX(R_3) AS R_3_max, 
        SUM(R_3) AS R_3_sum,
        COUNT(R_3) AS R_3_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_3_mean,
        a.R_3_min, 
        a.R_3_max, 
        a.R_3_sum,
        a.R_3_max - a.R_3_min AS R_3_range,
        a.R_3_count,
        f.R_3_first,
        l.R_3_last,
        d.R_3_delta_mean,
        d.R_3_delta_max,
        d.R_3_delta_min,
        pd.R_3_delta_pd,
        cs.R_3_span
    FROM
        aggs a
        LEFT JOIN first_R_3 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_3 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_3_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_3_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_3_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_3_mean, 
    v.R_3_min,
    v.R_3_max, 
    v.R_3_range,
    v.R_3_sum,
    ISNULL(v.R_3_count, 0) AS R_3_count,
    v.R_3_first, 
    v.R_3_last,
    v.R_3_delta_mean,
    v.R_3_delta_max,
    v.R_3_delta_min,
    v.R_3_delta_pd,
    v.R_3_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_52_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_52 
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
    WHERE D_52 IS NOT NULL
    GROUP BY customer_ID
),
first_D_52 AS
(
    SELECT
        f.customer_ID, s.D_52 AS D_52_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_52 AS
(
    SELECT
        f.customer_ID, s.D_52 AS D_52_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_52_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_52_span
    FROM
        first_last
),
D_52_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_52,
        s.D_52 - LAG(s.D_52, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_52_delta
    FROM
        subset s
),
D_52_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_52_delta
    FROM
        D_52_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_52_delta_per_day AS
(
    SELECT
        customer_ID,
        D_52_delta / date_delta AS D_52_delta_per_day
    FROM
        D_52_delta
),
D_52_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_52_delta_per_day) AS D_52_delta_pd
    FROM
        D_52_delta_per_day
    GROUP BY
        customer_ID
),      
D_52_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_52_delta) AS D_52_delta_mean,
        MAX(D_52_delta) AS D_52_delta_max,
        MIN(D_52_delta) AS D_52_delta_min
    FROM
        D_52_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_52) AS D_52_mean,
        MIN(D_52) AS D_52_min, 
        MAX(D_52) AS D_52_max, 
        SUM(D_52) AS D_52_sum,
        COUNT(D_52) AS D_52_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_52_mean,
        a.D_52_min, 
        a.D_52_max, 
        a.D_52_sum,
        a.D_52_max - a.D_52_min AS D_52_range,
        a.D_52_count,
        f.D_52_first,
        l.D_52_last,
        d.D_52_delta_mean,
        d.D_52_delta_max,
        d.D_52_delta_min,
        pd.D_52_delta_pd,
        cs.D_52_span
    FROM
        aggs a
        LEFT JOIN first_D_52 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_52 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_52_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_52_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_52_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_52_mean, 
    v.D_52_min,
    v.D_52_max, 
    v.D_52_range,
    v.D_52_sum,
    ISNULL(v.D_52_count, 0) AS D_52_count,
    v.D_52_first, 
    v.D_52_last,
    v.D_52_delta_mean,
    v.D_52_delta_max,
    v.D_52_delta_min,
    v.D_52_delta_pd,
    v.D_52_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_P_3_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.P_3 
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
    WHERE P_3 IS NOT NULL
    GROUP BY customer_ID
),
first_P_3 AS
(
    SELECT
        f.customer_ID, s.P_3 AS P_3_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_P_3 AS
(
    SELECT
        f.customer_ID, s.P_3 AS P_3_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
P_3_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS P_3_span
    FROM
        first_last
),
P_3_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.P_3,
        s.P_3 - LAG(s.P_3, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS P_3_delta
    FROM
        subset s
),
P_3_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.P_3_delta
    FROM
        P_3_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
P_3_delta_per_day AS
(
    SELECT
        customer_ID,
        P_3_delta / date_delta AS P_3_delta_per_day
    FROM
        P_3_delta
),
P_3_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(P_3_delta_per_day) AS P_3_delta_pd
    FROM
        P_3_delta_per_day
    GROUP BY
        customer_ID
),      
P_3_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(P_3_delta) AS P_3_delta_mean,
        MAX(P_3_delta) AS P_3_delta_max,
        MIN(P_3_delta) AS P_3_delta_min
    FROM
        P_3_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(P_3) AS P_3_mean,
        MIN(P_3) AS P_3_min, 
        MAX(P_3) AS P_3_max, 
        SUM(P_3) AS P_3_sum,
        COUNT(P_3) AS P_3_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.P_3_mean,
        a.P_3_min, 
        a.P_3_max, 
        a.P_3_sum,
        a.P_3_max - a.P_3_min AS P_3_range,
        a.P_3_count,
        f.P_3_first,
        l.P_3_last,
        d.P_3_delta_mean,
        d.P_3_delta_max,
        d.P_3_delta_min,
        pd.P_3_delta_pd,
        cs.P_3_span
    FROM
        aggs a
        LEFT JOIN first_P_3 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_P_3 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN P_3_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN P_3_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN P_3_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.P_3_mean, 
    v.P_3_min,
    v.P_3_max, 
    v.P_3_range,
    v.P_3_sum,
    ISNULL(v.P_3_count, 0) AS P_3_count,
    v.P_3_first, 
    v.P_3_last,
    v.P_3_delta_mean,
    v.P_3_delta_max,
    v.P_3_delta_min,
    v.P_3_delta_pd,
    v.P_3_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_10_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_10 
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
GO

CREATE VIEW test_data_D_53_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_53 
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
    WHERE D_53 IS NOT NULL
    GROUP BY customer_ID
),
first_D_53 AS
(
    SELECT
        f.customer_ID, s.D_53 AS D_53_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_53 AS
(
    SELECT
        f.customer_ID, s.D_53 AS D_53_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_53_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_53_span
    FROM
        first_last
),
D_53_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_53,
        s.D_53 - LAG(s.D_53, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_53_delta
    FROM
        subset s
),
D_53_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_53_delta
    FROM
        D_53_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_53_delta_per_day AS
(
    SELECT
        customer_ID,
        D_53_delta / date_delta AS D_53_delta_per_day
    FROM
        D_53_delta
),
D_53_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_53_delta_per_day) AS D_53_delta_pd
    FROM
        D_53_delta_per_day
    GROUP BY
        customer_ID
),      
D_53_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_53_delta) AS D_53_delta_mean,
        MAX(D_53_delta) AS D_53_delta_max,
        MIN(D_53_delta) AS D_53_delta_min
    FROM
        D_53_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_53) AS D_53_mean,
        MIN(D_53) AS D_53_min, 
        MAX(D_53) AS D_53_max, 
        SUM(D_53) AS D_53_sum,
        COUNT(D_53) AS D_53_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_53_mean,
        a.D_53_min, 
        a.D_53_max, 
        a.D_53_sum,
        a.D_53_max - a.D_53_min AS D_53_range,
        a.D_53_count,
        f.D_53_first,
        l.D_53_last,
        d.D_53_delta_mean,
        d.D_53_delta_max,
        d.D_53_delta_min,
        pd.D_53_delta_pd,
        cs.D_53_span
    FROM
        aggs a
        LEFT JOIN first_D_53 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_53 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_53_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_53_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_53_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_53_mean, 
    v.D_53_min,
    v.D_53_max, 
    v.D_53_range,
    v.D_53_sum,
    ISNULL(v.D_53_count, 0) AS D_53_count,
    v.D_53_first, 
    v.D_53_last,
    v.D_53_delta_mean,
    v.D_53_delta_max,
    v.D_53_delta_min,
    v.D_53_delta_pd,
    v.D_53_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_5_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_5 
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
    WHERE S_5 IS NOT NULL
    GROUP BY customer_ID
),
first_S_5 AS
(
    SELECT
        f.customer_ID, s.S_5 AS S_5_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_5 AS
(
    SELECT
        f.customer_ID, s.S_5 AS S_5_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_5_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_5_span
    FROM
        first_last
),
S_5_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_5,
        s.S_5 - LAG(s.S_5, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_5_delta
    FROM
        subset s
),
S_5_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_5_delta
    FROM
        S_5_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_5_delta_per_day AS
(
    SELECT
        customer_ID,
        S_5_delta / date_delta AS S_5_delta_per_day
    FROM
        S_5_delta
),
S_5_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_5_delta_per_day) AS S_5_delta_pd
    FROM
        S_5_delta_per_day
    GROUP BY
        customer_ID
),      
S_5_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_5_delta) AS S_5_delta_mean,
        MAX(S_5_delta) AS S_5_delta_max,
        MIN(S_5_delta) AS S_5_delta_min
    FROM
        S_5_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_5) AS S_5_mean,
        MIN(S_5) AS S_5_min, 
        MAX(S_5) AS S_5_max, 
        SUM(S_5) AS S_5_sum,
        COUNT(S_5) AS S_5_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_5_mean,
        a.S_5_min, 
        a.S_5_max, 
        a.S_5_sum,
        a.S_5_max - a.S_5_min AS S_5_range,
        a.S_5_count,
        f.S_5_first,
        l.S_5_last,
        d.S_5_delta_mean,
        d.S_5_delta_max,
        d.S_5_delta_min,
        pd.S_5_delta_pd,
        cs.S_5_span
    FROM
        aggs a
        LEFT JOIN first_S_5 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_5 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_5_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_5_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_5_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_5_mean, 
    v.S_5_min,
    v.S_5_max, 
    v.S_5_range,
    v.S_5_sum,
    ISNULL(v.S_5_count, 0) AS S_5_count,
    v.S_5_first, 
    v.S_5_last,
    v.S_5_delta_mean,
    v.S_5_delta_max,
    v.S_5_delta_min,
    v.S_5_delta_pd,
    v.S_5_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_11_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_11 
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
    WHERE B_11 IS NOT NULL
    GROUP BY customer_ID
),
first_B_11 AS
(
    SELECT
        f.customer_ID, s.B_11 AS B_11_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_11 AS
(
    SELECT
        f.customer_ID, s.B_11 AS B_11_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_11_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_11_span
    FROM
        first_last
),
B_11_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_11,
        s.B_11 - LAG(s.B_11, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_11_delta
    FROM
        subset s
),
B_11_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_11_delta
    FROM
        B_11_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_11_delta_per_day AS
(
    SELECT
        customer_ID,
        B_11_delta / date_delta AS B_11_delta_per_day
    FROM
        B_11_delta
),
B_11_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_11_delta_per_day) AS B_11_delta_pd
    FROM
        B_11_delta_per_day
    GROUP BY
        customer_ID
),      
B_11_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_11_delta) AS B_11_delta_mean,
        MAX(B_11_delta) AS B_11_delta_max,
        MIN(B_11_delta) AS B_11_delta_min
    FROM
        B_11_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_11) AS B_11_mean,
        MIN(B_11) AS B_11_min, 
        MAX(B_11) AS B_11_max, 
        SUM(B_11) AS B_11_sum,
        COUNT(B_11) AS B_11_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_11_mean,
        a.B_11_min, 
        a.B_11_max, 
        a.B_11_sum,
        a.B_11_max - a.B_11_min AS B_11_range,
        a.B_11_count,
        f.B_11_first,
        l.B_11_last,
        d.B_11_delta_mean,
        d.B_11_delta_max,
        d.B_11_delta_min,
        pd.B_11_delta_pd,
        cs.B_11_span
    FROM
        aggs a
        LEFT JOIN first_B_11 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_11 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_11_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_11_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_11_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_11_mean, 
    v.B_11_min,
    v.B_11_max, 
    v.B_11_range,
    v.B_11_sum,
    ISNULL(v.B_11_count, 0) AS B_11_count,
    v.B_11_first, 
    v.B_11_last,
    v.B_11_delta_mean,
    v.B_11_delta_max,
    v.B_11_delta_min,
    v.B_11_delta_pd,
    v.B_11_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_6_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_6 
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
    WHERE S_6 IS NOT NULL
    GROUP BY customer_ID
),
first_S_6 AS
(
    SELECT
        f.customer_ID, s.S_6 AS S_6_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_6 AS
(
    SELECT
        f.customer_ID, s.S_6 AS S_6_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_6_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_6_span
    FROM
        first_last
),
S_6_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_6,
        s.S_6 - LAG(s.S_6, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_6_delta
    FROM
        subset s
),
S_6_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_6_delta
    FROM
        S_6_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_6_delta_per_day AS
(
    SELECT
        customer_ID,
        S_6_delta / date_delta AS S_6_delta_per_day
    FROM
        S_6_delta
),
S_6_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_6_delta_per_day) AS S_6_delta_pd
    FROM
        S_6_delta_per_day
    GROUP BY
        customer_ID
),      
S_6_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_6_delta) AS S_6_delta_mean,
        MAX(S_6_delta) AS S_6_delta_max,
        MIN(S_6_delta) AS S_6_delta_min
    FROM
        S_6_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_6) AS S_6_mean,
        MIN(S_6) AS S_6_min, 
        MAX(S_6) AS S_6_max, 
        SUM(S_6) AS S_6_sum,
        COUNT(S_6) AS S_6_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_6_mean,
        a.S_6_min, 
        a.S_6_max, 
        a.S_6_sum,
        a.S_6_max - a.S_6_min AS S_6_range,
        a.S_6_count,
        f.S_6_first,
        l.S_6_last,
        d.S_6_delta_mean,
        d.S_6_delta_max,
        d.S_6_delta_min,
        pd.S_6_delta_pd,
        cs.S_6_span
    FROM
        aggs a
        LEFT JOIN first_S_6 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_6 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_6_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_6_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_6_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_6_mean, 
    v.S_6_min,
    v.S_6_max, 
    v.S_6_range,
    v.S_6_sum,
    ISNULL(v.S_6_count, 0) AS S_6_count,
    v.S_6_first, 
    v.S_6_last,
    v.S_6_delta_mean,
    v.S_6_delta_max,
    v.S_6_delta_min,
    v.S_6_delta_pd,
    v.S_6_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_54_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_54 
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
    WHERE D_54 IS NOT NULL
    GROUP BY customer_ID
),
first_D_54 AS
(
    SELECT
        f.customer_ID, s.D_54 AS D_54_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_54 AS
(
    SELECT
        f.customer_ID, s.D_54 AS D_54_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_54_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_54_span
    FROM
        first_last
),
D_54_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_54,
        s.D_54 - LAG(s.D_54, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_54_delta
    FROM
        subset s
),
D_54_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_54_delta
    FROM
        D_54_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_54_delta_per_day AS
(
    SELECT
        customer_ID,
        D_54_delta / date_delta AS D_54_delta_per_day
    FROM
        D_54_delta
),
D_54_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_54_delta_per_day) AS D_54_delta_pd
    FROM
        D_54_delta_per_day
    GROUP BY
        customer_ID
),      
D_54_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_54_delta) AS D_54_delta_mean,
        MAX(D_54_delta) AS D_54_delta_max,
        MIN(D_54_delta) AS D_54_delta_min
    FROM
        D_54_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_54) AS D_54_mean,
        MIN(D_54) AS D_54_min, 
        MAX(D_54) AS D_54_max, 
        SUM(D_54) AS D_54_sum,
        COUNT(D_54) AS D_54_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_54_mean,
        a.D_54_min, 
        a.D_54_max, 
        a.D_54_sum,
        a.D_54_max - a.D_54_min AS D_54_range,
        a.D_54_count,
        f.D_54_first,
        l.D_54_last,
        d.D_54_delta_mean,
        d.D_54_delta_max,
        d.D_54_delta_min,
        pd.D_54_delta_pd,
        cs.D_54_span
    FROM
        aggs a
        LEFT JOIN first_D_54 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_54 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_54_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_54_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_54_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_54_mean, 
    v.D_54_min,
    v.D_54_max, 
    v.D_54_range,
    v.D_54_sum,
    ISNULL(v.D_54_count, 0) AS D_54_count,
    v.D_54_first, 
    v.D_54_last,
    v.D_54_delta_mean,
    v.D_54_delta_max,
    v.D_54_delta_min,
    v.D_54_delta_pd,
    v.D_54_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_4_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_4 
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
    WHERE R_4 IS NOT NULL
    GROUP BY customer_ID
),
first_R_4 AS
(
    SELECT
        f.customer_ID, s.R_4 AS R_4_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_4 AS
(
    SELECT
        f.customer_ID, s.R_4 AS R_4_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_4_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_4_span
    FROM
        first_last
),
R_4_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_4,
        s.R_4 - LAG(s.R_4, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_4_delta
    FROM
        subset s
),
R_4_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_4_delta
    FROM
        R_4_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_4_delta_per_day AS
(
    SELECT
        customer_ID,
        R_4_delta / date_delta AS R_4_delta_per_day
    FROM
        R_4_delta
),
R_4_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_4_delta_per_day) AS R_4_delta_pd
    FROM
        R_4_delta_per_day
    GROUP BY
        customer_ID
),      
R_4_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_4_delta) AS R_4_delta_mean,
        MAX(R_4_delta) AS R_4_delta_max,
        MIN(R_4_delta) AS R_4_delta_min
    FROM
        R_4_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_4) AS R_4_mean,
        MIN(R_4) AS R_4_min, 
        MAX(R_4) AS R_4_max, 
        SUM(R_4) AS R_4_sum,
        COUNT(R_4) AS R_4_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_4_mean,
        a.R_4_min, 
        a.R_4_max, 
        a.R_4_sum,
        a.R_4_max - a.R_4_min AS R_4_range,
        a.R_4_count,
        f.R_4_first,
        l.R_4_last,
        d.R_4_delta_mean,
        d.R_4_delta_max,
        d.R_4_delta_min,
        pd.R_4_delta_pd,
        cs.R_4_span
    FROM
        aggs a
        LEFT JOIN first_R_4 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_4 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_4_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_4_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_4_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_4_mean, 
    v.R_4_min,
    v.R_4_max, 
    v.R_4_range,
    v.R_4_sum,
    ISNULL(v.R_4_count, 0) AS R_4_count,
    v.R_4_first, 
    v.R_4_last,
    v.R_4_delta_mean,
    v.R_4_delta_max,
    v.R_4_delta_min,
    v.R_4_delta_pd,
    v.R_4_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_7_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_7 
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
    WHERE S_7 IS NOT NULL
    GROUP BY customer_ID
),
first_S_7 AS
(
    SELECT
        f.customer_ID, s.S_7 AS S_7_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_7 AS
(
    SELECT
        f.customer_ID, s.S_7 AS S_7_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_7_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_7_span
    FROM
        first_last
),
S_7_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_7,
        s.S_7 - LAG(s.S_7, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_7_delta
    FROM
        subset s
),
S_7_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_7_delta
    FROM
        S_7_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_7_delta_per_day AS
(
    SELECT
        customer_ID,
        S_7_delta / date_delta AS S_7_delta_per_day
    FROM
        S_7_delta
),
S_7_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_7_delta_per_day) AS S_7_delta_pd
    FROM
        S_7_delta_per_day
    GROUP BY
        customer_ID
),      
S_7_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_7_delta) AS S_7_delta_mean,
        MAX(S_7_delta) AS S_7_delta_max,
        MIN(S_7_delta) AS S_7_delta_min
    FROM
        S_7_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_7) AS S_7_mean,
        MIN(S_7) AS S_7_min, 
        MAX(S_7) AS S_7_max, 
        SUM(S_7) AS S_7_sum,
        COUNT(S_7) AS S_7_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_7_mean,
        a.S_7_min, 
        a.S_7_max, 
        a.S_7_sum,
        a.S_7_max - a.S_7_min AS S_7_range,
        a.S_7_count,
        f.S_7_first,
        l.S_7_last,
        d.S_7_delta_mean,
        d.S_7_delta_max,
        d.S_7_delta_min,
        pd.S_7_delta_pd,
        cs.S_7_span
    FROM
        aggs a
        LEFT JOIN first_S_7 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_7 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_7_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_7_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_7_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_7_mean, 
    v.S_7_min,
    v.S_7_max, 
    v.S_7_range,
    v.S_7_sum,
    ISNULL(v.S_7_count, 0) AS S_7_count,
    v.S_7_first, 
    v.S_7_last,
    v.S_7_delta_mean,
    v.S_7_delta_max,
    v.S_7_delta_min,
    v.S_7_delta_pd,
    v.S_7_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_12_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_12 
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
    WHERE B_12 IS NOT NULL
    GROUP BY customer_ID
),
first_B_12 AS
(
    SELECT
        f.customer_ID, s.B_12 AS B_12_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_12 AS
(
    SELECT
        f.customer_ID, s.B_12 AS B_12_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_12_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_12_span
    FROM
        first_last
),
B_12_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_12,
        s.B_12 - LAG(s.B_12, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_12_delta
    FROM
        subset s
),
B_12_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_12_delta
    FROM
        B_12_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_12_delta_per_day AS
(
    SELECT
        customer_ID,
        B_12_delta / date_delta AS B_12_delta_per_day
    FROM
        B_12_delta
),
B_12_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_12_delta_per_day) AS B_12_delta_pd
    FROM
        B_12_delta_per_day
    GROUP BY
        customer_ID
),      
B_12_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_12_delta) AS B_12_delta_mean,
        MAX(B_12_delta) AS B_12_delta_max,
        MIN(B_12_delta) AS B_12_delta_min
    FROM
        B_12_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_12) AS B_12_mean,
        MIN(B_12) AS B_12_min, 
        MAX(B_12) AS B_12_max, 
        SUM(B_12) AS B_12_sum,
        COUNT(B_12) AS B_12_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_12_mean,
        a.B_12_min, 
        a.B_12_max, 
        a.B_12_sum,
        a.B_12_max - a.B_12_min AS B_12_range,
        a.B_12_count,
        f.B_12_first,
        l.B_12_last,
        d.B_12_delta_mean,
        d.B_12_delta_max,
        d.B_12_delta_min,
        pd.B_12_delta_pd,
        cs.B_12_span
    FROM
        aggs a
        LEFT JOIN first_B_12 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_12 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_12_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_12_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_12_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_12_mean, 
    v.B_12_min,
    v.B_12_max, 
    v.B_12_range,
    v.B_12_sum,
    ISNULL(v.B_12_count, 0) AS B_12_count,
    v.B_12_first, 
    v.B_12_last,
    v.B_12_delta_mean,
    v.B_12_delta_max,
    v.B_12_delta_min,
    v.B_12_delta_pd,
    v.B_12_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_8_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_8 
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
    WHERE S_8 IS NOT NULL
    GROUP BY customer_ID
),
first_S_8 AS
(
    SELECT
        f.customer_ID, s.S_8 AS S_8_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_8 AS
(
    SELECT
        f.customer_ID, s.S_8 AS S_8_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_8_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_8_span
    FROM
        first_last
),
S_8_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_8,
        s.S_8 - LAG(s.S_8, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_8_delta
    FROM
        subset s
),
S_8_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_8_delta
    FROM
        S_8_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_8_delta_per_day AS
(
    SELECT
        customer_ID,
        S_8_delta / date_delta AS S_8_delta_per_day
    FROM
        S_8_delta
),
S_8_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_8_delta_per_day) AS S_8_delta_pd
    FROM
        S_8_delta_per_day
    GROUP BY
        customer_ID
),      
S_8_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_8_delta) AS S_8_delta_mean,
        MAX(S_8_delta) AS S_8_delta_max,
        MIN(S_8_delta) AS S_8_delta_min
    FROM
        S_8_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_8) AS S_8_mean,
        MIN(S_8) AS S_8_min, 
        MAX(S_8) AS S_8_max, 
        SUM(S_8) AS S_8_sum,
        COUNT(S_8) AS S_8_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_8_mean,
        a.S_8_min, 
        a.S_8_max, 
        a.S_8_sum,
        a.S_8_max - a.S_8_min AS S_8_range,
        a.S_8_count,
        f.S_8_first,
        l.S_8_last,
        d.S_8_delta_mean,
        d.S_8_delta_max,
        d.S_8_delta_min,
        pd.S_8_delta_pd,
        cs.S_8_span
    FROM
        aggs a
        LEFT JOIN first_S_8 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_8 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_8_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_8_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_8_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_8_mean, 
    v.S_8_min,
    v.S_8_max, 
    v.S_8_range,
    v.S_8_sum,
    ISNULL(v.S_8_count, 0) AS S_8_count,
    v.S_8_first, 
    v.S_8_last,
    v.S_8_delta_mean,
    v.S_8_delta_max,
    v.S_8_delta_min,
    v.S_8_delta_pd,
    v.S_8_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_55_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_55 
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
    WHERE D_55 IS NOT NULL
    GROUP BY customer_ID
),
first_D_55 AS
(
    SELECT
        f.customer_ID, s.D_55 AS D_55_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_55 AS
(
    SELECT
        f.customer_ID, s.D_55 AS D_55_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_55_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_55_span
    FROM
        first_last
),
D_55_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_55,
        s.D_55 - LAG(s.D_55, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_55_delta
    FROM
        subset s
),
D_55_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_55_delta
    FROM
        D_55_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_55_delta_per_day AS
(
    SELECT
        customer_ID,
        D_55_delta / date_delta AS D_55_delta_per_day
    FROM
        D_55_delta
),
D_55_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_55_delta_per_day) AS D_55_delta_pd
    FROM
        D_55_delta_per_day
    GROUP BY
        customer_ID
),      
D_55_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_55_delta) AS D_55_delta_mean,
        MAX(D_55_delta) AS D_55_delta_max,
        MIN(D_55_delta) AS D_55_delta_min
    FROM
        D_55_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_55) AS D_55_mean,
        MIN(D_55) AS D_55_min, 
        MAX(D_55) AS D_55_max, 
        SUM(D_55) AS D_55_sum,
        COUNT(D_55) AS D_55_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_55_mean,
        a.D_55_min, 
        a.D_55_max, 
        a.D_55_sum,
        a.D_55_max - a.D_55_min AS D_55_range,
        a.D_55_count,
        f.D_55_first,
        l.D_55_last,
        d.D_55_delta_mean,
        d.D_55_delta_max,
        d.D_55_delta_min,
        pd.D_55_delta_pd,
        cs.D_55_span
    FROM
        aggs a
        LEFT JOIN first_D_55 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_55 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_55_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_55_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_55_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_55_mean, 
    v.D_55_min,
    v.D_55_max, 
    v.D_55_range,
    v.D_55_sum,
    ISNULL(v.D_55_count, 0) AS D_55_count,
    v.D_55_first, 
    v.D_55_last,
    v.D_55_delta_mean,
    v.D_55_delta_max,
    v.D_55_delta_min,
    v.D_55_delta_pd,
    v.D_55_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_56_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_56 
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
    WHERE D_56 IS NOT NULL
    GROUP BY customer_ID
),
first_D_56 AS
(
    SELECT
        f.customer_ID, s.D_56 AS D_56_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_56 AS
(
    SELECT
        f.customer_ID, s.D_56 AS D_56_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_56_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_56_span
    FROM
        first_last
),
D_56_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_56,
        s.D_56 - LAG(s.D_56, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_56_delta
    FROM
        subset s
),
D_56_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_56_delta
    FROM
        D_56_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_56_delta_per_day AS
(
    SELECT
        customer_ID,
        D_56_delta / date_delta AS D_56_delta_per_day
    FROM
        D_56_delta
),
D_56_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_56_delta_per_day) AS D_56_delta_pd
    FROM
        D_56_delta_per_day
    GROUP BY
        customer_ID
),      
D_56_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_56_delta) AS D_56_delta_mean,
        MAX(D_56_delta) AS D_56_delta_max,
        MIN(D_56_delta) AS D_56_delta_min
    FROM
        D_56_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_56) AS D_56_mean,
        MIN(D_56) AS D_56_min, 
        MAX(D_56) AS D_56_max, 
        SUM(D_56) AS D_56_sum,
        COUNT(D_56) AS D_56_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_56_mean,
        a.D_56_min, 
        a.D_56_max, 
        a.D_56_sum,
        a.D_56_max - a.D_56_min AS D_56_range,
        a.D_56_count,
        f.D_56_first,
        l.D_56_last,
        d.D_56_delta_mean,
        d.D_56_delta_max,
        d.D_56_delta_min,
        pd.D_56_delta_pd,
        cs.D_56_span
    FROM
        aggs a
        LEFT JOIN first_D_56 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_56 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_56_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_56_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_56_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_56_mean, 
    v.D_56_min,
    v.D_56_max, 
    v.D_56_range,
    v.D_56_sum,
    ISNULL(v.D_56_count, 0) AS D_56_count,
    v.D_56_first, 
    v.D_56_last,
    v.D_56_delta_mean,
    v.D_56_delta_max,
    v.D_56_delta_min,
    v.D_56_delta_pd,
    v.D_56_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_13_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_13 
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
    WHERE B_13 IS NOT NULL
    GROUP BY customer_ID
),
first_B_13 AS
(
    SELECT
        f.customer_ID, s.B_13 AS B_13_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_13 AS
(
    SELECT
        f.customer_ID, s.B_13 AS B_13_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_13_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_13_span
    FROM
        first_last
),
B_13_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_13,
        s.B_13 - LAG(s.B_13, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_13_delta
    FROM
        subset s
),
B_13_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_13_delta
    FROM
        B_13_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_13_delta_per_day AS
(
    SELECT
        customer_ID,
        B_13_delta / date_delta AS B_13_delta_per_day
    FROM
        B_13_delta
),
B_13_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_13_delta_per_day) AS B_13_delta_pd
    FROM
        B_13_delta_per_day
    GROUP BY
        customer_ID
),      
B_13_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_13_delta) AS B_13_delta_mean,
        MAX(B_13_delta) AS B_13_delta_max,
        MIN(B_13_delta) AS B_13_delta_min
    FROM
        B_13_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_13) AS B_13_mean,
        MIN(B_13) AS B_13_min, 
        MAX(B_13) AS B_13_max, 
        SUM(B_13) AS B_13_sum,
        COUNT(B_13) AS B_13_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_13_mean,
        a.B_13_min, 
        a.B_13_max, 
        a.B_13_sum,
        a.B_13_max - a.B_13_min AS B_13_range,
        a.B_13_count,
        f.B_13_first,
        l.B_13_last,
        d.B_13_delta_mean,
        d.B_13_delta_max,
        d.B_13_delta_min,
        pd.B_13_delta_pd,
        cs.B_13_span
    FROM
        aggs a
        LEFT JOIN first_B_13 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_13 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_13_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_13_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_13_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_13_mean, 
    v.B_13_min,
    v.B_13_max, 
    v.B_13_range,
    v.B_13_sum,
    ISNULL(v.B_13_count, 0) AS B_13_count,
    v.B_13_first, 
    v.B_13_last,
    v.B_13_delta_mean,
    v.B_13_delta_max,
    v.B_13_delta_min,
    v.B_13_delta_pd,
    v.B_13_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_5_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_5 
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
    WHERE R_5 IS NOT NULL
    GROUP BY customer_ID
),
first_R_5 AS
(
    SELECT
        f.customer_ID, s.R_5 AS R_5_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_5 AS
(
    SELECT
        f.customer_ID, s.R_5 AS R_5_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_5_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_5_span
    FROM
        first_last
),
R_5_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_5,
        s.R_5 - LAG(s.R_5, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_5_delta
    FROM
        subset s
),
R_5_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_5_delta
    FROM
        R_5_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_5_delta_per_day AS
(
    SELECT
        customer_ID,
        R_5_delta / date_delta AS R_5_delta_per_day
    FROM
        R_5_delta
),
R_5_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_5_delta_per_day) AS R_5_delta_pd
    FROM
        R_5_delta_per_day
    GROUP BY
        customer_ID
),      
R_5_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_5_delta) AS R_5_delta_mean,
        MAX(R_5_delta) AS R_5_delta_max,
        MIN(R_5_delta) AS R_5_delta_min
    FROM
        R_5_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_5) AS R_5_mean,
        MIN(R_5) AS R_5_min, 
        MAX(R_5) AS R_5_max, 
        SUM(R_5) AS R_5_sum,
        COUNT(R_5) AS R_5_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_5_mean,
        a.R_5_min, 
        a.R_5_max, 
        a.R_5_sum,
        a.R_5_max - a.R_5_min AS R_5_range,
        a.R_5_count,
        f.R_5_first,
        l.R_5_last,
        d.R_5_delta_mean,
        d.R_5_delta_max,
        d.R_5_delta_min,
        pd.R_5_delta_pd,
        cs.R_5_span
    FROM
        aggs a
        LEFT JOIN first_R_5 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_5 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_5_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_5_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_5_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_5_mean, 
    v.R_5_min,
    v.R_5_max, 
    v.R_5_range,
    v.R_5_sum,
    ISNULL(v.R_5_count, 0) AS R_5_count,
    v.R_5_first, 
    v.R_5_last,
    v.R_5_delta_mean,
    v.R_5_delta_max,
    v.R_5_delta_min,
    v.R_5_delta_pd,
    v.R_5_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_58_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_58 
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
    WHERE D_58 IS NOT NULL
    GROUP BY customer_ID
),
first_D_58 AS
(
    SELECT
        f.customer_ID, s.D_58 AS D_58_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_58 AS
(
    SELECT
        f.customer_ID, s.D_58 AS D_58_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_58_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_58_span
    FROM
        first_last
),
D_58_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_58,
        s.D_58 - LAG(s.D_58, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_58_delta
    FROM
        subset s
),
D_58_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_58_delta
    FROM
        D_58_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_58_delta_per_day AS
(
    SELECT
        customer_ID,
        D_58_delta / date_delta AS D_58_delta_per_day
    FROM
        D_58_delta
),
D_58_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_58_delta_per_day) AS D_58_delta_pd
    FROM
        D_58_delta_per_day
    GROUP BY
        customer_ID
),      
D_58_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_58_delta) AS D_58_delta_mean,
        MAX(D_58_delta) AS D_58_delta_max,
        MIN(D_58_delta) AS D_58_delta_min
    FROM
        D_58_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_58) AS D_58_mean,
        MIN(D_58) AS D_58_min, 
        MAX(D_58) AS D_58_max, 
        SUM(D_58) AS D_58_sum,
        COUNT(D_58) AS D_58_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_58_mean,
        a.D_58_min, 
        a.D_58_max, 
        a.D_58_sum,
        a.D_58_max - a.D_58_min AS D_58_range,
        a.D_58_count,
        f.D_58_first,
        l.D_58_last,
        d.D_58_delta_mean,
        d.D_58_delta_max,
        d.D_58_delta_min,
        pd.D_58_delta_pd,
        cs.D_58_span
    FROM
        aggs a
        LEFT JOIN first_D_58 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_58 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_58_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_58_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_58_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_58_mean, 
    v.D_58_min,
    v.D_58_max, 
    v.D_58_range,
    v.D_58_sum,
    ISNULL(v.D_58_count, 0) AS D_58_count,
    v.D_58_first, 
    v.D_58_last,
    v.D_58_delta_mean,
    v.D_58_delta_max,
    v.D_58_delta_min,
    v.D_58_delta_pd,
    v.D_58_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_9_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_9 
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
    WHERE S_9 IS NOT NULL
    GROUP BY customer_ID
),
first_S_9 AS
(
    SELECT
        f.customer_ID, s.S_9 AS S_9_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_9 AS
(
    SELECT
        f.customer_ID, s.S_9 AS S_9_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_9_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_9_span
    FROM
        first_last
),
S_9_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_9,
        s.S_9 - LAG(s.S_9, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_9_delta
    FROM
        subset s
),
S_9_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_9_delta
    FROM
        S_9_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_9_delta_per_day AS
(
    SELECT
        customer_ID,
        S_9_delta / date_delta AS S_9_delta_per_day
    FROM
        S_9_delta
),
S_9_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_9_delta_per_day) AS S_9_delta_pd
    FROM
        S_9_delta_per_day
    GROUP BY
        customer_ID
),      
S_9_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_9_delta) AS S_9_delta_mean,
        MAX(S_9_delta) AS S_9_delta_max,
        MIN(S_9_delta) AS S_9_delta_min
    FROM
        S_9_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_9) AS S_9_mean,
        MIN(S_9) AS S_9_min, 
        MAX(S_9) AS S_9_max, 
        SUM(S_9) AS S_9_sum,
        COUNT(S_9) AS S_9_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_9_mean,
        a.S_9_min, 
        a.S_9_max, 
        a.S_9_sum,
        a.S_9_max - a.S_9_min AS S_9_range,
        a.S_9_count,
        f.S_9_first,
        l.S_9_last,
        d.S_9_delta_mean,
        d.S_9_delta_max,
        d.S_9_delta_min,
        pd.S_9_delta_pd,
        cs.S_9_span
    FROM
        aggs a
        LEFT JOIN first_S_9 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_9 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_9_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_9_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_9_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_9_mean, 
    v.S_9_min,
    v.S_9_max, 
    v.S_9_range,
    v.S_9_sum,
    ISNULL(v.S_9_count, 0) AS S_9_count,
    v.S_9_first, 
    v.S_9_last,
    v.S_9_delta_mean,
    v.S_9_delta_max,
    v.S_9_delta_min,
    v.S_9_delta_pd,
    v.S_9_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_14_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_14 
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
    WHERE B_14 IS NOT NULL
    GROUP BY customer_ID
),
first_B_14 AS
(
    SELECT
        f.customer_ID, s.B_14 AS B_14_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_14 AS
(
    SELECT
        f.customer_ID, s.B_14 AS B_14_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_14_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_14_span
    FROM
        first_last
),
B_14_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_14,
        s.B_14 - LAG(s.B_14, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_14_delta
    FROM
        subset s
),
B_14_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_14_delta
    FROM
        B_14_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_14_delta_per_day AS
(
    SELECT
        customer_ID,
        B_14_delta / date_delta AS B_14_delta_per_day
    FROM
        B_14_delta
),
B_14_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_14_delta_per_day) AS B_14_delta_pd
    FROM
        B_14_delta_per_day
    GROUP BY
        customer_ID
),      
B_14_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_14_delta) AS B_14_delta_mean,
        MAX(B_14_delta) AS B_14_delta_max,
        MIN(B_14_delta) AS B_14_delta_min
    FROM
        B_14_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_14) AS B_14_mean,
        MIN(B_14) AS B_14_min, 
        MAX(B_14) AS B_14_max, 
        SUM(B_14) AS B_14_sum,
        COUNT(B_14) AS B_14_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_14_mean,
        a.B_14_min, 
        a.B_14_max, 
        a.B_14_sum,
        a.B_14_max - a.B_14_min AS B_14_range,
        a.B_14_count,
        f.B_14_first,
        l.B_14_last,
        d.B_14_delta_mean,
        d.B_14_delta_max,
        d.B_14_delta_min,
        pd.B_14_delta_pd,
        cs.B_14_span
    FROM
        aggs a
        LEFT JOIN first_B_14 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_14 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_14_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_14_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_14_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_14_mean, 
    v.B_14_min,
    v.B_14_max, 
    v.B_14_range,
    v.B_14_sum,
    ISNULL(v.B_14_count, 0) AS B_14_count,
    v.B_14_first, 
    v.B_14_last,
    v.B_14_delta_mean,
    v.B_14_delta_max,
    v.B_14_delta_min,
    v.B_14_delta_pd,
    v.B_14_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_59_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_59 
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
    WHERE D_59 IS NOT NULL
    GROUP BY customer_ID
),
first_D_59 AS
(
    SELECT
        f.customer_ID, s.D_59 AS D_59_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_59 AS
(
    SELECT
        f.customer_ID, s.D_59 AS D_59_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_59_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_59_span
    FROM
        first_last
),
D_59_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_59,
        s.D_59 - LAG(s.D_59, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_59_delta
    FROM
        subset s
),
D_59_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_59_delta
    FROM
        D_59_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_59_delta_per_day AS
(
    SELECT
        customer_ID,
        D_59_delta / date_delta AS D_59_delta_per_day
    FROM
        D_59_delta
),
D_59_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_59_delta_per_day) AS D_59_delta_pd
    FROM
        D_59_delta_per_day
    GROUP BY
        customer_ID
),      
D_59_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_59_delta) AS D_59_delta_mean,
        MAX(D_59_delta) AS D_59_delta_max,
        MIN(D_59_delta) AS D_59_delta_min
    FROM
        D_59_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_59) AS D_59_mean,
        MIN(D_59) AS D_59_min, 
        MAX(D_59) AS D_59_max, 
        SUM(D_59) AS D_59_sum,
        COUNT(D_59) AS D_59_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_59_mean,
        a.D_59_min, 
        a.D_59_max, 
        a.D_59_sum,
        a.D_59_max - a.D_59_min AS D_59_range,
        a.D_59_count,
        f.D_59_first,
        l.D_59_last,
        d.D_59_delta_mean,
        d.D_59_delta_max,
        d.D_59_delta_min,
        pd.D_59_delta_pd,
        cs.D_59_span
    FROM
        aggs a
        LEFT JOIN first_D_59 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_59 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_59_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_59_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_59_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_59_mean, 
    v.D_59_min,
    v.D_59_max, 
    v.D_59_range,
    v.D_59_sum,
    ISNULL(v.D_59_count, 0) AS D_59_count,
    v.D_59_first, 
    v.D_59_last,
    v.D_59_delta_mean,
    v.D_59_delta_max,
    v.D_59_delta_min,
    v.D_59_delta_pd,
    v.D_59_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_60_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_60 
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
    WHERE D_60 IS NOT NULL
    GROUP BY customer_ID
),
first_D_60 AS
(
    SELECT
        f.customer_ID, s.D_60 AS D_60_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_60 AS
(
    SELECT
        f.customer_ID, s.D_60 AS D_60_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_60_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_60_span
    FROM
        first_last
),
D_60_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_60,
        s.D_60 - LAG(s.D_60, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_60_delta
    FROM
        subset s
),
D_60_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_60_delta
    FROM
        D_60_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_60_delta_per_day AS
(
    SELECT
        customer_ID,
        D_60_delta / date_delta AS D_60_delta_per_day
    FROM
        D_60_delta
),
D_60_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_60_delta_per_day) AS D_60_delta_pd
    FROM
        D_60_delta_per_day
    GROUP BY
        customer_ID
),      
D_60_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_60_delta) AS D_60_delta_mean,
        MAX(D_60_delta) AS D_60_delta_max,
        MIN(D_60_delta) AS D_60_delta_min
    FROM
        D_60_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_60) AS D_60_mean,
        MIN(D_60) AS D_60_min, 
        MAX(D_60) AS D_60_max, 
        SUM(D_60) AS D_60_sum,
        COUNT(D_60) AS D_60_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_60_mean,
        a.D_60_min, 
        a.D_60_max, 
        a.D_60_sum,
        a.D_60_max - a.D_60_min AS D_60_range,
        a.D_60_count,
        f.D_60_first,
        l.D_60_last,
        d.D_60_delta_mean,
        d.D_60_delta_max,
        d.D_60_delta_min,
        pd.D_60_delta_pd,
        cs.D_60_span
    FROM
        aggs a
        LEFT JOIN first_D_60 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_60 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_60_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_60_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_60_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_60_mean, 
    v.D_60_min,
    v.D_60_max, 
    v.D_60_range,
    v.D_60_sum,
    ISNULL(v.D_60_count, 0) AS D_60_count,
    v.D_60_first, 
    v.D_60_last,
    v.D_60_delta_mean,
    v.D_60_delta_max,
    v.D_60_delta_min,
    v.D_60_delta_pd,
    v.D_60_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_61_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_61 
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
    WHERE D_61 IS NOT NULL
    GROUP BY customer_ID
),
first_D_61 AS
(
    SELECT
        f.customer_ID, s.D_61 AS D_61_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_61 AS
(
    SELECT
        f.customer_ID, s.D_61 AS D_61_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_61_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_61_span
    FROM
        first_last
),
D_61_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_61,
        s.D_61 - LAG(s.D_61, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_61_delta
    FROM
        subset s
),
D_61_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_61_delta
    FROM
        D_61_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_61_delta_per_day AS
(
    SELECT
        customer_ID,
        D_61_delta / date_delta AS D_61_delta_per_day
    FROM
        D_61_delta
),
D_61_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_61_delta_per_day) AS D_61_delta_pd
    FROM
        D_61_delta_per_day
    GROUP BY
        customer_ID
),      
D_61_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_61_delta) AS D_61_delta_mean,
        MAX(D_61_delta) AS D_61_delta_max,
        MIN(D_61_delta) AS D_61_delta_min
    FROM
        D_61_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_61) AS D_61_mean,
        MIN(D_61) AS D_61_min, 
        MAX(D_61) AS D_61_max, 
        SUM(D_61) AS D_61_sum,
        COUNT(D_61) AS D_61_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_61_mean,
        a.D_61_min, 
        a.D_61_max, 
        a.D_61_sum,
        a.D_61_max - a.D_61_min AS D_61_range,
        a.D_61_count,
        f.D_61_first,
        l.D_61_last,
        d.D_61_delta_mean,
        d.D_61_delta_max,
        d.D_61_delta_min,
        pd.D_61_delta_pd,
        cs.D_61_span
    FROM
        aggs a
        LEFT JOIN first_D_61 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_61 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_61_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_61_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_61_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_61_mean, 
    v.D_61_min,
    v.D_61_max, 
    v.D_61_range,
    v.D_61_sum,
    ISNULL(v.D_61_count, 0) AS D_61_count,
    v.D_61_first, 
    v.D_61_last,
    v.D_61_delta_mean,
    v.D_61_delta_max,
    v.D_61_delta_min,
    v.D_61_delta_pd,
    v.D_61_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_15_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_15 
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
    WHERE B_15 IS NOT NULL
    GROUP BY customer_ID
),
first_B_15 AS
(
    SELECT
        f.customer_ID, s.B_15 AS B_15_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_15 AS
(
    SELECT
        f.customer_ID, s.B_15 AS B_15_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_15_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_15_span
    FROM
        first_last
),
B_15_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_15,
        s.B_15 - LAG(s.B_15, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_15_delta
    FROM
        subset s
),
B_15_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_15_delta
    FROM
        B_15_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_15_delta_per_day AS
(
    SELECT
        customer_ID,
        B_15_delta / date_delta AS B_15_delta_per_day
    FROM
        B_15_delta
),
B_15_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_15_delta_per_day) AS B_15_delta_pd
    FROM
        B_15_delta_per_day
    GROUP BY
        customer_ID
),      
B_15_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_15_delta) AS B_15_delta_mean,
        MAX(B_15_delta) AS B_15_delta_max,
        MIN(B_15_delta) AS B_15_delta_min
    FROM
        B_15_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_15) AS B_15_mean,
        MIN(B_15) AS B_15_min, 
        MAX(B_15) AS B_15_max, 
        SUM(B_15) AS B_15_sum,
        COUNT(B_15) AS B_15_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_15_mean,
        a.B_15_min, 
        a.B_15_max, 
        a.B_15_sum,
        a.B_15_max - a.B_15_min AS B_15_range,
        a.B_15_count,
        f.B_15_first,
        l.B_15_last,
        d.B_15_delta_mean,
        d.B_15_delta_max,
        d.B_15_delta_min,
        pd.B_15_delta_pd,
        cs.B_15_span
    FROM
        aggs a
        LEFT JOIN first_B_15 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_15 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_15_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_15_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_15_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_15_mean, 
    v.B_15_min,
    v.B_15_max, 
    v.B_15_range,
    v.B_15_sum,
    ISNULL(v.B_15_count, 0) AS B_15_count,
    v.B_15_first, 
    v.B_15_last,
    v.B_15_delta_mean,
    v.B_15_delta_max,
    v.B_15_delta_min,
    v.B_15_delta_pd,
    v.B_15_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_11_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_11 
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
    WHERE S_11 IS NOT NULL
    GROUP BY customer_ID
),
first_S_11 AS
(
    SELECT
        f.customer_ID, s.S_11 AS S_11_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_11 AS
(
    SELECT
        f.customer_ID, s.S_11 AS S_11_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_11_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_11_span
    FROM
        first_last
),
S_11_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_11,
        s.S_11 - LAG(s.S_11, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_11_delta
    FROM
        subset s
),
S_11_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_11_delta
    FROM
        S_11_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_11_delta_per_day AS
(
    SELECT
        customer_ID,
        S_11_delta / date_delta AS S_11_delta_per_day
    FROM
        S_11_delta
),
S_11_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_11_delta_per_day) AS S_11_delta_pd
    FROM
        S_11_delta_per_day
    GROUP BY
        customer_ID
),      
S_11_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_11_delta) AS S_11_delta_mean,
        MAX(S_11_delta) AS S_11_delta_max,
        MIN(S_11_delta) AS S_11_delta_min
    FROM
        S_11_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_11) AS S_11_mean,
        MIN(S_11) AS S_11_min, 
        MAX(S_11) AS S_11_max, 
        SUM(S_11) AS S_11_sum,
        COUNT(S_11) AS S_11_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_11_mean,
        a.S_11_min, 
        a.S_11_max, 
        a.S_11_sum,
        a.S_11_max - a.S_11_min AS S_11_range,
        a.S_11_count,
        f.S_11_first,
        l.S_11_last,
        d.S_11_delta_mean,
        d.S_11_delta_max,
        d.S_11_delta_min,
        pd.S_11_delta_pd,
        cs.S_11_span
    FROM
        aggs a
        LEFT JOIN first_S_11 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_11 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_11_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_11_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_11_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_11_mean, 
    v.S_11_min,
    v.S_11_max, 
    v.S_11_range,
    v.S_11_sum,
    ISNULL(v.S_11_count, 0) AS S_11_count,
    v.S_11_first, 
    v.S_11_last,
    v.S_11_delta_mean,
    v.S_11_delta_max,
    v.S_11_delta_min,
    v.S_11_delta_pd,
    v.S_11_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_62_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_62 
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
    WHERE D_62 IS NOT NULL
    GROUP BY customer_ID
),
first_D_62 AS
(
    SELECT
        f.customer_ID, s.D_62 AS D_62_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_62 AS
(
    SELECT
        f.customer_ID, s.D_62 AS D_62_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_62_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_62_span
    FROM
        first_last
),
D_62_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_62,
        s.D_62 - LAG(s.D_62, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_62_delta
    FROM
        subset s
),
D_62_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_62_delta
    FROM
        D_62_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_62_delta_per_day AS
(
    SELECT
        customer_ID,
        D_62_delta / date_delta AS D_62_delta_per_day
    FROM
        D_62_delta
),
D_62_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_62_delta_per_day) AS D_62_delta_pd
    FROM
        D_62_delta_per_day
    GROUP BY
        customer_ID
),      
D_62_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_62_delta) AS D_62_delta_mean,
        MAX(D_62_delta) AS D_62_delta_max,
        MIN(D_62_delta) AS D_62_delta_min
    FROM
        D_62_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_62) AS D_62_mean,
        MIN(D_62) AS D_62_min, 
        MAX(D_62) AS D_62_max, 
        SUM(D_62) AS D_62_sum,
        COUNT(D_62) AS D_62_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_62_mean,
        a.D_62_min, 
        a.D_62_max, 
        a.D_62_sum,
        a.D_62_max - a.D_62_min AS D_62_range,
        a.D_62_count,
        f.D_62_first,
        l.D_62_last,
        d.D_62_delta_mean,
        d.D_62_delta_max,
        d.D_62_delta_min,
        pd.D_62_delta_pd,
        cs.D_62_span
    FROM
        aggs a
        LEFT JOIN first_D_62 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_62 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_62_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_62_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_62_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_62_mean, 
    v.D_62_min,
    v.D_62_max, 
    v.D_62_range,
    v.D_62_sum,
    ISNULL(v.D_62_count, 0) AS D_62_count,
    v.D_62_first, 
    v.D_62_last,
    v.D_62_delta_mean,
    v.D_62_delta_max,
    v.D_62_delta_min,
    v.D_62_delta_pd,
    v.D_62_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_65_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_65 
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
    WHERE D_65 IS NOT NULL
    GROUP BY customer_ID
),
first_D_65 AS
(
    SELECT
        f.customer_ID, s.D_65 AS D_65_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_65 AS
(
    SELECT
        f.customer_ID, s.D_65 AS D_65_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_65_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_65_span
    FROM
        first_last
),
D_65_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_65,
        s.D_65 - LAG(s.D_65, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_65_delta
    FROM
        subset s
),
D_65_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_65_delta
    FROM
        D_65_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_65_delta_per_day AS
(
    SELECT
        customer_ID,
        D_65_delta / date_delta AS D_65_delta_per_day
    FROM
        D_65_delta
),
D_65_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_65_delta_per_day) AS D_65_delta_pd
    FROM
        D_65_delta_per_day
    GROUP BY
        customer_ID
),      
D_65_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_65_delta) AS D_65_delta_mean,
        MAX(D_65_delta) AS D_65_delta_max,
        MIN(D_65_delta) AS D_65_delta_min
    FROM
        D_65_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_65) AS D_65_mean,
        MIN(D_65) AS D_65_min, 
        MAX(D_65) AS D_65_max, 
        SUM(D_65) AS D_65_sum,
        COUNT(D_65) AS D_65_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_65_mean,
        a.D_65_min, 
        a.D_65_max, 
        a.D_65_sum,
        a.D_65_max - a.D_65_min AS D_65_range,
        a.D_65_count,
        f.D_65_first,
        l.D_65_last,
        d.D_65_delta_mean,
        d.D_65_delta_max,
        d.D_65_delta_min,
        pd.D_65_delta_pd,
        cs.D_65_span
    FROM
        aggs a
        LEFT JOIN first_D_65 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_65 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_65_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_65_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_65_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_65_mean, 
    v.D_65_min,
    v.D_65_max, 
    v.D_65_range,
    v.D_65_sum,
    ISNULL(v.D_65_count, 0) AS D_65_count,
    v.D_65_first, 
    v.D_65_last,
    v.D_65_delta_mean,
    v.D_65_delta_max,
    v.D_65_delta_min,
    v.D_65_delta_pd,
    v.D_65_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_16_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_16 
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
GO

CREATE VIEW test_data_B_17_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_17 
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
GO

CREATE VIEW test_data_B_18_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_18 
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
    WHERE B_18 IS NOT NULL
    GROUP BY customer_ID
),
first_B_18 AS
(
    SELECT
        f.customer_ID, s.B_18 AS B_18_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_18 AS
(
    SELECT
        f.customer_ID, s.B_18 AS B_18_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_18_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_18_span
    FROM
        first_last
),
B_18_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_18,
        s.B_18 - LAG(s.B_18, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_18_delta
    FROM
        subset s
),
B_18_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_18_delta
    FROM
        B_18_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_18_delta_per_day AS
(
    SELECT
        customer_ID,
        B_18_delta / date_delta AS B_18_delta_per_day
    FROM
        B_18_delta
),
B_18_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_18_delta_per_day) AS B_18_delta_pd
    FROM
        B_18_delta_per_day
    GROUP BY
        customer_ID
),      
B_18_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_18_delta) AS B_18_delta_mean,
        MAX(B_18_delta) AS B_18_delta_max,
        MIN(B_18_delta) AS B_18_delta_min
    FROM
        B_18_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_18) AS B_18_mean,
        MIN(B_18) AS B_18_min, 
        MAX(B_18) AS B_18_max, 
        SUM(B_18) AS B_18_sum,
        COUNT(B_18) AS B_18_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_18_mean,
        a.B_18_min, 
        a.B_18_max, 
        a.B_18_sum,
        a.B_18_max - a.B_18_min AS B_18_range,
        a.B_18_count,
        f.B_18_first,
        l.B_18_last,
        d.B_18_delta_mean,
        d.B_18_delta_max,
        d.B_18_delta_min,
        pd.B_18_delta_pd,
        cs.B_18_span
    FROM
        aggs a
        LEFT JOIN first_B_18 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_18 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_18_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_18_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_18_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_18_mean, 
    v.B_18_min,
    v.B_18_max, 
    v.B_18_range,
    v.B_18_sum,
    ISNULL(v.B_18_count, 0) AS B_18_count,
    v.B_18_first, 
    v.B_18_last,
    v.B_18_delta_mean,
    v.B_18_delta_max,
    v.B_18_delta_min,
    v.B_18_delta_pd,
    v.B_18_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_19_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_19 
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
    WHERE B_19 IS NOT NULL
    GROUP BY customer_ID
),
first_B_19 AS
(
    SELECT
        f.customer_ID, s.B_19 AS B_19_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_19 AS
(
    SELECT
        f.customer_ID, s.B_19 AS B_19_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_19_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_19_span
    FROM
        first_last
),
B_19_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_19,
        s.B_19 - LAG(s.B_19, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_19_delta
    FROM
        subset s
),
B_19_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_19_delta
    FROM
        B_19_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_19_delta_per_day AS
(
    SELECT
        customer_ID,
        B_19_delta / date_delta AS B_19_delta_per_day
    FROM
        B_19_delta
),
B_19_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_19_delta_per_day) AS B_19_delta_pd
    FROM
        B_19_delta_per_day
    GROUP BY
        customer_ID
),      
B_19_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_19_delta) AS B_19_delta_mean,
        MAX(B_19_delta) AS B_19_delta_max,
        MIN(B_19_delta) AS B_19_delta_min
    FROM
        B_19_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_19) AS B_19_mean,
        MIN(B_19) AS B_19_min, 
        MAX(B_19) AS B_19_max, 
        SUM(B_19) AS B_19_sum,
        COUNT(B_19) AS B_19_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_19_mean,
        a.B_19_min, 
        a.B_19_max, 
        a.B_19_sum,
        a.B_19_max - a.B_19_min AS B_19_range,
        a.B_19_count,
        f.B_19_first,
        l.B_19_last,
        d.B_19_delta_mean,
        d.B_19_delta_max,
        d.B_19_delta_min,
        pd.B_19_delta_pd,
        cs.B_19_span
    FROM
        aggs a
        LEFT JOIN first_B_19 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_19 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_19_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_19_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_19_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_19_mean, 
    v.B_19_min,
    v.B_19_max, 
    v.B_19_range,
    v.B_19_sum,
    ISNULL(v.B_19_count, 0) AS B_19_count,
    v.B_19_first, 
    v.B_19_last,
    v.B_19_delta_mean,
    v.B_19_delta_max,
    v.B_19_delta_min,
    v.B_19_delta_pd,
    v.B_19_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_20_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_20 
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
GO

CREATE VIEW test_data_S_12_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_12 
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
    WHERE S_12 IS NOT NULL
    GROUP BY customer_ID
),
first_S_12 AS
(
    SELECT
        f.customer_ID, s.S_12 AS S_12_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_12 AS
(
    SELECT
        f.customer_ID, s.S_12 AS S_12_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_12_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_12_span
    FROM
        first_last
),
S_12_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_12,
        s.S_12 - LAG(s.S_12, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_12_delta
    FROM
        subset s
),
S_12_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_12_delta
    FROM
        S_12_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_12_delta_per_day AS
(
    SELECT
        customer_ID,
        S_12_delta / date_delta AS S_12_delta_per_day
    FROM
        S_12_delta
),
S_12_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_12_delta_per_day) AS S_12_delta_pd
    FROM
        S_12_delta_per_day
    GROUP BY
        customer_ID
),      
S_12_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_12_delta) AS S_12_delta_mean,
        MAX(S_12_delta) AS S_12_delta_max,
        MIN(S_12_delta) AS S_12_delta_min
    FROM
        S_12_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_12) AS S_12_mean,
        MIN(S_12) AS S_12_min, 
        MAX(S_12) AS S_12_max, 
        SUM(S_12) AS S_12_sum,
        COUNT(S_12) AS S_12_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_12_mean,
        a.S_12_min, 
        a.S_12_max, 
        a.S_12_sum,
        a.S_12_max - a.S_12_min AS S_12_range,
        a.S_12_count,
        f.S_12_first,
        l.S_12_last,
        d.S_12_delta_mean,
        d.S_12_delta_max,
        d.S_12_delta_min,
        pd.S_12_delta_pd,
        cs.S_12_span
    FROM
        aggs a
        LEFT JOIN first_S_12 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_12 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_12_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_12_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_12_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_12_mean, 
    v.S_12_min,
    v.S_12_max, 
    v.S_12_range,
    v.S_12_sum,
    ISNULL(v.S_12_count, 0) AS S_12_count,
    v.S_12_first, 
    v.S_12_last,
    v.S_12_delta_mean,
    v.S_12_delta_max,
    v.S_12_delta_min,
    v.S_12_delta_pd,
    v.S_12_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_6_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_6 
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
    WHERE R_6 IS NOT NULL
    GROUP BY customer_ID
),
first_R_6 AS
(
    SELECT
        f.customer_ID, s.R_6 AS R_6_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_6 AS
(
    SELECT
        f.customer_ID, s.R_6 AS R_6_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_6_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_6_span
    FROM
        first_last
),
R_6_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_6,
        s.R_6 - LAG(s.R_6, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_6_delta
    FROM
        subset s
),
R_6_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_6_delta
    FROM
        R_6_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_6_delta_per_day AS
(
    SELECT
        customer_ID,
        R_6_delta / date_delta AS R_6_delta_per_day
    FROM
        R_6_delta
),
R_6_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_6_delta_per_day) AS R_6_delta_pd
    FROM
        R_6_delta_per_day
    GROUP BY
        customer_ID
),      
R_6_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_6_delta) AS R_6_delta_mean,
        MAX(R_6_delta) AS R_6_delta_max,
        MIN(R_6_delta) AS R_6_delta_min
    FROM
        R_6_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_6) AS R_6_mean,
        MIN(R_6) AS R_6_min, 
        MAX(R_6) AS R_6_max, 
        SUM(R_6) AS R_6_sum,
        COUNT(R_6) AS R_6_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_6_mean,
        a.R_6_min, 
        a.R_6_max, 
        a.R_6_sum,
        a.R_6_max - a.R_6_min AS R_6_range,
        a.R_6_count,
        f.R_6_first,
        l.R_6_last,
        d.R_6_delta_mean,
        d.R_6_delta_max,
        d.R_6_delta_min,
        pd.R_6_delta_pd,
        cs.R_6_span
    FROM
        aggs a
        LEFT JOIN first_R_6 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_6 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_6_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_6_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_6_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_6_mean, 
    v.R_6_min,
    v.R_6_max, 
    v.R_6_range,
    v.R_6_sum,
    ISNULL(v.R_6_count, 0) AS R_6_count,
    v.R_6_first, 
    v.R_6_last,
    v.R_6_delta_mean,
    v.R_6_delta_max,
    v.R_6_delta_min,
    v.R_6_delta_pd,
    v.R_6_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_13_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_13 
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
    WHERE S_13 IS NOT NULL
    GROUP BY customer_ID
),
first_S_13 AS
(
    SELECT
        f.customer_ID, s.S_13 AS S_13_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_13 AS
(
    SELECT
        f.customer_ID, s.S_13 AS S_13_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_13_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_13_span
    FROM
        first_last
),
S_13_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_13,
        s.S_13 - LAG(s.S_13, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_13_delta
    FROM
        subset s
),
S_13_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_13_delta
    FROM
        S_13_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_13_delta_per_day AS
(
    SELECT
        customer_ID,
        S_13_delta / date_delta AS S_13_delta_per_day
    FROM
        S_13_delta
),
S_13_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_13_delta_per_day) AS S_13_delta_pd
    FROM
        S_13_delta_per_day
    GROUP BY
        customer_ID
),      
S_13_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_13_delta) AS S_13_delta_mean,
        MAX(S_13_delta) AS S_13_delta_max,
        MIN(S_13_delta) AS S_13_delta_min
    FROM
        S_13_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_13) AS S_13_mean,
        MIN(S_13) AS S_13_min, 
        MAX(S_13) AS S_13_max, 
        SUM(S_13) AS S_13_sum,
        COUNT(S_13) AS S_13_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_13_mean,
        a.S_13_min, 
        a.S_13_max, 
        a.S_13_sum,
        a.S_13_max - a.S_13_min AS S_13_range,
        a.S_13_count,
        f.S_13_first,
        l.S_13_last,
        d.S_13_delta_mean,
        d.S_13_delta_max,
        d.S_13_delta_min,
        pd.S_13_delta_pd,
        cs.S_13_span
    FROM
        aggs a
        LEFT JOIN first_S_13 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_13 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_13_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_13_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_13_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_13_mean, 
    v.S_13_min,
    v.S_13_max, 
    v.S_13_range,
    v.S_13_sum,
    ISNULL(v.S_13_count, 0) AS S_13_count,
    v.S_13_first, 
    v.S_13_last,
    v.S_13_delta_mean,
    v.S_13_delta_max,
    v.S_13_delta_min,
    v.S_13_delta_pd,
    v.S_13_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_21_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_21 
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
    WHERE B_21 IS NOT NULL
    GROUP BY customer_ID
),
first_B_21 AS
(
    SELECT
        f.customer_ID, s.B_21 AS B_21_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_21 AS
(
    SELECT
        f.customer_ID, s.B_21 AS B_21_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_21_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_21_span
    FROM
        first_last
),
B_21_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_21,
        s.B_21 - LAG(s.B_21, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_21_delta
    FROM
        subset s
),
B_21_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_21_delta
    FROM
        B_21_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_21_delta_per_day AS
(
    SELECT
        customer_ID,
        B_21_delta / date_delta AS B_21_delta_per_day
    FROM
        B_21_delta
),
B_21_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_21_delta_per_day) AS B_21_delta_pd
    FROM
        B_21_delta_per_day
    GROUP BY
        customer_ID
),      
B_21_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_21_delta) AS B_21_delta_mean,
        MAX(B_21_delta) AS B_21_delta_max,
        MIN(B_21_delta) AS B_21_delta_min
    FROM
        B_21_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_21) AS B_21_mean,
        MIN(B_21) AS B_21_min, 
        MAX(B_21) AS B_21_max, 
        SUM(B_21) AS B_21_sum,
        COUNT(B_21) AS B_21_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_21_mean,
        a.B_21_min, 
        a.B_21_max, 
        a.B_21_sum,
        a.B_21_max - a.B_21_min AS B_21_range,
        a.B_21_count,
        f.B_21_first,
        l.B_21_last,
        d.B_21_delta_mean,
        d.B_21_delta_max,
        d.B_21_delta_min,
        pd.B_21_delta_pd,
        cs.B_21_span
    FROM
        aggs a
        LEFT JOIN first_B_21 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_21 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_21_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_21_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_21_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_21_mean, 
    v.B_21_min,
    v.B_21_max, 
    v.B_21_range,
    v.B_21_sum,
    ISNULL(v.B_21_count, 0) AS B_21_count,
    v.B_21_first, 
    v.B_21_last,
    v.B_21_delta_mean,
    v.B_21_delta_max,
    v.B_21_delta_min,
    v.B_21_delta_pd,
    v.B_21_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_69_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_69 
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
    WHERE D_69 IS NOT NULL
    GROUP BY customer_ID
),
first_D_69 AS
(
    SELECT
        f.customer_ID, s.D_69 AS D_69_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_69 AS
(
    SELECT
        f.customer_ID, s.D_69 AS D_69_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_69_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_69_span
    FROM
        first_last
),
D_69_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_69,
        s.D_69 - LAG(s.D_69, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_69_delta
    FROM
        subset s
),
D_69_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_69_delta
    FROM
        D_69_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_69_delta_per_day AS
(
    SELECT
        customer_ID,
        D_69_delta / date_delta AS D_69_delta_per_day
    FROM
        D_69_delta
),
D_69_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_69_delta_per_day) AS D_69_delta_pd
    FROM
        D_69_delta_per_day
    GROUP BY
        customer_ID
),      
D_69_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_69_delta) AS D_69_delta_mean,
        MAX(D_69_delta) AS D_69_delta_max,
        MIN(D_69_delta) AS D_69_delta_min
    FROM
        D_69_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_69) AS D_69_mean,
        MIN(D_69) AS D_69_min, 
        MAX(D_69) AS D_69_max, 
        SUM(D_69) AS D_69_sum,
        COUNT(D_69) AS D_69_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_69_mean,
        a.D_69_min, 
        a.D_69_max, 
        a.D_69_sum,
        a.D_69_max - a.D_69_min AS D_69_range,
        a.D_69_count,
        f.D_69_first,
        l.D_69_last,
        d.D_69_delta_mean,
        d.D_69_delta_max,
        d.D_69_delta_min,
        pd.D_69_delta_pd,
        cs.D_69_span
    FROM
        aggs a
        LEFT JOIN first_D_69 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_69 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_69_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_69_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_69_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_69_mean, 
    v.D_69_min,
    v.D_69_max, 
    v.D_69_range,
    v.D_69_sum,
    ISNULL(v.D_69_count, 0) AS D_69_count,
    v.D_69_first, 
    v.D_69_last,
    v.D_69_delta_mean,
    v.D_69_delta_max,
    v.D_69_delta_min,
    v.D_69_delta_pd,
    v.D_69_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_22_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_22 
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
GO

CREATE VIEW test_data_D_70_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_70 
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
    WHERE D_70 IS NOT NULL
    GROUP BY customer_ID
),
first_D_70 AS
(
    SELECT
        f.customer_ID, s.D_70 AS D_70_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_70 AS
(
    SELECT
        f.customer_ID, s.D_70 AS D_70_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_70_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_70_span
    FROM
        first_last
),
D_70_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_70,
        s.D_70 - LAG(s.D_70, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_70_delta
    FROM
        subset s
),
D_70_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_70_delta
    FROM
        D_70_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_70_delta_per_day AS
(
    SELECT
        customer_ID,
        D_70_delta / date_delta AS D_70_delta_per_day
    FROM
        D_70_delta
),
D_70_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_70_delta_per_day) AS D_70_delta_pd
    FROM
        D_70_delta_per_day
    GROUP BY
        customer_ID
),      
D_70_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_70_delta) AS D_70_delta_mean,
        MAX(D_70_delta) AS D_70_delta_max,
        MIN(D_70_delta) AS D_70_delta_min
    FROM
        D_70_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_70) AS D_70_mean,
        MIN(D_70) AS D_70_min, 
        MAX(D_70) AS D_70_max, 
        SUM(D_70) AS D_70_sum,
        COUNT(D_70) AS D_70_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_70_mean,
        a.D_70_min, 
        a.D_70_max, 
        a.D_70_sum,
        a.D_70_max - a.D_70_min AS D_70_range,
        a.D_70_count,
        f.D_70_first,
        l.D_70_last,
        d.D_70_delta_mean,
        d.D_70_delta_max,
        d.D_70_delta_min,
        pd.D_70_delta_pd,
        cs.D_70_span
    FROM
        aggs a
        LEFT JOIN first_D_70 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_70 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_70_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_70_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_70_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_70_mean, 
    v.D_70_min,
    v.D_70_max, 
    v.D_70_range,
    v.D_70_sum,
    ISNULL(v.D_70_count, 0) AS D_70_count,
    v.D_70_first, 
    v.D_70_last,
    v.D_70_delta_mean,
    v.D_70_delta_max,
    v.D_70_delta_min,
    v.D_70_delta_pd,
    v.D_70_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_71_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_71 
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
    WHERE D_71 IS NOT NULL
    GROUP BY customer_ID
),
first_D_71 AS
(
    SELECT
        f.customer_ID, s.D_71 AS D_71_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_71 AS
(
    SELECT
        f.customer_ID, s.D_71 AS D_71_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_71_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_71_span
    FROM
        first_last
),
D_71_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_71,
        s.D_71 - LAG(s.D_71, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_71_delta
    FROM
        subset s
),
D_71_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_71_delta
    FROM
        D_71_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_71_delta_per_day AS
(
    SELECT
        customer_ID,
        D_71_delta / date_delta AS D_71_delta_per_day
    FROM
        D_71_delta
),
D_71_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_71_delta_per_day) AS D_71_delta_pd
    FROM
        D_71_delta_per_day
    GROUP BY
        customer_ID
),      
D_71_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_71_delta) AS D_71_delta_mean,
        MAX(D_71_delta) AS D_71_delta_max,
        MIN(D_71_delta) AS D_71_delta_min
    FROM
        D_71_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_71) AS D_71_mean,
        MIN(D_71) AS D_71_min, 
        MAX(D_71) AS D_71_max, 
        SUM(D_71) AS D_71_sum,
        COUNT(D_71) AS D_71_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_71_mean,
        a.D_71_min, 
        a.D_71_max, 
        a.D_71_sum,
        a.D_71_max - a.D_71_min AS D_71_range,
        a.D_71_count,
        f.D_71_first,
        l.D_71_last,
        d.D_71_delta_mean,
        d.D_71_delta_max,
        d.D_71_delta_min,
        pd.D_71_delta_pd,
        cs.D_71_span
    FROM
        aggs a
        LEFT JOIN first_D_71 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_71 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_71_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_71_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_71_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_71_mean, 
    v.D_71_min,
    v.D_71_max, 
    v.D_71_range,
    v.D_71_sum,
    ISNULL(v.D_71_count, 0) AS D_71_count,
    v.D_71_first, 
    v.D_71_last,
    v.D_71_delta_mean,
    v.D_71_delta_max,
    v.D_71_delta_min,
    v.D_71_delta_pd,
    v.D_71_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_72_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_72 
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
    WHERE D_72 IS NOT NULL
    GROUP BY customer_ID
),
first_D_72 AS
(
    SELECT
        f.customer_ID, s.D_72 AS D_72_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_72 AS
(
    SELECT
        f.customer_ID, s.D_72 AS D_72_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_72_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_72_span
    FROM
        first_last
),
D_72_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_72,
        s.D_72 - LAG(s.D_72, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_72_delta
    FROM
        subset s
),
D_72_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_72_delta
    FROM
        D_72_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_72_delta_per_day AS
(
    SELECT
        customer_ID,
        D_72_delta / date_delta AS D_72_delta_per_day
    FROM
        D_72_delta
),
D_72_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_72_delta_per_day) AS D_72_delta_pd
    FROM
        D_72_delta_per_day
    GROUP BY
        customer_ID
),      
D_72_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_72_delta) AS D_72_delta_mean,
        MAX(D_72_delta) AS D_72_delta_max,
        MIN(D_72_delta) AS D_72_delta_min
    FROM
        D_72_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_72) AS D_72_mean,
        MIN(D_72) AS D_72_min, 
        MAX(D_72) AS D_72_max, 
        SUM(D_72) AS D_72_sum,
        COUNT(D_72) AS D_72_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_72_mean,
        a.D_72_min, 
        a.D_72_max, 
        a.D_72_sum,
        a.D_72_max - a.D_72_min AS D_72_range,
        a.D_72_count,
        f.D_72_first,
        l.D_72_last,
        d.D_72_delta_mean,
        d.D_72_delta_max,
        d.D_72_delta_min,
        pd.D_72_delta_pd,
        cs.D_72_span
    FROM
        aggs a
        LEFT JOIN first_D_72 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_72 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_72_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_72_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_72_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_72_mean, 
    v.D_72_min,
    v.D_72_max, 
    v.D_72_range,
    v.D_72_sum,
    ISNULL(v.D_72_count, 0) AS D_72_count,
    v.D_72_first, 
    v.D_72_last,
    v.D_72_delta_mean,
    v.D_72_delta_max,
    v.D_72_delta_min,
    v.D_72_delta_pd,
    v.D_72_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_15_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_15 
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
    WHERE S_15 IS NOT NULL
    GROUP BY customer_ID
),
first_S_15 AS
(
    SELECT
        f.customer_ID, s.S_15 AS S_15_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_15 AS
(
    SELECT
        f.customer_ID, s.S_15 AS S_15_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_15_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_15_span
    FROM
        first_last
),
S_15_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_15,
        s.S_15 - LAG(s.S_15, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_15_delta
    FROM
        subset s
),
S_15_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_15_delta
    FROM
        S_15_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_15_delta_per_day AS
(
    SELECT
        customer_ID,
        S_15_delta / date_delta AS S_15_delta_per_day
    FROM
        S_15_delta
),
S_15_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_15_delta_per_day) AS S_15_delta_pd
    FROM
        S_15_delta_per_day
    GROUP BY
        customer_ID
),      
S_15_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_15_delta) AS S_15_delta_mean,
        MAX(S_15_delta) AS S_15_delta_max,
        MIN(S_15_delta) AS S_15_delta_min
    FROM
        S_15_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_15) AS S_15_mean,
        MIN(S_15) AS S_15_min, 
        MAX(S_15) AS S_15_max, 
        SUM(S_15) AS S_15_sum,
        COUNT(S_15) AS S_15_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_15_mean,
        a.S_15_min, 
        a.S_15_max, 
        a.S_15_sum,
        a.S_15_max - a.S_15_min AS S_15_range,
        a.S_15_count,
        f.S_15_first,
        l.S_15_last,
        d.S_15_delta_mean,
        d.S_15_delta_max,
        d.S_15_delta_min,
        pd.S_15_delta_pd,
        cs.S_15_span
    FROM
        aggs a
        LEFT JOIN first_S_15 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_15 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_15_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_15_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_15_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_15_mean, 
    v.S_15_min,
    v.S_15_max, 
    v.S_15_range,
    v.S_15_sum,
    ISNULL(v.S_15_count, 0) AS S_15_count,
    v.S_15_first, 
    v.S_15_last,
    v.S_15_delta_mean,
    v.S_15_delta_max,
    v.S_15_delta_min,
    v.S_15_delta_pd,
    v.S_15_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_23_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_23 
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
    WHERE B_23 IS NOT NULL
    GROUP BY customer_ID
),
first_B_23 AS
(
    SELECT
        f.customer_ID, s.B_23 AS B_23_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_23 AS
(
    SELECT
        f.customer_ID, s.B_23 AS B_23_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_23_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_23_span
    FROM
        first_last
),
B_23_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_23,
        s.B_23 - LAG(s.B_23, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_23_delta
    FROM
        subset s
),
B_23_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_23_delta
    FROM
        B_23_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_23_delta_per_day AS
(
    SELECT
        customer_ID,
        B_23_delta / date_delta AS B_23_delta_per_day
    FROM
        B_23_delta
),
B_23_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_23_delta_per_day) AS B_23_delta_pd
    FROM
        B_23_delta_per_day
    GROUP BY
        customer_ID
),      
B_23_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_23_delta) AS B_23_delta_mean,
        MAX(B_23_delta) AS B_23_delta_max,
        MIN(B_23_delta) AS B_23_delta_min
    FROM
        B_23_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_23) AS B_23_mean,
        MIN(B_23) AS B_23_min, 
        MAX(B_23) AS B_23_max, 
        SUM(B_23) AS B_23_sum,
        COUNT(B_23) AS B_23_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_23_mean,
        a.B_23_min, 
        a.B_23_max, 
        a.B_23_sum,
        a.B_23_max - a.B_23_min AS B_23_range,
        a.B_23_count,
        f.B_23_first,
        l.B_23_last,
        d.B_23_delta_mean,
        d.B_23_delta_max,
        d.B_23_delta_min,
        pd.B_23_delta_pd,
        cs.B_23_span
    FROM
        aggs a
        LEFT JOIN first_B_23 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_23 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_23_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_23_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_23_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_23_mean, 
    v.B_23_min,
    v.B_23_max, 
    v.B_23_range,
    v.B_23_sum,
    ISNULL(v.B_23_count, 0) AS B_23_count,
    v.B_23_first, 
    v.B_23_last,
    v.B_23_delta_mean,
    v.B_23_delta_max,
    v.B_23_delta_min,
    v.B_23_delta_pd,
    v.B_23_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_73_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_73 
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
    WHERE D_73 IS NOT NULL
    GROUP BY customer_ID
),
first_D_73 AS
(
    SELECT
        f.customer_ID, s.D_73 AS D_73_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_73 AS
(
    SELECT
        f.customer_ID, s.D_73 AS D_73_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_73_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_73_span
    FROM
        first_last
),
D_73_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_73,
        s.D_73 - LAG(s.D_73, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_73_delta
    FROM
        subset s
),
D_73_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_73_delta
    FROM
        D_73_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_73_delta_per_day AS
(
    SELECT
        customer_ID,
        D_73_delta / date_delta AS D_73_delta_per_day
    FROM
        D_73_delta
),
D_73_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_73_delta_per_day) AS D_73_delta_pd
    FROM
        D_73_delta_per_day
    GROUP BY
        customer_ID
),      
D_73_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_73_delta) AS D_73_delta_mean,
        MAX(D_73_delta) AS D_73_delta_max,
        MIN(D_73_delta) AS D_73_delta_min
    FROM
        D_73_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_73) AS D_73_mean,
        MIN(D_73) AS D_73_min, 
        MAX(D_73) AS D_73_max, 
        SUM(D_73) AS D_73_sum,
        COUNT(D_73) AS D_73_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_73_mean,
        a.D_73_min, 
        a.D_73_max, 
        a.D_73_sum,
        a.D_73_max - a.D_73_min AS D_73_range,
        a.D_73_count,
        f.D_73_first,
        l.D_73_last,
        d.D_73_delta_mean,
        d.D_73_delta_max,
        d.D_73_delta_min,
        pd.D_73_delta_pd,
        cs.D_73_span
    FROM
        aggs a
        LEFT JOIN first_D_73 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_73 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_73_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_73_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_73_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_73_mean, 
    v.D_73_min,
    v.D_73_max, 
    v.D_73_range,
    v.D_73_sum,
    ISNULL(v.D_73_count, 0) AS D_73_count,
    v.D_73_first, 
    v.D_73_last,
    v.D_73_delta_mean,
    v.D_73_delta_max,
    v.D_73_delta_min,
    v.D_73_delta_pd,
    v.D_73_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_P_4_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.P_4 
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
    WHERE P_4 IS NOT NULL
    GROUP BY customer_ID
),
first_P_4 AS
(
    SELECT
        f.customer_ID, s.P_4 AS P_4_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_P_4 AS
(
    SELECT
        f.customer_ID, s.P_4 AS P_4_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
P_4_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS P_4_span
    FROM
        first_last
),
P_4_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.P_4,
        s.P_4 - LAG(s.P_4, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS P_4_delta
    FROM
        subset s
),
P_4_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.P_4_delta
    FROM
        P_4_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
P_4_delta_per_day AS
(
    SELECT
        customer_ID,
        P_4_delta / date_delta AS P_4_delta_per_day
    FROM
        P_4_delta
),
P_4_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(P_4_delta_per_day) AS P_4_delta_pd
    FROM
        P_4_delta_per_day
    GROUP BY
        customer_ID
),      
P_4_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(P_4_delta) AS P_4_delta_mean,
        MAX(P_4_delta) AS P_4_delta_max,
        MIN(P_4_delta) AS P_4_delta_min
    FROM
        P_4_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(P_4) AS P_4_mean,
        MIN(P_4) AS P_4_min, 
        MAX(P_4) AS P_4_max, 
        SUM(P_4) AS P_4_sum,
        COUNT(P_4) AS P_4_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.P_4_mean,
        a.P_4_min, 
        a.P_4_max, 
        a.P_4_sum,
        a.P_4_max - a.P_4_min AS P_4_range,
        a.P_4_count,
        f.P_4_first,
        l.P_4_last,
        d.P_4_delta_mean,
        d.P_4_delta_max,
        d.P_4_delta_min,
        pd.P_4_delta_pd,
        cs.P_4_span
    FROM
        aggs a
        LEFT JOIN first_P_4 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_P_4 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN P_4_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN P_4_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN P_4_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.P_4_mean, 
    v.P_4_min,
    v.P_4_max, 
    v.P_4_range,
    v.P_4_sum,
    ISNULL(v.P_4_count, 0) AS P_4_count,
    v.P_4_first, 
    v.P_4_last,
    v.P_4_delta_mean,
    v.P_4_delta_max,
    v.P_4_delta_min,
    v.P_4_delta_pd,
    v.P_4_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_74_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_74 
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
    WHERE D_74 IS NOT NULL
    GROUP BY customer_ID
),
first_D_74 AS
(
    SELECT
        f.customer_ID, s.D_74 AS D_74_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_74 AS
(
    SELECT
        f.customer_ID, s.D_74 AS D_74_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_74_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_74_span
    FROM
        first_last
),
D_74_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_74,
        s.D_74 - LAG(s.D_74, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_74_delta
    FROM
        subset s
),
D_74_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_74_delta
    FROM
        D_74_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_74_delta_per_day AS
(
    SELECT
        customer_ID,
        D_74_delta / date_delta AS D_74_delta_per_day
    FROM
        D_74_delta
),
D_74_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_74_delta_per_day) AS D_74_delta_pd
    FROM
        D_74_delta_per_day
    GROUP BY
        customer_ID
),      
D_74_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_74_delta) AS D_74_delta_mean,
        MAX(D_74_delta) AS D_74_delta_max,
        MIN(D_74_delta) AS D_74_delta_min
    FROM
        D_74_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_74) AS D_74_mean,
        MIN(D_74) AS D_74_min, 
        MAX(D_74) AS D_74_max, 
        SUM(D_74) AS D_74_sum,
        COUNT(D_74) AS D_74_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_74_mean,
        a.D_74_min, 
        a.D_74_max, 
        a.D_74_sum,
        a.D_74_max - a.D_74_min AS D_74_range,
        a.D_74_count,
        f.D_74_first,
        l.D_74_last,
        d.D_74_delta_mean,
        d.D_74_delta_max,
        d.D_74_delta_min,
        pd.D_74_delta_pd,
        cs.D_74_span
    FROM
        aggs a
        LEFT JOIN first_D_74 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_74 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_74_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_74_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_74_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_74_mean, 
    v.D_74_min,
    v.D_74_max, 
    v.D_74_range,
    v.D_74_sum,
    ISNULL(v.D_74_count, 0) AS D_74_count,
    v.D_74_first, 
    v.D_74_last,
    v.D_74_delta_mean,
    v.D_74_delta_max,
    v.D_74_delta_min,
    v.D_74_delta_pd,
    v.D_74_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_75_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_75 
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
    WHERE D_75 IS NOT NULL
    GROUP BY customer_ID
),
first_D_75 AS
(
    SELECT
        f.customer_ID, s.D_75 AS D_75_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_75 AS
(
    SELECT
        f.customer_ID, s.D_75 AS D_75_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_75_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_75_span
    FROM
        first_last
),
D_75_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_75,
        s.D_75 - LAG(s.D_75, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_75_delta
    FROM
        subset s
),
D_75_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_75_delta
    FROM
        D_75_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_75_delta_per_day AS
(
    SELECT
        customer_ID,
        D_75_delta / date_delta AS D_75_delta_per_day
    FROM
        D_75_delta
),
D_75_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_75_delta_per_day) AS D_75_delta_pd
    FROM
        D_75_delta_per_day
    GROUP BY
        customer_ID
),      
D_75_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_75_delta) AS D_75_delta_mean,
        MAX(D_75_delta) AS D_75_delta_max,
        MIN(D_75_delta) AS D_75_delta_min
    FROM
        D_75_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_75) AS D_75_mean,
        MIN(D_75) AS D_75_min, 
        MAX(D_75) AS D_75_max, 
        SUM(D_75) AS D_75_sum,
        COUNT(D_75) AS D_75_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_75_mean,
        a.D_75_min, 
        a.D_75_max, 
        a.D_75_sum,
        a.D_75_max - a.D_75_min AS D_75_range,
        a.D_75_count,
        f.D_75_first,
        l.D_75_last,
        d.D_75_delta_mean,
        d.D_75_delta_max,
        d.D_75_delta_min,
        pd.D_75_delta_pd,
        cs.D_75_span
    FROM
        aggs a
        LEFT JOIN first_D_75 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_75 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_75_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_75_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_75_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_75_mean, 
    v.D_75_min,
    v.D_75_max, 
    v.D_75_range,
    v.D_75_sum,
    ISNULL(v.D_75_count, 0) AS D_75_count,
    v.D_75_first, 
    v.D_75_last,
    v.D_75_delta_mean,
    v.D_75_delta_max,
    v.D_75_delta_min,
    v.D_75_delta_pd,
    v.D_75_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_76_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_76 
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
    WHERE D_76 IS NOT NULL
    GROUP BY customer_ID
),
first_D_76 AS
(
    SELECT
        f.customer_ID, s.D_76 AS D_76_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_76 AS
(
    SELECT
        f.customer_ID, s.D_76 AS D_76_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_76_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_76_span
    FROM
        first_last
),
D_76_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_76,
        s.D_76 - LAG(s.D_76, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_76_delta
    FROM
        subset s
),
D_76_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_76_delta
    FROM
        D_76_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_76_delta_per_day AS
(
    SELECT
        customer_ID,
        D_76_delta / date_delta AS D_76_delta_per_day
    FROM
        D_76_delta
),
D_76_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_76_delta_per_day) AS D_76_delta_pd
    FROM
        D_76_delta_per_day
    GROUP BY
        customer_ID
),      
D_76_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_76_delta) AS D_76_delta_mean,
        MAX(D_76_delta) AS D_76_delta_max,
        MIN(D_76_delta) AS D_76_delta_min
    FROM
        D_76_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_76) AS D_76_mean,
        MIN(D_76) AS D_76_min, 
        MAX(D_76) AS D_76_max, 
        SUM(D_76) AS D_76_sum,
        COUNT(D_76) AS D_76_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_76_mean,
        a.D_76_min, 
        a.D_76_max, 
        a.D_76_sum,
        a.D_76_max - a.D_76_min AS D_76_range,
        a.D_76_count,
        f.D_76_first,
        l.D_76_last,
        d.D_76_delta_mean,
        d.D_76_delta_max,
        d.D_76_delta_min,
        pd.D_76_delta_pd,
        cs.D_76_span
    FROM
        aggs a
        LEFT JOIN first_D_76 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_76 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_76_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_76_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_76_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_76_mean, 
    v.D_76_min,
    v.D_76_max, 
    v.D_76_range,
    v.D_76_sum,
    ISNULL(v.D_76_count, 0) AS D_76_count,
    v.D_76_first, 
    v.D_76_last,
    v.D_76_delta_mean,
    v.D_76_delta_max,
    v.D_76_delta_min,
    v.D_76_delta_pd,
    v.D_76_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_24_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_24 
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
    WHERE B_24 IS NOT NULL
    GROUP BY customer_ID
),
first_B_24 AS
(
    SELECT
        f.customer_ID, s.B_24 AS B_24_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_24 AS
(
    SELECT
        f.customer_ID, s.B_24 AS B_24_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_24_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_24_span
    FROM
        first_last
),
B_24_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_24,
        s.B_24 - LAG(s.B_24, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_24_delta
    FROM
        subset s
),
B_24_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_24_delta
    FROM
        B_24_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_24_delta_per_day AS
(
    SELECT
        customer_ID,
        B_24_delta / date_delta AS B_24_delta_per_day
    FROM
        B_24_delta
),
B_24_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_24_delta_per_day) AS B_24_delta_pd
    FROM
        B_24_delta_per_day
    GROUP BY
        customer_ID
),      
B_24_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_24_delta) AS B_24_delta_mean,
        MAX(B_24_delta) AS B_24_delta_max,
        MIN(B_24_delta) AS B_24_delta_min
    FROM
        B_24_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_24) AS B_24_mean,
        MIN(B_24) AS B_24_min, 
        MAX(B_24) AS B_24_max, 
        SUM(B_24) AS B_24_sum,
        COUNT(B_24) AS B_24_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_24_mean,
        a.B_24_min, 
        a.B_24_max, 
        a.B_24_sum,
        a.B_24_max - a.B_24_min AS B_24_range,
        a.B_24_count,
        f.B_24_first,
        l.B_24_last,
        d.B_24_delta_mean,
        d.B_24_delta_max,
        d.B_24_delta_min,
        pd.B_24_delta_pd,
        cs.B_24_span
    FROM
        aggs a
        LEFT JOIN first_B_24 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_24 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_24_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_24_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_24_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_24_mean, 
    v.B_24_min,
    v.B_24_max, 
    v.B_24_range,
    v.B_24_sum,
    ISNULL(v.B_24_count, 0) AS B_24_count,
    v.B_24_first, 
    v.B_24_last,
    v.B_24_delta_mean,
    v.B_24_delta_max,
    v.B_24_delta_min,
    v.B_24_delta_pd,
    v.B_24_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_7_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_7 
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
    WHERE R_7 IS NOT NULL
    GROUP BY customer_ID
),
first_R_7 AS
(
    SELECT
        f.customer_ID, s.R_7 AS R_7_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_7 AS
(
    SELECT
        f.customer_ID, s.R_7 AS R_7_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_7_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_7_span
    FROM
        first_last
),
R_7_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_7,
        s.R_7 - LAG(s.R_7, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_7_delta
    FROM
        subset s
),
R_7_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_7_delta
    FROM
        R_7_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_7_delta_per_day AS
(
    SELECT
        customer_ID,
        R_7_delta / date_delta AS R_7_delta_per_day
    FROM
        R_7_delta
),
R_7_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_7_delta_per_day) AS R_7_delta_pd
    FROM
        R_7_delta_per_day
    GROUP BY
        customer_ID
),      
R_7_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_7_delta) AS R_7_delta_mean,
        MAX(R_7_delta) AS R_7_delta_max,
        MIN(R_7_delta) AS R_7_delta_min
    FROM
        R_7_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_7) AS R_7_mean,
        MIN(R_7) AS R_7_min, 
        MAX(R_7) AS R_7_max, 
        SUM(R_7) AS R_7_sum,
        COUNT(R_7) AS R_7_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_7_mean,
        a.R_7_min, 
        a.R_7_max, 
        a.R_7_sum,
        a.R_7_max - a.R_7_min AS R_7_range,
        a.R_7_count,
        f.R_7_first,
        l.R_7_last,
        d.R_7_delta_mean,
        d.R_7_delta_max,
        d.R_7_delta_min,
        pd.R_7_delta_pd,
        cs.R_7_span
    FROM
        aggs a
        LEFT JOIN first_R_7 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_7 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_7_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_7_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_7_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_7_mean, 
    v.R_7_min,
    v.R_7_max, 
    v.R_7_range,
    v.R_7_sum,
    ISNULL(v.R_7_count, 0) AS R_7_count,
    v.R_7_first, 
    v.R_7_last,
    v.R_7_delta_mean,
    v.R_7_delta_max,
    v.R_7_delta_min,
    v.R_7_delta_pd,
    v.R_7_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_77_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_77 
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
    WHERE D_77 IS NOT NULL
    GROUP BY customer_ID
),
first_D_77 AS
(
    SELECT
        f.customer_ID, s.D_77 AS D_77_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_77 AS
(
    SELECT
        f.customer_ID, s.D_77 AS D_77_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_77_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_77_span
    FROM
        first_last
),
D_77_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_77,
        s.D_77 - LAG(s.D_77, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_77_delta
    FROM
        subset s
),
D_77_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_77_delta
    FROM
        D_77_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_77_delta_per_day AS
(
    SELECT
        customer_ID,
        D_77_delta / date_delta AS D_77_delta_per_day
    FROM
        D_77_delta
),
D_77_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_77_delta_per_day) AS D_77_delta_pd
    FROM
        D_77_delta_per_day
    GROUP BY
        customer_ID
),      
D_77_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_77_delta) AS D_77_delta_mean,
        MAX(D_77_delta) AS D_77_delta_max,
        MIN(D_77_delta) AS D_77_delta_min
    FROM
        D_77_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_77) AS D_77_mean,
        MIN(D_77) AS D_77_min, 
        MAX(D_77) AS D_77_max, 
        SUM(D_77) AS D_77_sum,
        COUNT(D_77) AS D_77_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_77_mean,
        a.D_77_min, 
        a.D_77_max, 
        a.D_77_sum,
        a.D_77_max - a.D_77_min AS D_77_range,
        a.D_77_count,
        f.D_77_first,
        l.D_77_last,
        d.D_77_delta_mean,
        d.D_77_delta_max,
        d.D_77_delta_min,
        pd.D_77_delta_pd,
        cs.D_77_span
    FROM
        aggs a
        LEFT JOIN first_D_77 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_77 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_77_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_77_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_77_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_77_mean, 
    v.D_77_min,
    v.D_77_max, 
    v.D_77_range,
    v.D_77_sum,
    ISNULL(v.D_77_count, 0) AS D_77_count,
    v.D_77_first, 
    v.D_77_last,
    v.D_77_delta_mean,
    v.D_77_delta_max,
    v.D_77_delta_min,
    v.D_77_delta_pd,
    v.D_77_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_25_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_25 
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
    WHERE B_25 IS NOT NULL
    GROUP BY customer_ID
),
first_B_25 AS
(
    SELECT
        f.customer_ID, s.B_25 AS B_25_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_25 AS
(
    SELECT
        f.customer_ID, s.B_25 AS B_25_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_25_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_25_span
    FROM
        first_last
),
B_25_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_25,
        s.B_25 - LAG(s.B_25, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_25_delta
    FROM
        subset s
),
B_25_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_25_delta
    FROM
        B_25_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_25_delta_per_day AS
(
    SELECT
        customer_ID,
        B_25_delta / date_delta AS B_25_delta_per_day
    FROM
        B_25_delta
),
B_25_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_25_delta_per_day) AS B_25_delta_pd
    FROM
        B_25_delta_per_day
    GROUP BY
        customer_ID
),      
B_25_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_25_delta) AS B_25_delta_mean,
        MAX(B_25_delta) AS B_25_delta_max,
        MIN(B_25_delta) AS B_25_delta_min
    FROM
        B_25_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_25) AS B_25_mean,
        MIN(B_25) AS B_25_min, 
        MAX(B_25) AS B_25_max, 
        SUM(B_25) AS B_25_sum,
        COUNT(B_25) AS B_25_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_25_mean,
        a.B_25_min, 
        a.B_25_max, 
        a.B_25_sum,
        a.B_25_max - a.B_25_min AS B_25_range,
        a.B_25_count,
        f.B_25_first,
        l.B_25_last,
        d.B_25_delta_mean,
        d.B_25_delta_max,
        d.B_25_delta_min,
        pd.B_25_delta_pd,
        cs.B_25_span
    FROM
        aggs a
        LEFT JOIN first_B_25 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_25 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_25_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_25_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_25_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_25_mean, 
    v.B_25_min,
    v.B_25_max, 
    v.B_25_range,
    v.B_25_sum,
    ISNULL(v.B_25_count, 0) AS B_25_count,
    v.B_25_first, 
    v.B_25_last,
    v.B_25_delta_mean,
    v.B_25_delta_max,
    v.B_25_delta_min,
    v.B_25_delta_pd,
    v.B_25_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_26_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_26 
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
    WHERE B_26 IS NOT NULL
    GROUP BY customer_ID
),
first_B_26 AS
(
    SELECT
        f.customer_ID, s.B_26 AS B_26_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_26 AS
(
    SELECT
        f.customer_ID, s.B_26 AS B_26_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_26_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_26_span
    FROM
        first_last
),
B_26_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_26,
        s.B_26 - LAG(s.B_26, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_26_delta
    FROM
        subset s
),
B_26_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_26_delta
    FROM
        B_26_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_26_delta_per_day AS
(
    SELECT
        customer_ID,
        B_26_delta / date_delta AS B_26_delta_per_day
    FROM
        B_26_delta
),
B_26_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_26_delta_per_day) AS B_26_delta_pd
    FROM
        B_26_delta_per_day
    GROUP BY
        customer_ID
),      
B_26_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_26_delta) AS B_26_delta_mean,
        MAX(B_26_delta) AS B_26_delta_max,
        MIN(B_26_delta) AS B_26_delta_min
    FROM
        B_26_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_26) AS B_26_mean,
        MIN(B_26) AS B_26_min, 
        MAX(B_26) AS B_26_max, 
        SUM(B_26) AS B_26_sum,
        COUNT(B_26) AS B_26_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_26_mean,
        a.B_26_min, 
        a.B_26_max, 
        a.B_26_sum,
        a.B_26_max - a.B_26_min AS B_26_range,
        a.B_26_count,
        f.B_26_first,
        l.B_26_last,
        d.B_26_delta_mean,
        d.B_26_delta_max,
        d.B_26_delta_min,
        pd.B_26_delta_pd,
        cs.B_26_span
    FROM
        aggs a
        LEFT JOIN first_B_26 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_26 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_26_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_26_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_26_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_26_mean, 
    v.B_26_min,
    v.B_26_max, 
    v.B_26_range,
    v.B_26_sum,
    ISNULL(v.B_26_count, 0) AS B_26_count,
    v.B_26_first, 
    v.B_26_last,
    v.B_26_delta_mean,
    v.B_26_delta_max,
    v.B_26_delta_min,
    v.B_26_delta_pd,
    v.B_26_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_78_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_78 
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
    WHERE D_78 IS NOT NULL
    GROUP BY customer_ID
),
first_D_78 AS
(
    SELECT
        f.customer_ID, s.D_78 AS D_78_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_78 AS
(
    SELECT
        f.customer_ID, s.D_78 AS D_78_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_78_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_78_span
    FROM
        first_last
),
D_78_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_78,
        s.D_78 - LAG(s.D_78, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_78_delta
    FROM
        subset s
),
D_78_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_78_delta
    FROM
        D_78_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_78_delta_per_day AS
(
    SELECT
        customer_ID,
        D_78_delta / date_delta AS D_78_delta_per_day
    FROM
        D_78_delta
),
D_78_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_78_delta_per_day) AS D_78_delta_pd
    FROM
        D_78_delta_per_day
    GROUP BY
        customer_ID
),      
D_78_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_78_delta) AS D_78_delta_mean,
        MAX(D_78_delta) AS D_78_delta_max,
        MIN(D_78_delta) AS D_78_delta_min
    FROM
        D_78_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_78) AS D_78_mean,
        MIN(D_78) AS D_78_min, 
        MAX(D_78) AS D_78_max, 
        SUM(D_78) AS D_78_sum,
        COUNT(D_78) AS D_78_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_78_mean,
        a.D_78_min, 
        a.D_78_max, 
        a.D_78_sum,
        a.D_78_max - a.D_78_min AS D_78_range,
        a.D_78_count,
        f.D_78_first,
        l.D_78_last,
        d.D_78_delta_mean,
        d.D_78_delta_max,
        d.D_78_delta_min,
        pd.D_78_delta_pd,
        cs.D_78_span
    FROM
        aggs a
        LEFT JOIN first_D_78 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_78 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_78_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_78_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_78_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_78_mean, 
    v.D_78_min,
    v.D_78_max, 
    v.D_78_range,
    v.D_78_sum,
    ISNULL(v.D_78_count, 0) AS D_78_count,
    v.D_78_first, 
    v.D_78_last,
    v.D_78_delta_mean,
    v.D_78_delta_max,
    v.D_78_delta_min,
    v.D_78_delta_pd,
    v.D_78_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_79_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_79 
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
    WHERE D_79 IS NOT NULL
    GROUP BY customer_ID
),
first_D_79 AS
(
    SELECT
        f.customer_ID, s.D_79 AS D_79_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_79 AS
(
    SELECT
        f.customer_ID, s.D_79 AS D_79_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_79_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_79_span
    FROM
        first_last
),
D_79_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_79,
        s.D_79 - LAG(s.D_79, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_79_delta
    FROM
        subset s
),
D_79_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_79_delta
    FROM
        D_79_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_79_delta_per_day AS
(
    SELECT
        customer_ID,
        D_79_delta / date_delta AS D_79_delta_per_day
    FROM
        D_79_delta
),
D_79_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_79_delta_per_day) AS D_79_delta_pd
    FROM
        D_79_delta_per_day
    GROUP BY
        customer_ID
),      
D_79_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_79_delta) AS D_79_delta_mean,
        MAX(D_79_delta) AS D_79_delta_max,
        MIN(D_79_delta) AS D_79_delta_min
    FROM
        D_79_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_79) AS D_79_mean,
        MIN(D_79) AS D_79_min, 
        MAX(D_79) AS D_79_max, 
        SUM(D_79) AS D_79_sum,
        COUNT(D_79) AS D_79_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_79_mean,
        a.D_79_min, 
        a.D_79_max, 
        a.D_79_sum,
        a.D_79_max - a.D_79_min AS D_79_range,
        a.D_79_count,
        f.D_79_first,
        l.D_79_last,
        d.D_79_delta_mean,
        d.D_79_delta_max,
        d.D_79_delta_min,
        pd.D_79_delta_pd,
        cs.D_79_span
    FROM
        aggs a
        LEFT JOIN first_D_79 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_79 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_79_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_79_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_79_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_79_mean, 
    v.D_79_min,
    v.D_79_max, 
    v.D_79_range,
    v.D_79_sum,
    ISNULL(v.D_79_count, 0) AS D_79_count,
    v.D_79_first, 
    v.D_79_last,
    v.D_79_delta_mean,
    v.D_79_delta_max,
    v.D_79_delta_min,
    v.D_79_delta_pd,
    v.D_79_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_8_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_8 
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
    WHERE R_8 IS NOT NULL
    GROUP BY customer_ID
),
first_R_8 AS
(
    SELECT
        f.customer_ID, s.R_8 AS R_8_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_8 AS
(
    SELECT
        f.customer_ID, s.R_8 AS R_8_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_8_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_8_span
    FROM
        first_last
),
R_8_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_8,
        s.R_8 - LAG(s.R_8, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_8_delta
    FROM
        subset s
),
R_8_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_8_delta
    FROM
        R_8_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_8_delta_per_day AS
(
    SELECT
        customer_ID,
        R_8_delta / date_delta AS R_8_delta_per_day
    FROM
        R_8_delta
),
R_8_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_8_delta_per_day) AS R_8_delta_pd
    FROM
        R_8_delta_per_day
    GROUP BY
        customer_ID
),      
R_8_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_8_delta) AS R_8_delta_mean,
        MAX(R_8_delta) AS R_8_delta_max,
        MIN(R_8_delta) AS R_8_delta_min
    FROM
        R_8_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_8) AS R_8_mean,
        MIN(R_8) AS R_8_min, 
        MAX(R_8) AS R_8_max, 
        SUM(R_8) AS R_8_sum,
        COUNT(R_8) AS R_8_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_8_mean,
        a.R_8_min, 
        a.R_8_max, 
        a.R_8_sum,
        a.R_8_max - a.R_8_min AS R_8_range,
        a.R_8_count,
        f.R_8_first,
        l.R_8_last,
        d.R_8_delta_mean,
        d.R_8_delta_max,
        d.R_8_delta_min,
        pd.R_8_delta_pd,
        cs.R_8_span
    FROM
        aggs a
        LEFT JOIN first_R_8 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_8 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_8_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_8_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_8_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_8_mean, 
    v.R_8_min,
    v.R_8_max, 
    v.R_8_range,
    v.R_8_sum,
    ISNULL(v.R_8_count, 0) AS R_8_count,
    v.R_8_first, 
    v.R_8_last,
    v.R_8_delta_mean,
    v.R_8_delta_max,
    v.R_8_delta_min,
    v.R_8_delta_pd,
    v.R_8_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_9_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_9 
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
    WHERE R_9 IS NOT NULL
    GROUP BY customer_ID
),
first_R_9 AS
(
    SELECT
        f.customer_ID, s.R_9 AS R_9_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_9 AS
(
    SELECT
        f.customer_ID, s.R_9 AS R_9_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_9_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_9_span
    FROM
        first_last
),
R_9_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_9,
        s.R_9 - LAG(s.R_9, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_9_delta
    FROM
        subset s
),
R_9_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_9_delta
    FROM
        R_9_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_9_delta_per_day AS
(
    SELECT
        customer_ID,
        R_9_delta / date_delta AS R_9_delta_per_day
    FROM
        R_9_delta
),
R_9_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_9_delta_per_day) AS R_9_delta_pd
    FROM
        R_9_delta_per_day
    GROUP BY
        customer_ID
),      
R_9_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_9_delta) AS R_9_delta_mean,
        MAX(R_9_delta) AS R_9_delta_max,
        MIN(R_9_delta) AS R_9_delta_min
    FROM
        R_9_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_9) AS R_9_mean,
        MIN(R_9) AS R_9_min, 
        MAX(R_9) AS R_9_max, 
        SUM(R_9) AS R_9_sum,
        COUNT(R_9) AS R_9_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_9_mean,
        a.R_9_min, 
        a.R_9_max, 
        a.R_9_sum,
        a.R_9_max - a.R_9_min AS R_9_range,
        a.R_9_count,
        f.R_9_first,
        l.R_9_last,
        d.R_9_delta_mean,
        d.R_9_delta_max,
        d.R_9_delta_min,
        pd.R_9_delta_pd,
        cs.R_9_span
    FROM
        aggs a
        LEFT JOIN first_R_9 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_9 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_9_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_9_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_9_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_9_mean, 
    v.R_9_min,
    v.R_9_max, 
    v.R_9_range,
    v.R_9_sum,
    ISNULL(v.R_9_count, 0) AS R_9_count,
    v.R_9_first, 
    v.R_9_last,
    v.R_9_delta_mean,
    v.R_9_delta_max,
    v.R_9_delta_min,
    v.R_9_delta_pd,
    v.R_9_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_16_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_16 
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
    WHERE S_16 IS NOT NULL
    GROUP BY customer_ID
),
first_S_16 AS
(
    SELECT
        f.customer_ID, s.S_16 AS S_16_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_16 AS
(
    SELECT
        f.customer_ID, s.S_16 AS S_16_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_16_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_16_span
    FROM
        first_last
),
S_16_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_16,
        s.S_16 - LAG(s.S_16, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_16_delta
    FROM
        subset s
),
S_16_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_16_delta
    FROM
        S_16_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_16_delta_per_day AS
(
    SELECT
        customer_ID,
        S_16_delta / date_delta AS S_16_delta_per_day
    FROM
        S_16_delta
),
S_16_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_16_delta_per_day) AS S_16_delta_pd
    FROM
        S_16_delta_per_day
    GROUP BY
        customer_ID
),      
S_16_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_16_delta) AS S_16_delta_mean,
        MAX(S_16_delta) AS S_16_delta_max,
        MIN(S_16_delta) AS S_16_delta_min
    FROM
        S_16_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_16) AS S_16_mean,
        MIN(S_16) AS S_16_min, 
        MAX(S_16) AS S_16_max, 
        SUM(S_16) AS S_16_sum,
        COUNT(S_16) AS S_16_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_16_mean,
        a.S_16_min, 
        a.S_16_max, 
        a.S_16_sum,
        a.S_16_max - a.S_16_min AS S_16_range,
        a.S_16_count,
        f.S_16_first,
        l.S_16_last,
        d.S_16_delta_mean,
        d.S_16_delta_max,
        d.S_16_delta_min,
        pd.S_16_delta_pd,
        cs.S_16_span
    FROM
        aggs a
        LEFT JOIN first_S_16 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_16 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_16_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_16_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_16_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_16_mean, 
    v.S_16_min,
    v.S_16_max, 
    v.S_16_range,
    v.S_16_sum,
    ISNULL(v.S_16_count, 0) AS S_16_count,
    v.S_16_first, 
    v.S_16_last,
    v.S_16_delta_mean,
    v.S_16_delta_max,
    v.S_16_delta_min,
    v.S_16_delta_pd,
    v.S_16_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_80_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_80 
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
    WHERE D_80 IS NOT NULL
    GROUP BY customer_ID
),
first_D_80 AS
(
    SELECT
        f.customer_ID, s.D_80 AS D_80_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_80 AS
(
    SELECT
        f.customer_ID, s.D_80 AS D_80_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_80_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_80_span
    FROM
        first_last
),
D_80_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_80,
        s.D_80 - LAG(s.D_80, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_80_delta
    FROM
        subset s
),
D_80_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_80_delta
    FROM
        D_80_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_80_delta_per_day AS
(
    SELECT
        customer_ID,
        D_80_delta / date_delta AS D_80_delta_per_day
    FROM
        D_80_delta
),
D_80_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_80_delta_per_day) AS D_80_delta_pd
    FROM
        D_80_delta_per_day
    GROUP BY
        customer_ID
),      
D_80_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_80_delta) AS D_80_delta_mean,
        MAX(D_80_delta) AS D_80_delta_max,
        MIN(D_80_delta) AS D_80_delta_min
    FROM
        D_80_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_80) AS D_80_mean,
        MIN(D_80) AS D_80_min, 
        MAX(D_80) AS D_80_max, 
        SUM(D_80) AS D_80_sum,
        COUNT(D_80) AS D_80_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_80_mean,
        a.D_80_min, 
        a.D_80_max, 
        a.D_80_sum,
        a.D_80_max - a.D_80_min AS D_80_range,
        a.D_80_count,
        f.D_80_first,
        l.D_80_last,
        d.D_80_delta_mean,
        d.D_80_delta_max,
        d.D_80_delta_min,
        pd.D_80_delta_pd,
        cs.D_80_span
    FROM
        aggs a
        LEFT JOIN first_D_80 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_80 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_80_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_80_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_80_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_80_mean, 
    v.D_80_min,
    v.D_80_max, 
    v.D_80_range,
    v.D_80_sum,
    ISNULL(v.D_80_count, 0) AS D_80_count,
    v.D_80_first, 
    v.D_80_last,
    v.D_80_delta_mean,
    v.D_80_delta_max,
    v.D_80_delta_min,
    v.D_80_delta_pd,
    v.D_80_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_10_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_10 
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
    WHERE R_10 IS NOT NULL
    GROUP BY customer_ID
),
first_R_10 AS
(
    SELECT
        f.customer_ID, s.R_10 AS R_10_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_10 AS
(
    SELECT
        f.customer_ID, s.R_10 AS R_10_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_10_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_10_span
    FROM
        first_last
),
R_10_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_10,
        s.R_10 - LAG(s.R_10, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_10_delta
    FROM
        subset s
),
R_10_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_10_delta
    FROM
        R_10_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_10_delta_per_day AS
(
    SELECT
        customer_ID,
        R_10_delta / date_delta AS R_10_delta_per_day
    FROM
        R_10_delta
),
R_10_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_10_delta_per_day) AS R_10_delta_pd
    FROM
        R_10_delta_per_day
    GROUP BY
        customer_ID
),      
R_10_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_10_delta) AS R_10_delta_mean,
        MAX(R_10_delta) AS R_10_delta_max,
        MIN(R_10_delta) AS R_10_delta_min
    FROM
        R_10_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_10) AS R_10_mean,
        MIN(R_10) AS R_10_min, 
        MAX(R_10) AS R_10_max, 
        SUM(R_10) AS R_10_sum,
        COUNT(R_10) AS R_10_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_10_mean,
        a.R_10_min, 
        a.R_10_max, 
        a.R_10_sum,
        a.R_10_max - a.R_10_min AS R_10_range,
        a.R_10_count,
        f.R_10_first,
        l.R_10_last,
        d.R_10_delta_mean,
        d.R_10_delta_max,
        d.R_10_delta_min,
        pd.R_10_delta_pd,
        cs.R_10_span
    FROM
        aggs a
        LEFT JOIN first_R_10 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_10 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_10_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_10_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_10_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_10_mean, 
    v.R_10_min,
    v.R_10_max, 
    v.R_10_range,
    v.R_10_sum,
    ISNULL(v.R_10_count, 0) AS R_10_count,
    v.R_10_first, 
    v.R_10_last,
    v.R_10_delta_mean,
    v.R_10_delta_max,
    v.R_10_delta_min,
    v.R_10_delta_pd,
    v.R_10_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_11_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_11 
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
    WHERE R_11 IS NOT NULL
    GROUP BY customer_ID
),
first_R_11 AS
(
    SELECT
        f.customer_ID, s.R_11 AS R_11_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_11 AS
(
    SELECT
        f.customer_ID, s.R_11 AS R_11_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_11_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_11_span
    FROM
        first_last
),
R_11_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_11,
        s.R_11 - LAG(s.R_11, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_11_delta
    FROM
        subset s
),
R_11_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_11_delta
    FROM
        R_11_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_11_delta_per_day AS
(
    SELECT
        customer_ID,
        R_11_delta / date_delta AS R_11_delta_per_day
    FROM
        R_11_delta
),
R_11_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_11_delta_per_day) AS R_11_delta_pd
    FROM
        R_11_delta_per_day
    GROUP BY
        customer_ID
),      
R_11_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_11_delta) AS R_11_delta_mean,
        MAX(R_11_delta) AS R_11_delta_max,
        MIN(R_11_delta) AS R_11_delta_min
    FROM
        R_11_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_11) AS R_11_mean,
        MIN(R_11) AS R_11_min, 
        MAX(R_11) AS R_11_max, 
        SUM(R_11) AS R_11_sum,
        COUNT(R_11) AS R_11_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_11_mean,
        a.R_11_min, 
        a.R_11_max, 
        a.R_11_sum,
        a.R_11_max - a.R_11_min AS R_11_range,
        a.R_11_count,
        f.R_11_first,
        l.R_11_last,
        d.R_11_delta_mean,
        d.R_11_delta_max,
        d.R_11_delta_min,
        pd.R_11_delta_pd,
        cs.R_11_span
    FROM
        aggs a
        LEFT JOIN first_R_11 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_11 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_11_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_11_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_11_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_11_mean, 
    v.R_11_min,
    v.R_11_max, 
    v.R_11_range,
    v.R_11_sum,
    ISNULL(v.R_11_count, 0) AS R_11_count,
    v.R_11_first, 
    v.R_11_last,
    v.R_11_delta_mean,
    v.R_11_delta_max,
    v.R_11_delta_min,
    v.R_11_delta_pd,
    v.R_11_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_27_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_27 
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
    WHERE B_27 IS NOT NULL
    GROUP BY customer_ID
),
first_B_27 AS
(
    SELECT
        f.customer_ID, s.B_27 AS B_27_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_27 AS
(
    SELECT
        f.customer_ID, s.B_27 AS B_27_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_27_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_27_span
    FROM
        first_last
),
B_27_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_27,
        s.B_27 - LAG(s.B_27, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_27_delta
    FROM
        subset s
),
B_27_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_27_delta
    FROM
        B_27_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_27_delta_per_day AS
(
    SELECT
        customer_ID,
        B_27_delta / date_delta AS B_27_delta_per_day
    FROM
        B_27_delta
),
B_27_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_27_delta_per_day) AS B_27_delta_pd
    FROM
        B_27_delta_per_day
    GROUP BY
        customer_ID
),      
B_27_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_27_delta) AS B_27_delta_mean,
        MAX(B_27_delta) AS B_27_delta_max,
        MIN(B_27_delta) AS B_27_delta_min
    FROM
        B_27_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_27) AS B_27_mean,
        MIN(B_27) AS B_27_min, 
        MAX(B_27) AS B_27_max, 
        SUM(B_27) AS B_27_sum,
        COUNT(B_27) AS B_27_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_27_mean,
        a.B_27_min, 
        a.B_27_max, 
        a.B_27_sum,
        a.B_27_max - a.B_27_min AS B_27_range,
        a.B_27_count,
        f.B_27_first,
        l.B_27_last,
        d.B_27_delta_mean,
        d.B_27_delta_max,
        d.B_27_delta_min,
        pd.B_27_delta_pd,
        cs.B_27_span
    FROM
        aggs a
        LEFT JOIN first_B_27 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_27 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_27_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_27_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_27_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_27_mean, 
    v.B_27_min,
    v.B_27_max, 
    v.B_27_range,
    v.B_27_sum,
    ISNULL(v.B_27_count, 0) AS B_27_count,
    v.B_27_first, 
    v.B_27_last,
    v.B_27_delta_mean,
    v.B_27_delta_max,
    v.B_27_delta_min,
    v.B_27_delta_pd,
    v.B_27_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_81_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_81 
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
    WHERE D_81 IS NOT NULL
    GROUP BY customer_ID
),
first_D_81 AS
(
    SELECT
        f.customer_ID, s.D_81 AS D_81_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_81 AS
(
    SELECT
        f.customer_ID, s.D_81 AS D_81_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_81_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_81_span
    FROM
        first_last
),
D_81_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_81,
        s.D_81 - LAG(s.D_81, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_81_delta
    FROM
        subset s
),
D_81_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_81_delta
    FROM
        D_81_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_81_delta_per_day AS
(
    SELECT
        customer_ID,
        D_81_delta / date_delta AS D_81_delta_per_day
    FROM
        D_81_delta
),
D_81_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_81_delta_per_day) AS D_81_delta_pd
    FROM
        D_81_delta_per_day
    GROUP BY
        customer_ID
),      
D_81_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_81_delta) AS D_81_delta_mean,
        MAX(D_81_delta) AS D_81_delta_max,
        MIN(D_81_delta) AS D_81_delta_min
    FROM
        D_81_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_81) AS D_81_mean,
        MIN(D_81) AS D_81_min, 
        MAX(D_81) AS D_81_max, 
        SUM(D_81) AS D_81_sum,
        COUNT(D_81) AS D_81_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_81_mean,
        a.D_81_min, 
        a.D_81_max, 
        a.D_81_sum,
        a.D_81_max - a.D_81_min AS D_81_range,
        a.D_81_count,
        f.D_81_first,
        l.D_81_last,
        d.D_81_delta_mean,
        d.D_81_delta_max,
        d.D_81_delta_min,
        pd.D_81_delta_pd,
        cs.D_81_span
    FROM
        aggs a
        LEFT JOIN first_D_81 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_81 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_81_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_81_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_81_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_81_mean, 
    v.D_81_min,
    v.D_81_max, 
    v.D_81_range,
    v.D_81_sum,
    ISNULL(v.D_81_count, 0) AS D_81_count,
    v.D_81_first, 
    v.D_81_last,
    v.D_81_delta_mean,
    v.D_81_delta_max,
    v.D_81_delta_min,
    v.D_81_delta_pd,
    v.D_81_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_82_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_82 
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
    WHERE D_82 IS NOT NULL
    GROUP BY customer_ID
),
first_D_82 AS
(
    SELECT
        f.customer_ID, s.D_82 AS D_82_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_82 AS
(
    SELECT
        f.customer_ID, s.D_82 AS D_82_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_82_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_82_span
    FROM
        first_last
),
D_82_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_82,
        s.D_82 - LAG(s.D_82, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_82_delta
    FROM
        subset s
),
D_82_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_82_delta
    FROM
        D_82_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_82_delta_per_day AS
(
    SELECT
        customer_ID,
        D_82_delta / date_delta AS D_82_delta_per_day
    FROM
        D_82_delta
),
D_82_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_82_delta_per_day) AS D_82_delta_pd
    FROM
        D_82_delta_per_day
    GROUP BY
        customer_ID
),      
D_82_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_82_delta) AS D_82_delta_mean,
        MAX(D_82_delta) AS D_82_delta_max,
        MIN(D_82_delta) AS D_82_delta_min
    FROM
        D_82_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_82) AS D_82_mean,
        MIN(D_82) AS D_82_min, 
        MAX(D_82) AS D_82_max, 
        SUM(D_82) AS D_82_sum,
        COUNT(D_82) AS D_82_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_82_mean,
        a.D_82_min, 
        a.D_82_max, 
        a.D_82_sum,
        a.D_82_max - a.D_82_min AS D_82_range,
        a.D_82_count,
        f.D_82_first,
        l.D_82_last,
        d.D_82_delta_mean,
        d.D_82_delta_max,
        d.D_82_delta_min,
        pd.D_82_delta_pd,
        cs.D_82_span
    FROM
        aggs a
        LEFT JOIN first_D_82 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_82 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_82_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_82_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_82_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_82_mean, 
    v.D_82_min,
    v.D_82_max, 
    v.D_82_range,
    v.D_82_sum,
    ISNULL(v.D_82_count, 0) AS D_82_count,
    v.D_82_first, 
    v.D_82_last,
    v.D_82_delta_mean,
    v.D_82_delta_max,
    v.D_82_delta_min,
    v.D_82_delta_pd,
    v.D_82_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_17_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_17 
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
    WHERE S_17 IS NOT NULL
    GROUP BY customer_ID
),
first_S_17 AS
(
    SELECT
        f.customer_ID, s.S_17 AS S_17_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_17 AS
(
    SELECT
        f.customer_ID, s.S_17 AS S_17_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_17_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_17_span
    FROM
        first_last
),
S_17_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_17,
        s.S_17 - LAG(s.S_17, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_17_delta
    FROM
        subset s
),
S_17_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_17_delta
    FROM
        S_17_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_17_delta_per_day AS
(
    SELECT
        customer_ID,
        S_17_delta / date_delta AS S_17_delta_per_day
    FROM
        S_17_delta
),
S_17_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_17_delta_per_day) AS S_17_delta_pd
    FROM
        S_17_delta_per_day
    GROUP BY
        customer_ID
),      
S_17_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_17_delta) AS S_17_delta_mean,
        MAX(S_17_delta) AS S_17_delta_max,
        MIN(S_17_delta) AS S_17_delta_min
    FROM
        S_17_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_17) AS S_17_mean,
        MIN(S_17) AS S_17_min, 
        MAX(S_17) AS S_17_max, 
        SUM(S_17) AS S_17_sum,
        COUNT(S_17) AS S_17_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_17_mean,
        a.S_17_min, 
        a.S_17_max, 
        a.S_17_sum,
        a.S_17_max - a.S_17_min AS S_17_range,
        a.S_17_count,
        f.S_17_first,
        l.S_17_last,
        d.S_17_delta_mean,
        d.S_17_delta_max,
        d.S_17_delta_min,
        pd.S_17_delta_pd,
        cs.S_17_span
    FROM
        aggs a
        LEFT JOIN first_S_17 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_17 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_17_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_17_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_17_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_17_mean, 
    v.S_17_min,
    v.S_17_max, 
    v.S_17_range,
    v.S_17_sum,
    ISNULL(v.S_17_count, 0) AS S_17_count,
    v.S_17_first, 
    v.S_17_last,
    v.S_17_delta_mean,
    v.S_17_delta_max,
    v.S_17_delta_min,
    v.S_17_delta_pd,
    v.S_17_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_12_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_12 
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
    WHERE R_12 IS NOT NULL
    GROUP BY customer_ID
),
first_R_12 AS
(
    SELECT
        f.customer_ID, s.R_12 AS R_12_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_12 AS
(
    SELECT
        f.customer_ID, s.R_12 AS R_12_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_12_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_12_span
    FROM
        first_last
),
R_12_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_12,
        s.R_12 - LAG(s.R_12, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_12_delta
    FROM
        subset s
),
R_12_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_12_delta
    FROM
        R_12_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_12_delta_per_day AS
(
    SELECT
        customer_ID,
        R_12_delta / date_delta AS R_12_delta_per_day
    FROM
        R_12_delta
),
R_12_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_12_delta_per_day) AS R_12_delta_pd
    FROM
        R_12_delta_per_day
    GROUP BY
        customer_ID
),      
R_12_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_12_delta) AS R_12_delta_mean,
        MAX(R_12_delta) AS R_12_delta_max,
        MIN(R_12_delta) AS R_12_delta_min
    FROM
        R_12_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_12) AS R_12_mean,
        MIN(R_12) AS R_12_min, 
        MAX(R_12) AS R_12_max, 
        SUM(R_12) AS R_12_sum,
        COUNT(R_12) AS R_12_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_12_mean,
        a.R_12_min, 
        a.R_12_max, 
        a.R_12_sum,
        a.R_12_max - a.R_12_min AS R_12_range,
        a.R_12_count,
        f.R_12_first,
        l.R_12_last,
        d.R_12_delta_mean,
        d.R_12_delta_max,
        d.R_12_delta_min,
        pd.R_12_delta_pd,
        cs.R_12_span
    FROM
        aggs a
        LEFT JOIN first_R_12 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_12 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_12_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_12_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_12_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_12_mean, 
    v.R_12_min,
    v.R_12_max, 
    v.R_12_range,
    v.R_12_sum,
    ISNULL(v.R_12_count, 0) AS R_12_count,
    v.R_12_first, 
    v.R_12_last,
    v.R_12_delta_mean,
    v.R_12_delta_max,
    v.R_12_delta_min,
    v.R_12_delta_pd,
    v.R_12_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_28_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_28 
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
GO

CREATE VIEW test_data_R_13_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_13 
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
    WHERE R_13 IS NOT NULL
    GROUP BY customer_ID
),
first_R_13 AS
(
    SELECT
        f.customer_ID, s.R_13 AS R_13_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_13 AS
(
    SELECT
        f.customer_ID, s.R_13 AS R_13_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_13_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_13_span
    FROM
        first_last
),
R_13_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_13,
        s.R_13 - LAG(s.R_13, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_13_delta
    FROM
        subset s
),
R_13_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_13_delta
    FROM
        R_13_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_13_delta_per_day AS
(
    SELECT
        customer_ID,
        R_13_delta / date_delta AS R_13_delta_per_day
    FROM
        R_13_delta
),
R_13_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_13_delta_per_day) AS R_13_delta_pd
    FROM
        R_13_delta_per_day
    GROUP BY
        customer_ID
),      
R_13_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_13_delta) AS R_13_delta_mean,
        MAX(R_13_delta) AS R_13_delta_max,
        MIN(R_13_delta) AS R_13_delta_min
    FROM
        R_13_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_13) AS R_13_mean,
        MIN(R_13) AS R_13_min, 
        MAX(R_13) AS R_13_max, 
        SUM(R_13) AS R_13_sum,
        COUNT(R_13) AS R_13_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_13_mean,
        a.R_13_min, 
        a.R_13_max, 
        a.R_13_sum,
        a.R_13_max - a.R_13_min AS R_13_range,
        a.R_13_count,
        f.R_13_first,
        l.R_13_last,
        d.R_13_delta_mean,
        d.R_13_delta_max,
        d.R_13_delta_min,
        pd.R_13_delta_pd,
        cs.R_13_span
    FROM
        aggs a
        LEFT JOIN first_R_13 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_13 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_13_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_13_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_13_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_13_mean, 
    v.R_13_min,
    v.R_13_max, 
    v.R_13_range,
    v.R_13_sum,
    ISNULL(v.R_13_count, 0) AS R_13_count,
    v.R_13_first, 
    v.R_13_last,
    v.R_13_delta_mean,
    v.R_13_delta_max,
    v.R_13_delta_min,
    v.R_13_delta_pd,
    v.R_13_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_83_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_83 
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
    WHERE D_83 IS NOT NULL
    GROUP BY customer_ID
),
first_D_83 AS
(
    SELECT
        f.customer_ID, s.D_83 AS D_83_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_83 AS
(
    SELECT
        f.customer_ID, s.D_83 AS D_83_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_83_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_83_span
    FROM
        first_last
),
D_83_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_83,
        s.D_83 - LAG(s.D_83, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_83_delta
    FROM
        subset s
),
D_83_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_83_delta
    FROM
        D_83_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_83_delta_per_day AS
(
    SELECT
        customer_ID,
        D_83_delta / date_delta AS D_83_delta_per_day
    FROM
        D_83_delta
),
D_83_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_83_delta_per_day) AS D_83_delta_pd
    FROM
        D_83_delta_per_day
    GROUP BY
        customer_ID
),      
D_83_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_83_delta) AS D_83_delta_mean,
        MAX(D_83_delta) AS D_83_delta_max,
        MIN(D_83_delta) AS D_83_delta_min
    FROM
        D_83_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_83) AS D_83_mean,
        MIN(D_83) AS D_83_min, 
        MAX(D_83) AS D_83_max, 
        SUM(D_83) AS D_83_sum,
        COUNT(D_83) AS D_83_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_83_mean,
        a.D_83_min, 
        a.D_83_max, 
        a.D_83_sum,
        a.D_83_max - a.D_83_min AS D_83_range,
        a.D_83_count,
        f.D_83_first,
        l.D_83_last,
        d.D_83_delta_mean,
        d.D_83_delta_max,
        d.D_83_delta_min,
        pd.D_83_delta_pd,
        cs.D_83_span
    FROM
        aggs a
        LEFT JOIN first_D_83 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_83 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_83_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_83_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_83_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_83_mean, 
    v.D_83_min,
    v.D_83_max, 
    v.D_83_range,
    v.D_83_sum,
    ISNULL(v.D_83_count, 0) AS D_83_count,
    v.D_83_first, 
    v.D_83_last,
    v.D_83_delta_mean,
    v.D_83_delta_max,
    v.D_83_delta_min,
    v.D_83_delta_pd,
    v.D_83_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_14_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_14 
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
    WHERE R_14 IS NOT NULL
    GROUP BY customer_ID
),
first_R_14 AS
(
    SELECT
        f.customer_ID, s.R_14 AS R_14_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_14 AS
(
    SELECT
        f.customer_ID, s.R_14 AS R_14_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_14_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_14_span
    FROM
        first_last
),
R_14_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_14,
        s.R_14 - LAG(s.R_14, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_14_delta
    FROM
        subset s
),
R_14_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_14_delta
    FROM
        R_14_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_14_delta_per_day AS
(
    SELECT
        customer_ID,
        R_14_delta / date_delta AS R_14_delta_per_day
    FROM
        R_14_delta
),
R_14_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_14_delta_per_day) AS R_14_delta_pd
    FROM
        R_14_delta_per_day
    GROUP BY
        customer_ID
),      
R_14_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_14_delta) AS R_14_delta_mean,
        MAX(R_14_delta) AS R_14_delta_max,
        MIN(R_14_delta) AS R_14_delta_min
    FROM
        R_14_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_14) AS R_14_mean,
        MIN(R_14) AS R_14_min, 
        MAX(R_14) AS R_14_max, 
        SUM(R_14) AS R_14_sum,
        COUNT(R_14) AS R_14_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_14_mean,
        a.R_14_min, 
        a.R_14_max, 
        a.R_14_sum,
        a.R_14_max - a.R_14_min AS R_14_range,
        a.R_14_count,
        f.R_14_first,
        l.R_14_last,
        d.R_14_delta_mean,
        d.R_14_delta_max,
        d.R_14_delta_min,
        pd.R_14_delta_pd,
        cs.R_14_span
    FROM
        aggs a
        LEFT JOIN first_R_14 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_14 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_14_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_14_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_14_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_14_mean, 
    v.R_14_min,
    v.R_14_max, 
    v.R_14_range,
    v.R_14_sum,
    ISNULL(v.R_14_count, 0) AS R_14_count,
    v.R_14_first, 
    v.R_14_last,
    v.R_14_delta_mean,
    v.R_14_delta_max,
    v.R_14_delta_min,
    v.R_14_delta_pd,
    v.R_14_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_15_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_15 
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
    WHERE R_15 IS NOT NULL
    GROUP BY customer_ID
),
first_R_15 AS
(
    SELECT
        f.customer_ID, s.R_15 AS R_15_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_15 AS
(
    SELECT
        f.customer_ID, s.R_15 AS R_15_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_15_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_15_span
    FROM
        first_last
),
R_15_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_15,
        s.R_15 - LAG(s.R_15, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_15_delta
    FROM
        subset s
),
R_15_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_15_delta
    FROM
        R_15_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_15_delta_per_day AS
(
    SELECT
        customer_ID,
        R_15_delta / date_delta AS R_15_delta_per_day
    FROM
        R_15_delta
),
R_15_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_15_delta_per_day) AS R_15_delta_pd
    FROM
        R_15_delta_per_day
    GROUP BY
        customer_ID
),      
R_15_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_15_delta) AS R_15_delta_mean,
        MAX(R_15_delta) AS R_15_delta_max,
        MIN(R_15_delta) AS R_15_delta_min
    FROM
        R_15_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_15) AS R_15_mean,
        MIN(R_15) AS R_15_min, 
        MAX(R_15) AS R_15_max, 
        SUM(R_15) AS R_15_sum,
        COUNT(R_15) AS R_15_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_15_mean,
        a.R_15_min, 
        a.R_15_max, 
        a.R_15_sum,
        a.R_15_max - a.R_15_min AS R_15_range,
        a.R_15_count,
        f.R_15_first,
        l.R_15_last,
        d.R_15_delta_mean,
        d.R_15_delta_max,
        d.R_15_delta_min,
        pd.R_15_delta_pd,
        cs.R_15_span
    FROM
        aggs a
        LEFT JOIN first_R_15 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_15 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_15_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_15_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_15_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_15_mean, 
    v.R_15_min,
    v.R_15_max, 
    v.R_15_range,
    v.R_15_sum,
    ISNULL(v.R_15_count, 0) AS R_15_count,
    v.R_15_first, 
    v.R_15_last,
    v.R_15_delta_mean,
    v.R_15_delta_max,
    v.R_15_delta_min,
    v.R_15_delta_pd,
    v.R_15_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_84_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_84 
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
    WHERE D_84 IS NOT NULL
    GROUP BY customer_ID
),
first_D_84 AS
(
    SELECT
        f.customer_ID, s.D_84 AS D_84_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_84 AS
(
    SELECT
        f.customer_ID, s.D_84 AS D_84_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_84_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_84_span
    FROM
        first_last
),
D_84_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_84,
        s.D_84 - LAG(s.D_84, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_84_delta
    FROM
        subset s
),
D_84_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_84_delta
    FROM
        D_84_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_84_delta_per_day AS
(
    SELECT
        customer_ID,
        D_84_delta / date_delta AS D_84_delta_per_day
    FROM
        D_84_delta
),
D_84_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_84_delta_per_day) AS D_84_delta_pd
    FROM
        D_84_delta_per_day
    GROUP BY
        customer_ID
),      
D_84_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_84_delta) AS D_84_delta_mean,
        MAX(D_84_delta) AS D_84_delta_max,
        MIN(D_84_delta) AS D_84_delta_min
    FROM
        D_84_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_84) AS D_84_mean,
        MIN(D_84) AS D_84_min, 
        MAX(D_84) AS D_84_max, 
        SUM(D_84) AS D_84_sum,
        COUNT(D_84) AS D_84_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_84_mean,
        a.D_84_min, 
        a.D_84_max, 
        a.D_84_sum,
        a.D_84_max - a.D_84_min AS D_84_range,
        a.D_84_count,
        f.D_84_first,
        l.D_84_last,
        d.D_84_delta_mean,
        d.D_84_delta_max,
        d.D_84_delta_min,
        pd.D_84_delta_pd,
        cs.D_84_span
    FROM
        aggs a
        LEFT JOIN first_D_84 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_84 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_84_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_84_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_84_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_84_mean, 
    v.D_84_min,
    v.D_84_max, 
    v.D_84_range,
    v.D_84_sum,
    ISNULL(v.D_84_count, 0) AS D_84_count,
    v.D_84_first, 
    v.D_84_last,
    v.D_84_delta_mean,
    v.D_84_delta_max,
    v.D_84_delta_min,
    v.D_84_delta_pd,
    v.D_84_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_16_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_16 
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
    WHERE R_16 IS NOT NULL
    GROUP BY customer_ID
),
first_R_16 AS
(
    SELECT
        f.customer_ID, s.R_16 AS R_16_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_16 AS
(
    SELECT
        f.customer_ID, s.R_16 AS R_16_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_16_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_16_span
    FROM
        first_last
),
R_16_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_16,
        s.R_16 - LAG(s.R_16, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_16_delta
    FROM
        subset s
),
R_16_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_16_delta
    FROM
        R_16_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_16_delta_per_day AS
(
    SELECT
        customer_ID,
        R_16_delta / date_delta AS R_16_delta_per_day
    FROM
        R_16_delta
),
R_16_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_16_delta_per_day) AS R_16_delta_pd
    FROM
        R_16_delta_per_day
    GROUP BY
        customer_ID
),      
R_16_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_16_delta) AS R_16_delta_mean,
        MAX(R_16_delta) AS R_16_delta_max,
        MIN(R_16_delta) AS R_16_delta_min
    FROM
        R_16_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_16) AS R_16_mean,
        MIN(R_16) AS R_16_min, 
        MAX(R_16) AS R_16_max, 
        SUM(R_16) AS R_16_sum,
        COUNT(R_16) AS R_16_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_16_mean,
        a.R_16_min, 
        a.R_16_max, 
        a.R_16_sum,
        a.R_16_max - a.R_16_min AS R_16_range,
        a.R_16_count,
        f.R_16_first,
        l.R_16_last,
        d.R_16_delta_mean,
        d.R_16_delta_max,
        d.R_16_delta_min,
        pd.R_16_delta_pd,
        cs.R_16_span
    FROM
        aggs a
        LEFT JOIN first_R_16 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_16 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_16_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_16_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_16_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_16_mean, 
    v.R_16_min,
    v.R_16_max, 
    v.R_16_range,
    v.R_16_sum,
    ISNULL(v.R_16_count, 0) AS R_16_count,
    v.R_16_first, 
    v.R_16_last,
    v.R_16_delta_mean,
    v.R_16_delta_max,
    v.R_16_delta_min,
    v.R_16_delta_pd,
    v.R_16_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_29_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_29 
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
    WHERE B_29 IS NOT NULL
    GROUP BY customer_ID
),
first_B_29 AS
(
    SELECT
        f.customer_ID, s.B_29 AS B_29_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_29 AS
(
    SELECT
        f.customer_ID, s.B_29 AS B_29_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_29_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_29_span
    FROM
        first_last
),
B_29_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_29,
        s.B_29 - LAG(s.B_29, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_29_delta
    FROM
        subset s
),
B_29_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_29_delta
    FROM
        B_29_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_29_delta_per_day AS
(
    SELECT
        customer_ID,
        B_29_delta / date_delta AS B_29_delta_per_day
    FROM
        B_29_delta
),
B_29_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_29_delta_per_day) AS B_29_delta_pd
    FROM
        B_29_delta_per_day
    GROUP BY
        customer_ID
),      
B_29_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_29_delta) AS B_29_delta_mean,
        MAX(B_29_delta) AS B_29_delta_max,
        MIN(B_29_delta) AS B_29_delta_min
    FROM
        B_29_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_29) AS B_29_mean,
        MIN(B_29) AS B_29_min, 
        MAX(B_29) AS B_29_max, 
        SUM(B_29) AS B_29_sum,
        COUNT(B_29) AS B_29_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_29_mean,
        a.B_29_min, 
        a.B_29_max, 
        a.B_29_sum,
        a.B_29_max - a.B_29_min AS B_29_range,
        a.B_29_count,
        f.B_29_first,
        l.B_29_last,
        d.B_29_delta_mean,
        d.B_29_delta_max,
        d.B_29_delta_min,
        pd.B_29_delta_pd,
        cs.B_29_span
    FROM
        aggs a
        LEFT JOIN first_B_29 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_29 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_29_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_29_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_29_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_29_mean, 
    v.B_29_min,
    v.B_29_max, 
    v.B_29_range,
    v.B_29_sum,
    ISNULL(v.B_29_count, 0) AS B_29_count,
    v.B_29_first, 
    v.B_29_last,
    v.B_29_delta_mean,
    v.B_29_delta_max,
    v.B_29_delta_min,
    v.B_29_delta_pd,
    v.B_29_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_18_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_18 
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
    WHERE S_18 IS NOT NULL
    GROUP BY customer_ID
),
first_S_18 AS
(
    SELECT
        f.customer_ID, s.S_18 AS S_18_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_18 AS
(
    SELECT
        f.customer_ID, s.S_18 AS S_18_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_18_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_18_span
    FROM
        first_last
),
S_18_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_18,
        s.S_18 - LAG(s.S_18, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_18_delta
    FROM
        subset s
),
S_18_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_18_delta
    FROM
        S_18_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_18_delta_per_day AS
(
    SELECT
        customer_ID,
        S_18_delta / date_delta AS S_18_delta_per_day
    FROM
        S_18_delta
),
S_18_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_18_delta_per_day) AS S_18_delta_pd
    FROM
        S_18_delta_per_day
    GROUP BY
        customer_ID
),      
S_18_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_18_delta) AS S_18_delta_mean,
        MAX(S_18_delta) AS S_18_delta_max,
        MIN(S_18_delta) AS S_18_delta_min
    FROM
        S_18_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_18) AS S_18_mean,
        MIN(S_18) AS S_18_min, 
        MAX(S_18) AS S_18_max, 
        SUM(S_18) AS S_18_sum,
        COUNT(S_18) AS S_18_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_18_mean,
        a.S_18_min, 
        a.S_18_max, 
        a.S_18_sum,
        a.S_18_max - a.S_18_min AS S_18_range,
        a.S_18_count,
        f.S_18_first,
        l.S_18_last,
        d.S_18_delta_mean,
        d.S_18_delta_max,
        d.S_18_delta_min,
        pd.S_18_delta_pd,
        cs.S_18_span
    FROM
        aggs a
        LEFT JOIN first_S_18 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_18 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_18_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_18_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_18_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_18_mean, 
    v.S_18_min,
    v.S_18_max, 
    v.S_18_range,
    v.S_18_sum,
    ISNULL(v.S_18_count, 0) AS S_18_count,
    v.S_18_first, 
    v.S_18_last,
    v.S_18_delta_mean,
    v.S_18_delta_max,
    v.S_18_delta_min,
    v.S_18_delta_pd,
    v.S_18_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_86_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_86 
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
    WHERE D_86 IS NOT NULL
    GROUP BY customer_ID
),
first_D_86 AS
(
    SELECT
        f.customer_ID, s.D_86 AS D_86_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_86 AS
(
    SELECT
        f.customer_ID, s.D_86 AS D_86_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_86_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_86_span
    FROM
        first_last
),
D_86_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_86,
        s.D_86 - LAG(s.D_86, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_86_delta
    FROM
        subset s
),
D_86_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_86_delta
    FROM
        D_86_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_86_delta_per_day AS
(
    SELECT
        customer_ID,
        D_86_delta / date_delta AS D_86_delta_per_day
    FROM
        D_86_delta
),
D_86_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_86_delta_per_day) AS D_86_delta_pd
    FROM
        D_86_delta_per_day
    GROUP BY
        customer_ID
),      
D_86_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_86_delta) AS D_86_delta_mean,
        MAX(D_86_delta) AS D_86_delta_max,
        MIN(D_86_delta) AS D_86_delta_min
    FROM
        D_86_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_86) AS D_86_mean,
        MIN(D_86) AS D_86_min, 
        MAX(D_86) AS D_86_max, 
        SUM(D_86) AS D_86_sum,
        COUNT(D_86) AS D_86_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_86_mean,
        a.D_86_min, 
        a.D_86_max, 
        a.D_86_sum,
        a.D_86_max - a.D_86_min AS D_86_range,
        a.D_86_count,
        f.D_86_first,
        l.D_86_last,
        d.D_86_delta_mean,
        d.D_86_delta_max,
        d.D_86_delta_min,
        pd.D_86_delta_pd,
        cs.D_86_span
    FROM
        aggs a
        LEFT JOIN first_D_86 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_86 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_86_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_86_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_86_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_86_mean, 
    v.D_86_min,
    v.D_86_max, 
    v.D_86_range,
    v.D_86_sum,
    ISNULL(v.D_86_count, 0) AS D_86_count,
    v.D_86_first, 
    v.D_86_last,
    v.D_86_delta_mean,
    v.D_86_delta_max,
    v.D_86_delta_min,
    v.D_86_delta_pd,
    v.D_86_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_87_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_87 
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
    WHERE D_87 IS NOT NULL
    GROUP BY customer_ID
),
first_D_87 AS
(
    SELECT
        f.customer_ID, s.D_87 AS D_87_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_87 AS
(
    SELECT
        f.customer_ID, s.D_87 AS D_87_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_87_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_87_span
    FROM
        first_last
),
D_87_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_87,
        s.D_87 - LAG(s.D_87, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_87_delta
    FROM
        subset s
),
D_87_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_87_delta
    FROM
        D_87_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_87_delta_per_day AS
(
    SELECT
        customer_ID,
        D_87_delta / date_delta AS D_87_delta_per_day
    FROM
        D_87_delta
),
D_87_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_87_delta_per_day) AS D_87_delta_pd
    FROM
        D_87_delta_per_day
    GROUP BY
        customer_ID
),      
D_87_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_87_delta) AS D_87_delta_mean,
        MAX(D_87_delta) AS D_87_delta_max,
        MIN(D_87_delta) AS D_87_delta_min
    FROM
        D_87_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_87) AS D_87_mean,
        MIN(D_87) AS D_87_min, 
        MAX(D_87) AS D_87_max, 
        SUM(D_87) AS D_87_sum,
        COUNT(D_87) AS D_87_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_87_mean,
        a.D_87_min, 
        a.D_87_max, 
        a.D_87_sum,
        a.D_87_max - a.D_87_min AS D_87_range,
        a.D_87_count,
        f.D_87_first,
        l.D_87_last,
        d.D_87_delta_mean,
        d.D_87_delta_max,
        d.D_87_delta_min,
        pd.D_87_delta_pd,
        cs.D_87_span
    FROM
        aggs a
        LEFT JOIN first_D_87 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_87 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_87_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_87_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_87_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_87_mean, 
    v.D_87_min,
    v.D_87_max, 
    v.D_87_range,
    v.D_87_sum,
    ISNULL(v.D_87_count, 0) AS D_87_count,
    v.D_87_first, 
    v.D_87_last,
    v.D_87_delta_mean,
    v.D_87_delta_max,
    v.D_87_delta_min,
    v.D_87_delta_pd,
    v.D_87_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_17_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_17 
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
    WHERE R_17 IS NOT NULL
    GROUP BY customer_ID
),
first_R_17 AS
(
    SELECT
        f.customer_ID, s.R_17 AS R_17_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_17 AS
(
    SELECT
        f.customer_ID, s.R_17 AS R_17_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_17_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_17_span
    FROM
        first_last
),
R_17_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_17,
        s.R_17 - LAG(s.R_17, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_17_delta
    FROM
        subset s
),
R_17_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_17_delta
    FROM
        R_17_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_17_delta_per_day AS
(
    SELECT
        customer_ID,
        R_17_delta / date_delta AS R_17_delta_per_day
    FROM
        R_17_delta
),
R_17_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_17_delta_per_day) AS R_17_delta_pd
    FROM
        R_17_delta_per_day
    GROUP BY
        customer_ID
),      
R_17_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_17_delta) AS R_17_delta_mean,
        MAX(R_17_delta) AS R_17_delta_max,
        MIN(R_17_delta) AS R_17_delta_min
    FROM
        R_17_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_17) AS R_17_mean,
        MIN(R_17) AS R_17_min, 
        MAX(R_17) AS R_17_max, 
        SUM(R_17) AS R_17_sum,
        COUNT(R_17) AS R_17_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_17_mean,
        a.R_17_min, 
        a.R_17_max, 
        a.R_17_sum,
        a.R_17_max - a.R_17_min AS R_17_range,
        a.R_17_count,
        f.R_17_first,
        l.R_17_last,
        d.R_17_delta_mean,
        d.R_17_delta_max,
        d.R_17_delta_min,
        pd.R_17_delta_pd,
        cs.R_17_span
    FROM
        aggs a
        LEFT JOIN first_R_17 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_17 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_17_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_17_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_17_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_17_mean, 
    v.R_17_min,
    v.R_17_max, 
    v.R_17_range,
    v.R_17_sum,
    ISNULL(v.R_17_count, 0) AS R_17_count,
    v.R_17_first, 
    v.R_17_last,
    v.R_17_delta_mean,
    v.R_17_delta_max,
    v.R_17_delta_min,
    v.R_17_delta_pd,
    v.R_17_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_18_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_18 
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
    WHERE R_18 IS NOT NULL
    GROUP BY customer_ID
),
first_R_18 AS
(
    SELECT
        f.customer_ID, s.R_18 AS R_18_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_18 AS
(
    SELECT
        f.customer_ID, s.R_18 AS R_18_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_18_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_18_span
    FROM
        first_last
),
R_18_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_18,
        s.R_18 - LAG(s.R_18, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_18_delta
    FROM
        subset s
),
R_18_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_18_delta
    FROM
        R_18_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_18_delta_per_day AS
(
    SELECT
        customer_ID,
        R_18_delta / date_delta AS R_18_delta_per_day
    FROM
        R_18_delta
),
R_18_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_18_delta_per_day) AS R_18_delta_pd
    FROM
        R_18_delta_per_day
    GROUP BY
        customer_ID
),      
R_18_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_18_delta) AS R_18_delta_mean,
        MAX(R_18_delta) AS R_18_delta_max,
        MIN(R_18_delta) AS R_18_delta_min
    FROM
        R_18_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_18) AS R_18_mean,
        MIN(R_18) AS R_18_min, 
        MAX(R_18) AS R_18_max, 
        SUM(R_18) AS R_18_sum,
        COUNT(R_18) AS R_18_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_18_mean,
        a.R_18_min, 
        a.R_18_max, 
        a.R_18_sum,
        a.R_18_max - a.R_18_min AS R_18_range,
        a.R_18_count,
        f.R_18_first,
        l.R_18_last,
        d.R_18_delta_mean,
        d.R_18_delta_max,
        d.R_18_delta_min,
        pd.R_18_delta_pd,
        cs.R_18_span
    FROM
        aggs a
        LEFT JOIN first_R_18 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_18 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_18_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_18_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_18_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_18_mean, 
    v.R_18_min,
    v.R_18_max, 
    v.R_18_range,
    v.R_18_sum,
    ISNULL(v.R_18_count, 0) AS R_18_count,
    v.R_18_first, 
    v.R_18_last,
    v.R_18_delta_mean,
    v.R_18_delta_max,
    v.R_18_delta_min,
    v.R_18_delta_pd,
    v.R_18_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_88_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_88 
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
    WHERE D_88 IS NOT NULL
    GROUP BY customer_ID
),
first_D_88 AS
(
    SELECT
        f.customer_ID, s.D_88 AS D_88_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_88 AS
(
    SELECT
        f.customer_ID, s.D_88 AS D_88_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_88_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_88_span
    FROM
        first_last
),
D_88_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_88,
        s.D_88 - LAG(s.D_88, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_88_delta
    FROM
        subset s
),
D_88_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_88_delta
    FROM
        D_88_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_88_delta_per_day AS
(
    SELECT
        customer_ID,
        D_88_delta / date_delta AS D_88_delta_per_day
    FROM
        D_88_delta
),
D_88_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_88_delta_per_day) AS D_88_delta_pd
    FROM
        D_88_delta_per_day
    GROUP BY
        customer_ID
),      
D_88_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_88_delta) AS D_88_delta_mean,
        MAX(D_88_delta) AS D_88_delta_max,
        MIN(D_88_delta) AS D_88_delta_min
    FROM
        D_88_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_88) AS D_88_mean,
        MIN(D_88) AS D_88_min, 
        MAX(D_88) AS D_88_max, 
        SUM(D_88) AS D_88_sum,
        COUNT(D_88) AS D_88_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_88_mean,
        a.D_88_min, 
        a.D_88_max, 
        a.D_88_sum,
        a.D_88_max - a.D_88_min AS D_88_range,
        a.D_88_count,
        f.D_88_first,
        l.D_88_last,
        d.D_88_delta_mean,
        d.D_88_delta_max,
        d.D_88_delta_min,
        pd.D_88_delta_pd,
        cs.D_88_span
    FROM
        aggs a
        LEFT JOIN first_D_88 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_88 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_88_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_88_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_88_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_88_mean, 
    v.D_88_min,
    v.D_88_max, 
    v.D_88_range,
    v.D_88_sum,
    ISNULL(v.D_88_count, 0) AS D_88_count,
    v.D_88_first, 
    v.D_88_last,
    v.D_88_delta_mean,
    v.D_88_delta_max,
    v.D_88_delta_min,
    v.D_88_delta_pd,
    v.D_88_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_31_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_31 
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
    WHERE B_31 IS NOT NULL
    GROUP BY customer_ID
),
first_B_31 AS
(
    SELECT
        f.customer_ID, s.B_31 AS B_31_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_31 AS
(
    SELECT
        f.customer_ID, s.B_31 AS B_31_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_31_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_31_span
    FROM
        first_last
),
B_31_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_31,
        s.B_31 - LAG(s.B_31, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_31_delta
    FROM
        subset s
),
B_31_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_31_delta
    FROM
        B_31_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_31_delta_per_day AS
(
    SELECT
        customer_ID,
        B_31_delta / date_delta AS B_31_delta_per_day
    FROM
        B_31_delta
),
B_31_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_31_delta_per_day) AS B_31_delta_pd
    FROM
        B_31_delta_per_day
    GROUP BY
        customer_ID
),      
B_31_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_31_delta) AS B_31_delta_mean,
        MAX(B_31_delta) AS B_31_delta_max,
        MIN(B_31_delta) AS B_31_delta_min
    FROM
        B_31_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_31) AS B_31_mean,
        MIN(B_31) AS B_31_min, 
        MAX(B_31) AS B_31_max, 
        SUM(B_31) AS B_31_sum,
        COUNT(B_31) AS B_31_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_31_mean,
        a.B_31_min, 
        a.B_31_max, 
        a.B_31_sum,
        a.B_31_max - a.B_31_min AS B_31_range,
        a.B_31_count,
        f.B_31_first,
        l.B_31_last,
        d.B_31_delta_mean,
        d.B_31_delta_max,
        d.B_31_delta_min,
        pd.B_31_delta_pd,
        cs.B_31_span
    FROM
        aggs a
        LEFT JOIN first_B_31 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_31 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_31_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_31_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_31_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_31_mean, 
    v.B_31_min,
    v.B_31_max, 
    v.B_31_range,
    v.B_31_sum,
    ISNULL(v.B_31_count, 0) AS B_31_count,
    v.B_31_first, 
    v.B_31_last,
    v.B_31_delta_mean,
    v.B_31_delta_max,
    v.B_31_delta_min,
    v.B_31_delta_pd,
    v.B_31_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_19_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_19 
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
    WHERE S_19 IS NOT NULL
    GROUP BY customer_ID
),
first_S_19 AS
(
    SELECT
        f.customer_ID, s.S_19 AS S_19_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_19 AS
(
    SELECT
        f.customer_ID, s.S_19 AS S_19_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_19_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_19_span
    FROM
        first_last
),
S_19_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_19,
        s.S_19 - LAG(s.S_19, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_19_delta
    FROM
        subset s
),
S_19_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_19_delta
    FROM
        S_19_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_19_delta_per_day AS
(
    SELECT
        customer_ID,
        S_19_delta / date_delta AS S_19_delta_per_day
    FROM
        S_19_delta
),
S_19_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_19_delta_per_day) AS S_19_delta_pd
    FROM
        S_19_delta_per_day
    GROUP BY
        customer_ID
),      
S_19_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_19_delta) AS S_19_delta_mean,
        MAX(S_19_delta) AS S_19_delta_max,
        MIN(S_19_delta) AS S_19_delta_min
    FROM
        S_19_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_19) AS S_19_mean,
        MIN(S_19) AS S_19_min, 
        MAX(S_19) AS S_19_max, 
        SUM(S_19) AS S_19_sum,
        COUNT(S_19) AS S_19_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_19_mean,
        a.S_19_min, 
        a.S_19_max, 
        a.S_19_sum,
        a.S_19_max - a.S_19_min AS S_19_range,
        a.S_19_count,
        f.S_19_first,
        l.S_19_last,
        d.S_19_delta_mean,
        d.S_19_delta_max,
        d.S_19_delta_min,
        pd.S_19_delta_pd,
        cs.S_19_span
    FROM
        aggs a
        LEFT JOIN first_S_19 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_19 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_19_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_19_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_19_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_19_mean, 
    v.S_19_min,
    v.S_19_max, 
    v.S_19_range,
    v.S_19_sum,
    ISNULL(v.S_19_count, 0) AS S_19_count,
    v.S_19_first, 
    v.S_19_last,
    v.S_19_delta_mean,
    v.S_19_delta_max,
    v.S_19_delta_min,
    v.S_19_delta_pd,
    v.S_19_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_19_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_19 
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
    WHERE R_19 IS NOT NULL
    GROUP BY customer_ID
),
first_R_19 AS
(
    SELECT
        f.customer_ID, s.R_19 AS R_19_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_19 AS
(
    SELECT
        f.customer_ID, s.R_19 AS R_19_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_19_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_19_span
    FROM
        first_last
),
R_19_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_19,
        s.R_19 - LAG(s.R_19, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_19_delta
    FROM
        subset s
),
R_19_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_19_delta
    FROM
        R_19_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_19_delta_per_day AS
(
    SELECT
        customer_ID,
        R_19_delta / date_delta AS R_19_delta_per_day
    FROM
        R_19_delta
),
R_19_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_19_delta_per_day) AS R_19_delta_pd
    FROM
        R_19_delta_per_day
    GROUP BY
        customer_ID
),      
R_19_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_19_delta) AS R_19_delta_mean,
        MAX(R_19_delta) AS R_19_delta_max,
        MIN(R_19_delta) AS R_19_delta_min
    FROM
        R_19_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_19) AS R_19_mean,
        MIN(R_19) AS R_19_min, 
        MAX(R_19) AS R_19_max, 
        SUM(R_19) AS R_19_sum,
        COUNT(R_19) AS R_19_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_19_mean,
        a.R_19_min, 
        a.R_19_max, 
        a.R_19_sum,
        a.R_19_max - a.R_19_min AS R_19_range,
        a.R_19_count,
        f.R_19_first,
        l.R_19_last,
        d.R_19_delta_mean,
        d.R_19_delta_max,
        d.R_19_delta_min,
        pd.R_19_delta_pd,
        cs.R_19_span
    FROM
        aggs a
        LEFT JOIN first_R_19 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_19 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_19_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_19_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_19_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_19_mean, 
    v.R_19_min,
    v.R_19_max, 
    v.R_19_range,
    v.R_19_sum,
    ISNULL(v.R_19_count, 0) AS R_19_count,
    v.R_19_first, 
    v.R_19_last,
    v.R_19_delta_mean,
    v.R_19_delta_max,
    v.R_19_delta_min,
    v.R_19_delta_pd,
    v.R_19_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_32_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_32 
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
    WHERE B_32 IS NOT NULL
    GROUP BY customer_ID
),
first_B_32 AS
(
    SELECT
        f.customer_ID, s.B_32 AS B_32_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_32 AS
(
    SELECT
        f.customer_ID, s.B_32 AS B_32_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_32_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_32_span
    FROM
        first_last
),
B_32_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_32,
        s.B_32 - LAG(s.B_32, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_32_delta
    FROM
        subset s
),
B_32_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_32_delta
    FROM
        B_32_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_32_delta_per_day AS
(
    SELECT
        customer_ID,
        B_32_delta / date_delta AS B_32_delta_per_day
    FROM
        B_32_delta
),
B_32_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_32_delta_per_day) AS B_32_delta_pd
    FROM
        B_32_delta_per_day
    GROUP BY
        customer_ID
),      
B_32_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_32_delta) AS B_32_delta_mean,
        MAX(B_32_delta) AS B_32_delta_max,
        MIN(B_32_delta) AS B_32_delta_min
    FROM
        B_32_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_32) AS B_32_mean,
        MIN(B_32) AS B_32_min, 
        MAX(B_32) AS B_32_max, 
        SUM(B_32) AS B_32_sum,
        COUNT(B_32) AS B_32_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_32_mean,
        a.B_32_min, 
        a.B_32_max, 
        a.B_32_sum,
        a.B_32_max - a.B_32_min AS B_32_range,
        a.B_32_count,
        f.B_32_first,
        l.B_32_last,
        d.B_32_delta_mean,
        d.B_32_delta_max,
        d.B_32_delta_min,
        pd.B_32_delta_pd,
        cs.B_32_span
    FROM
        aggs a
        LEFT JOIN first_B_32 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_32 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_32_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_32_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_32_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_32_mean, 
    v.B_32_min,
    v.B_32_max, 
    v.B_32_range,
    v.B_32_sum,
    ISNULL(v.B_32_count, 0) AS B_32_count,
    v.B_32_first, 
    v.B_32_last,
    v.B_32_delta_mean,
    v.B_32_delta_max,
    v.B_32_delta_min,
    v.B_32_delta_pd,
    v.B_32_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_20_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_20 
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
    WHERE S_20 IS NOT NULL
    GROUP BY customer_ID
),
first_S_20 AS
(
    SELECT
        f.customer_ID, s.S_20 AS S_20_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_20 AS
(
    SELECT
        f.customer_ID, s.S_20 AS S_20_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_20_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_20_span
    FROM
        first_last
),
S_20_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_20,
        s.S_20 - LAG(s.S_20, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_20_delta
    FROM
        subset s
),
S_20_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_20_delta
    FROM
        S_20_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_20_delta_per_day AS
(
    SELECT
        customer_ID,
        S_20_delta / date_delta AS S_20_delta_per_day
    FROM
        S_20_delta
),
S_20_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_20_delta_per_day) AS S_20_delta_pd
    FROM
        S_20_delta_per_day
    GROUP BY
        customer_ID
),      
S_20_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_20_delta) AS S_20_delta_mean,
        MAX(S_20_delta) AS S_20_delta_max,
        MIN(S_20_delta) AS S_20_delta_min
    FROM
        S_20_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_20) AS S_20_mean,
        MIN(S_20) AS S_20_min, 
        MAX(S_20) AS S_20_max, 
        SUM(S_20) AS S_20_sum,
        COUNT(S_20) AS S_20_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_20_mean,
        a.S_20_min, 
        a.S_20_max, 
        a.S_20_sum,
        a.S_20_max - a.S_20_min AS S_20_range,
        a.S_20_count,
        f.S_20_first,
        l.S_20_last,
        d.S_20_delta_mean,
        d.S_20_delta_max,
        d.S_20_delta_min,
        pd.S_20_delta_pd,
        cs.S_20_span
    FROM
        aggs a
        LEFT JOIN first_S_20 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_20 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_20_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_20_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_20_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_20_mean, 
    v.S_20_min,
    v.S_20_max, 
    v.S_20_range,
    v.S_20_sum,
    ISNULL(v.S_20_count, 0) AS S_20_count,
    v.S_20_first, 
    v.S_20_last,
    v.S_20_delta_mean,
    v.S_20_delta_max,
    v.S_20_delta_min,
    v.S_20_delta_pd,
    v.S_20_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_20_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_20 
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
    WHERE R_20 IS NOT NULL
    GROUP BY customer_ID
),
first_R_20 AS
(
    SELECT
        f.customer_ID, s.R_20 AS R_20_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_20 AS
(
    SELECT
        f.customer_ID, s.R_20 AS R_20_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_20_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_20_span
    FROM
        first_last
),
R_20_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_20,
        s.R_20 - LAG(s.R_20, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_20_delta
    FROM
        subset s
),
R_20_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_20_delta
    FROM
        R_20_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_20_delta_per_day AS
(
    SELECT
        customer_ID,
        R_20_delta / date_delta AS R_20_delta_per_day
    FROM
        R_20_delta
),
R_20_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_20_delta_per_day) AS R_20_delta_pd
    FROM
        R_20_delta_per_day
    GROUP BY
        customer_ID
),      
R_20_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_20_delta) AS R_20_delta_mean,
        MAX(R_20_delta) AS R_20_delta_max,
        MIN(R_20_delta) AS R_20_delta_min
    FROM
        R_20_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_20) AS R_20_mean,
        MIN(R_20) AS R_20_min, 
        MAX(R_20) AS R_20_max, 
        SUM(R_20) AS R_20_sum,
        COUNT(R_20) AS R_20_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_20_mean,
        a.R_20_min, 
        a.R_20_max, 
        a.R_20_sum,
        a.R_20_max - a.R_20_min AS R_20_range,
        a.R_20_count,
        f.R_20_first,
        l.R_20_last,
        d.R_20_delta_mean,
        d.R_20_delta_max,
        d.R_20_delta_min,
        pd.R_20_delta_pd,
        cs.R_20_span
    FROM
        aggs a
        LEFT JOIN first_R_20 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_20 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_20_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_20_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_20_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_20_mean, 
    v.R_20_min,
    v.R_20_max, 
    v.R_20_range,
    v.R_20_sum,
    ISNULL(v.R_20_count, 0) AS R_20_count,
    v.R_20_first, 
    v.R_20_last,
    v.R_20_delta_mean,
    v.R_20_delta_max,
    v.R_20_delta_min,
    v.R_20_delta_pd,
    v.R_20_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_21_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_21 
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
    WHERE R_21 IS NOT NULL
    GROUP BY customer_ID
),
first_R_21 AS
(
    SELECT
        f.customer_ID, s.R_21 AS R_21_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_21 AS
(
    SELECT
        f.customer_ID, s.R_21 AS R_21_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_21_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_21_span
    FROM
        first_last
),
R_21_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_21,
        s.R_21 - LAG(s.R_21, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_21_delta
    FROM
        subset s
),
R_21_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_21_delta
    FROM
        R_21_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_21_delta_per_day AS
(
    SELECT
        customer_ID,
        R_21_delta / date_delta AS R_21_delta_per_day
    FROM
        R_21_delta
),
R_21_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_21_delta_per_day) AS R_21_delta_pd
    FROM
        R_21_delta_per_day
    GROUP BY
        customer_ID
),      
R_21_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_21_delta) AS R_21_delta_mean,
        MAX(R_21_delta) AS R_21_delta_max,
        MIN(R_21_delta) AS R_21_delta_min
    FROM
        R_21_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_21) AS R_21_mean,
        MIN(R_21) AS R_21_min, 
        MAX(R_21) AS R_21_max, 
        SUM(R_21) AS R_21_sum,
        COUNT(R_21) AS R_21_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_21_mean,
        a.R_21_min, 
        a.R_21_max, 
        a.R_21_sum,
        a.R_21_max - a.R_21_min AS R_21_range,
        a.R_21_count,
        f.R_21_first,
        l.R_21_last,
        d.R_21_delta_mean,
        d.R_21_delta_max,
        d.R_21_delta_min,
        pd.R_21_delta_pd,
        cs.R_21_span
    FROM
        aggs a
        LEFT JOIN first_R_21 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_21 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_21_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_21_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_21_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_21_mean, 
    v.R_21_min,
    v.R_21_max, 
    v.R_21_range,
    v.R_21_sum,
    ISNULL(v.R_21_count, 0) AS R_21_count,
    v.R_21_first, 
    v.R_21_last,
    v.R_21_delta_mean,
    v.R_21_delta_max,
    v.R_21_delta_min,
    v.R_21_delta_pd,
    v.R_21_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_33_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_33 
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
GO

CREATE VIEW test_data_D_89_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_89 
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
    WHERE D_89 IS NOT NULL
    GROUP BY customer_ID
),
first_D_89 AS
(
    SELECT
        f.customer_ID, s.D_89 AS D_89_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_89 AS
(
    SELECT
        f.customer_ID, s.D_89 AS D_89_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_89_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_89_span
    FROM
        first_last
),
D_89_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_89,
        s.D_89 - LAG(s.D_89, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_89_delta
    FROM
        subset s
),
D_89_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_89_delta
    FROM
        D_89_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_89_delta_per_day AS
(
    SELECT
        customer_ID,
        D_89_delta / date_delta AS D_89_delta_per_day
    FROM
        D_89_delta
),
D_89_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_89_delta_per_day) AS D_89_delta_pd
    FROM
        D_89_delta_per_day
    GROUP BY
        customer_ID
),      
D_89_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_89_delta) AS D_89_delta_mean,
        MAX(D_89_delta) AS D_89_delta_max,
        MIN(D_89_delta) AS D_89_delta_min
    FROM
        D_89_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_89) AS D_89_mean,
        MIN(D_89) AS D_89_min, 
        MAX(D_89) AS D_89_max, 
        SUM(D_89) AS D_89_sum,
        COUNT(D_89) AS D_89_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_89_mean,
        a.D_89_min, 
        a.D_89_max, 
        a.D_89_sum,
        a.D_89_max - a.D_89_min AS D_89_range,
        a.D_89_count,
        f.D_89_first,
        l.D_89_last,
        d.D_89_delta_mean,
        d.D_89_delta_max,
        d.D_89_delta_min,
        pd.D_89_delta_pd,
        cs.D_89_span
    FROM
        aggs a
        LEFT JOIN first_D_89 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_89 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_89_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_89_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_89_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_89_mean, 
    v.D_89_min,
    v.D_89_max, 
    v.D_89_range,
    v.D_89_sum,
    ISNULL(v.D_89_count, 0) AS D_89_count,
    v.D_89_first, 
    v.D_89_last,
    v.D_89_delta_mean,
    v.D_89_delta_max,
    v.D_89_delta_min,
    v.D_89_delta_pd,
    v.D_89_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_22_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_22 
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
    WHERE R_22 IS NOT NULL
    GROUP BY customer_ID
),
first_R_22 AS
(
    SELECT
        f.customer_ID, s.R_22 AS R_22_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_22 AS
(
    SELECT
        f.customer_ID, s.R_22 AS R_22_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_22_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_22_span
    FROM
        first_last
),
R_22_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_22,
        s.R_22 - LAG(s.R_22, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_22_delta
    FROM
        subset s
),
R_22_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_22_delta
    FROM
        R_22_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_22_delta_per_day AS
(
    SELECT
        customer_ID,
        R_22_delta / date_delta AS R_22_delta_per_day
    FROM
        R_22_delta
),
R_22_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_22_delta_per_day) AS R_22_delta_pd
    FROM
        R_22_delta_per_day
    GROUP BY
        customer_ID
),      
R_22_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_22_delta) AS R_22_delta_mean,
        MAX(R_22_delta) AS R_22_delta_max,
        MIN(R_22_delta) AS R_22_delta_min
    FROM
        R_22_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_22) AS R_22_mean,
        MIN(R_22) AS R_22_min, 
        MAX(R_22) AS R_22_max, 
        SUM(R_22) AS R_22_sum,
        COUNT(R_22) AS R_22_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_22_mean,
        a.R_22_min, 
        a.R_22_max, 
        a.R_22_sum,
        a.R_22_max - a.R_22_min AS R_22_range,
        a.R_22_count,
        f.R_22_first,
        l.R_22_last,
        d.R_22_delta_mean,
        d.R_22_delta_max,
        d.R_22_delta_min,
        pd.R_22_delta_pd,
        cs.R_22_span
    FROM
        aggs a
        LEFT JOIN first_R_22 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_22 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_22_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_22_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_22_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_22_mean, 
    v.R_22_min,
    v.R_22_max, 
    v.R_22_range,
    v.R_22_sum,
    ISNULL(v.R_22_count, 0) AS R_22_count,
    v.R_22_first, 
    v.R_22_last,
    v.R_22_delta_mean,
    v.R_22_delta_max,
    v.R_22_delta_min,
    v.R_22_delta_pd,
    v.R_22_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_23_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_23 
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
    WHERE R_23 IS NOT NULL
    GROUP BY customer_ID
),
first_R_23 AS
(
    SELECT
        f.customer_ID, s.R_23 AS R_23_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_23 AS
(
    SELECT
        f.customer_ID, s.R_23 AS R_23_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_23_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_23_span
    FROM
        first_last
),
R_23_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_23,
        s.R_23 - LAG(s.R_23, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_23_delta
    FROM
        subset s
),
R_23_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_23_delta
    FROM
        R_23_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_23_delta_per_day AS
(
    SELECT
        customer_ID,
        R_23_delta / date_delta AS R_23_delta_per_day
    FROM
        R_23_delta
),
R_23_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_23_delta_per_day) AS R_23_delta_pd
    FROM
        R_23_delta_per_day
    GROUP BY
        customer_ID
),      
R_23_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_23_delta) AS R_23_delta_mean,
        MAX(R_23_delta) AS R_23_delta_max,
        MIN(R_23_delta) AS R_23_delta_min
    FROM
        R_23_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_23) AS R_23_mean,
        MIN(R_23) AS R_23_min, 
        MAX(R_23) AS R_23_max, 
        SUM(R_23) AS R_23_sum,
        COUNT(R_23) AS R_23_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_23_mean,
        a.R_23_min, 
        a.R_23_max, 
        a.R_23_sum,
        a.R_23_max - a.R_23_min AS R_23_range,
        a.R_23_count,
        f.R_23_first,
        l.R_23_last,
        d.R_23_delta_mean,
        d.R_23_delta_max,
        d.R_23_delta_min,
        pd.R_23_delta_pd,
        cs.R_23_span
    FROM
        aggs a
        LEFT JOIN first_R_23 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_23 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_23_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_23_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_23_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_23_mean, 
    v.R_23_min,
    v.R_23_max, 
    v.R_23_range,
    v.R_23_sum,
    ISNULL(v.R_23_count, 0) AS R_23_count,
    v.R_23_first, 
    v.R_23_last,
    v.R_23_delta_mean,
    v.R_23_delta_max,
    v.R_23_delta_min,
    v.R_23_delta_pd,
    v.R_23_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_91_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_91 
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
    WHERE D_91 IS NOT NULL
    GROUP BY customer_ID
),
first_D_91 AS
(
    SELECT
        f.customer_ID, s.D_91 AS D_91_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_91 AS
(
    SELECT
        f.customer_ID, s.D_91 AS D_91_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_91_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_91_span
    FROM
        first_last
),
D_91_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_91,
        s.D_91 - LAG(s.D_91, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_91_delta
    FROM
        subset s
),
D_91_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_91_delta
    FROM
        D_91_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_91_delta_per_day AS
(
    SELECT
        customer_ID,
        D_91_delta / date_delta AS D_91_delta_per_day
    FROM
        D_91_delta
),
D_91_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_91_delta_per_day) AS D_91_delta_pd
    FROM
        D_91_delta_per_day
    GROUP BY
        customer_ID
),      
D_91_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_91_delta) AS D_91_delta_mean,
        MAX(D_91_delta) AS D_91_delta_max,
        MIN(D_91_delta) AS D_91_delta_min
    FROM
        D_91_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_91) AS D_91_mean,
        MIN(D_91) AS D_91_min, 
        MAX(D_91) AS D_91_max, 
        SUM(D_91) AS D_91_sum,
        COUNT(D_91) AS D_91_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_91_mean,
        a.D_91_min, 
        a.D_91_max, 
        a.D_91_sum,
        a.D_91_max - a.D_91_min AS D_91_range,
        a.D_91_count,
        f.D_91_first,
        l.D_91_last,
        d.D_91_delta_mean,
        d.D_91_delta_max,
        d.D_91_delta_min,
        pd.D_91_delta_pd,
        cs.D_91_span
    FROM
        aggs a
        LEFT JOIN first_D_91 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_91 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_91_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_91_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_91_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_91_mean, 
    v.D_91_min,
    v.D_91_max, 
    v.D_91_range,
    v.D_91_sum,
    ISNULL(v.D_91_count, 0) AS D_91_count,
    v.D_91_first, 
    v.D_91_last,
    v.D_91_delta_mean,
    v.D_91_delta_max,
    v.D_91_delta_min,
    v.D_91_delta_pd,
    v.D_91_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_92_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_92 
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
    WHERE D_92 IS NOT NULL
    GROUP BY customer_ID
),
first_D_92 AS
(
    SELECT
        f.customer_ID, s.D_92 AS D_92_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_92 AS
(
    SELECT
        f.customer_ID, s.D_92 AS D_92_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_92_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_92_span
    FROM
        first_last
),
D_92_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_92,
        s.D_92 - LAG(s.D_92, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_92_delta
    FROM
        subset s
),
D_92_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_92_delta
    FROM
        D_92_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_92_delta_per_day AS
(
    SELECT
        customer_ID,
        D_92_delta / date_delta AS D_92_delta_per_day
    FROM
        D_92_delta
),
D_92_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_92_delta_per_day) AS D_92_delta_pd
    FROM
        D_92_delta_per_day
    GROUP BY
        customer_ID
),      
D_92_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_92_delta) AS D_92_delta_mean,
        MAX(D_92_delta) AS D_92_delta_max,
        MIN(D_92_delta) AS D_92_delta_min
    FROM
        D_92_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_92) AS D_92_mean,
        MIN(D_92) AS D_92_min, 
        MAX(D_92) AS D_92_max, 
        SUM(D_92) AS D_92_sum,
        COUNT(D_92) AS D_92_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_92_mean,
        a.D_92_min, 
        a.D_92_max, 
        a.D_92_sum,
        a.D_92_max - a.D_92_min AS D_92_range,
        a.D_92_count,
        f.D_92_first,
        l.D_92_last,
        d.D_92_delta_mean,
        d.D_92_delta_max,
        d.D_92_delta_min,
        pd.D_92_delta_pd,
        cs.D_92_span
    FROM
        aggs a
        LEFT JOIN first_D_92 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_92 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_92_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_92_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_92_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_92_mean, 
    v.D_92_min,
    v.D_92_max, 
    v.D_92_range,
    v.D_92_sum,
    ISNULL(v.D_92_count, 0) AS D_92_count,
    v.D_92_first, 
    v.D_92_last,
    v.D_92_delta_mean,
    v.D_92_delta_max,
    v.D_92_delta_min,
    v.D_92_delta_pd,
    v.D_92_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_93_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_93 
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
    WHERE D_93 IS NOT NULL
    GROUP BY customer_ID
),
first_D_93 AS
(
    SELECT
        f.customer_ID, s.D_93 AS D_93_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_93 AS
(
    SELECT
        f.customer_ID, s.D_93 AS D_93_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_93_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_93_span
    FROM
        first_last
),
D_93_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_93,
        s.D_93 - LAG(s.D_93, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_93_delta
    FROM
        subset s
),
D_93_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_93_delta
    FROM
        D_93_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_93_delta_per_day AS
(
    SELECT
        customer_ID,
        D_93_delta / date_delta AS D_93_delta_per_day
    FROM
        D_93_delta
),
D_93_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_93_delta_per_day) AS D_93_delta_pd
    FROM
        D_93_delta_per_day
    GROUP BY
        customer_ID
),      
D_93_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_93_delta) AS D_93_delta_mean,
        MAX(D_93_delta) AS D_93_delta_max,
        MIN(D_93_delta) AS D_93_delta_min
    FROM
        D_93_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_93) AS D_93_mean,
        MIN(D_93) AS D_93_min, 
        MAX(D_93) AS D_93_max, 
        SUM(D_93) AS D_93_sum,
        COUNT(D_93) AS D_93_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_93_mean,
        a.D_93_min, 
        a.D_93_max, 
        a.D_93_sum,
        a.D_93_max - a.D_93_min AS D_93_range,
        a.D_93_count,
        f.D_93_first,
        l.D_93_last,
        d.D_93_delta_mean,
        d.D_93_delta_max,
        d.D_93_delta_min,
        pd.D_93_delta_pd,
        cs.D_93_span
    FROM
        aggs a
        LEFT JOIN first_D_93 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_93 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_93_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_93_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_93_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_93_mean, 
    v.D_93_min,
    v.D_93_max, 
    v.D_93_range,
    v.D_93_sum,
    ISNULL(v.D_93_count, 0) AS D_93_count,
    v.D_93_first, 
    v.D_93_last,
    v.D_93_delta_mean,
    v.D_93_delta_max,
    v.D_93_delta_min,
    v.D_93_delta_pd,
    v.D_93_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_94_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_94 
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
    WHERE D_94 IS NOT NULL
    GROUP BY customer_ID
),
first_D_94 AS
(
    SELECT
        f.customer_ID, s.D_94 AS D_94_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_94 AS
(
    SELECT
        f.customer_ID, s.D_94 AS D_94_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_94_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_94_span
    FROM
        first_last
),
D_94_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_94,
        s.D_94 - LAG(s.D_94, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_94_delta
    FROM
        subset s
),
D_94_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_94_delta
    FROM
        D_94_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_94_delta_per_day AS
(
    SELECT
        customer_ID,
        D_94_delta / date_delta AS D_94_delta_per_day
    FROM
        D_94_delta
),
D_94_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_94_delta_per_day) AS D_94_delta_pd
    FROM
        D_94_delta_per_day
    GROUP BY
        customer_ID
),      
D_94_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_94_delta) AS D_94_delta_mean,
        MAX(D_94_delta) AS D_94_delta_max,
        MIN(D_94_delta) AS D_94_delta_min
    FROM
        D_94_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_94) AS D_94_mean,
        MIN(D_94) AS D_94_min, 
        MAX(D_94) AS D_94_max, 
        SUM(D_94) AS D_94_sum,
        COUNT(D_94) AS D_94_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_94_mean,
        a.D_94_min, 
        a.D_94_max, 
        a.D_94_sum,
        a.D_94_max - a.D_94_min AS D_94_range,
        a.D_94_count,
        f.D_94_first,
        l.D_94_last,
        d.D_94_delta_mean,
        d.D_94_delta_max,
        d.D_94_delta_min,
        pd.D_94_delta_pd,
        cs.D_94_span
    FROM
        aggs a
        LEFT JOIN first_D_94 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_94 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_94_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_94_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_94_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_94_mean, 
    v.D_94_min,
    v.D_94_max, 
    v.D_94_range,
    v.D_94_sum,
    ISNULL(v.D_94_count, 0) AS D_94_count,
    v.D_94_first, 
    v.D_94_last,
    v.D_94_delta_mean,
    v.D_94_delta_max,
    v.D_94_delta_min,
    v.D_94_delta_pd,
    v.D_94_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_24_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_24 
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
    WHERE R_24 IS NOT NULL
    GROUP BY customer_ID
),
first_R_24 AS
(
    SELECT
        f.customer_ID, s.R_24 AS R_24_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_24 AS
(
    SELECT
        f.customer_ID, s.R_24 AS R_24_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_24_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_24_span
    FROM
        first_last
),
R_24_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_24,
        s.R_24 - LAG(s.R_24, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_24_delta
    FROM
        subset s
),
R_24_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_24_delta
    FROM
        R_24_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_24_delta_per_day AS
(
    SELECT
        customer_ID,
        R_24_delta / date_delta AS R_24_delta_per_day
    FROM
        R_24_delta
),
R_24_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_24_delta_per_day) AS R_24_delta_pd
    FROM
        R_24_delta_per_day
    GROUP BY
        customer_ID
),      
R_24_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_24_delta) AS R_24_delta_mean,
        MAX(R_24_delta) AS R_24_delta_max,
        MIN(R_24_delta) AS R_24_delta_min
    FROM
        R_24_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_24) AS R_24_mean,
        MIN(R_24) AS R_24_min, 
        MAX(R_24) AS R_24_max, 
        SUM(R_24) AS R_24_sum,
        COUNT(R_24) AS R_24_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_24_mean,
        a.R_24_min, 
        a.R_24_max, 
        a.R_24_sum,
        a.R_24_max - a.R_24_min AS R_24_range,
        a.R_24_count,
        f.R_24_first,
        l.R_24_last,
        d.R_24_delta_mean,
        d.R_24_delta_max,
        d.R_24_delta_min,
        pd.R_24_delta_pd,
        cs.R_24_span
    FROM
        aggs a
        LEFT JOIN first_R_24 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_24 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_24_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_24_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_24_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_24_mean, 
    v.R_24_min,
    v.R_24_max, 
    v.R_24_range,
    v.R_24_sum,
    ISNULL(v.R_24_count, 0) AS R_24_count,
    v.R_24_first, 
    v.R_24_last,
    v.R_24_delta_mean,
    v.R_24_delta_max,
    v.R_24_delta_min,
    v.R_24_delta_pd,
    v.R_24_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_25_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_25 
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
    WHERE R_25 IS NOT NULL
    GROUP BY customer_ID
),
first_R_25 AS
(
    SELECT
        f.customer_ID, s.R_25 AS R_25_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_25 AS
(
    SELECT
        f.customer_ID, s.R_25 AS R_25_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_25_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_25_span
    FROM
        first_last
),
R_25_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_25,
        s.R_25 - LAG(s.R_25, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_25_delta
    FROM
        subset s
),
R_25_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_25_delta
    FROM
        R_25_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_25_delta_per_day AS
(
    SELECT
        customer_ID,
        R_25_delta / date_delta AS R_25_delta_per_day
    FROM
        R_25_delta
),
R_25_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_25_delta_per_day) AS R_25_delta_pd
    FROM
        R_25_delta_per_day
    GROUP BY
        customer_ID
),      
R_25_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_25_delta) AS R_25_delta_mean,
        MAX(R_25_delta) AS R_25_delta_max,
        MIN(R_25_delta) AS R_25_delta_min
    FROM
        R_25_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_25) AS R_25_mean,
        MIN(R_25) AS R_25_min, 
        MAX(R_25) AS R_25_max, 
        SUM(R_25) AS R_25_sum,
        COUNT(R_25) AS R_25_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_25_mean,
        a.R_25_min, 
        a.R_25_max, 
        a.R_25_sum,
        a.R_25_max - a.R_25_min AS R_25_range,
        a.R_25_count,
        f.R_25_first,
        l.R_25_last,
        d.R_25_delta_mean,
        d.R_25_delta_max,
        d.R_25_delta_min,
        pd.R_25_delta_pd,
        cs.R_25_span
    FROM
        aggs a
        LEFT JOIN first_R_25 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_25 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_25_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_25_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_25_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_25_mean, 
    v.R_25_min,
    v.R_25_max, 
    v.R_25_range,
    v.R_25_sum,
    ISNULL(v.R_25_count, 0) AS R_25_count,
    v.R_25_first, 
    v.R_25_last,
    v.R_25_delta_mean,
    v.R_25_delta_max,
    v.R_25_delta_min,
    v.R_25_delta_pd,
    v.R_25_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_96_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_96 
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
    WHERE D_96 IS NOT NULL
    GROUP BY customer_ID
),
first_D_96 AS
(
    SELECT
        f.customer_ID, s.D_96 AS D_96_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_96 AS
(
    SELECT
        f.customer_ID, s.D_96 AS D_96_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_96_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_96_span
    FROM
        first_last
),
D_96_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_96,
        s.D_96 - LAG(s.D_96, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_96_delta
    FROM
        subset s
),
D_96_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_96_delta
    FROM
        D_96_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_96_delta_per_day AS
(
    SELECT
        customer_ID,
        D_96_delta / date_delta AS D_96_delta_per_day
    FROM
        D_96_delta
),
D_96_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_96_delta_per_day) AS D_96_delta_pd
    FROM
        D_96_delta_per_day
    GROUP BY
        customer_ID
),      
D_96_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_96_delta) AS D_96_delta_mean,
        MAX(D_96_delta) AS D_96_delta_max,
        MIN(D_96_delta) AS D_96_delta_min
    FROM
        D_96_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_96) AS D_96_mean,
        MIN(D_96) AS D_96_min, 
        MAX(D_96) AS D_96_max, 
        SUM(D_96) AS D_96_sum,
        COUNT(D_96) AS D_96_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_96_mean,
        a.D_96_min, 
        a.D_96_max, 
        a.D_96_sum,
        a.D_96_max - a.D_96_min AS D_96_range,
        a.D_96_count,
        f.D_96_first,
        l.D_96_last,
        d.D_96_delta_mean,
        d.D_96_delta_max,
        d.D_96_delta_min,
        pd.D_96_delta_pd,
        cs.D_96_span
    FROM
        aggs a
        LEFT JOIN first_D_96 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_96 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_96_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_96_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_96_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_96_mean, 
    v.D_96_min,
    v.D_96_max, 
    v.D_96_range,
    v.D_96_sum,
    ISNULL(v.D_96_count, 0) AS D_96_count,
    v.D_96_first, 
    v.D_96_last,
    v.D_96_delta_mean,
    v.D_96_delta_max,
    v.D_96_delta_min,
    v.D_96_delta_pd,
    v.D_96_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_22_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_22 
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
    WHERE S_22 IS NOT NULL
    GROUP BY customer_ID
),
first_S_22 AS
(
    SELECT
        f.customer_ID, s.S_22 AS S_22_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_22 AS
(
    SELECT
        f.customer_ID, s.S_22 AS S_22_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_22_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_22_span
    FROM
        first_last
),
S_22_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_22,
        s.S_22 - LAG(s.S_22, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_22_delta
    FROM
        subset s
),
S_22_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_22_delta
    FROM
        S_22_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_22_delta_per_day AS
(
    SELECT
        customer_ID,
        S_22_delta / date_delta AS S_22_delta_per_day
    FROM
        S_22_delta
),
S_22_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_22_delta_per_day) AS S_22_delta_pd
    FROM
        S_22_delta_per_day
    GROUP BY
        customer_ID
),      
S_22_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_22_delta) AS S_22_delta_mean,
        MAX(S_22_delta) AS S_22_delta_max,
        MIN(S_22_delta) AS S_22_delta_min
    FROM
        S_22_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_22) AS S_22_mean,
        MIN(S_22) AS S_22_min, 
        MAX(S_22) AS S_22_max, 
        SUM(S_22) AS S_22_sum,
        COUNT(S_22) AS S_22_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_22_mean,
        a.S_22_min, 
        a.S_22_max, 
        a.S_22_sum,
        a.S_22_max - a.S_22_min AS S_22_range,
        a.S_22_count,
        f.S_22_first,
        l.S_22_last,
        d.S_22_delta_mean,
        d.S_22_delta_max,
        d.S_22_delta_min,
        pd.S_22_delta_pd,
        cs.S_22_span
    FROM
        aggs a
        LEFT JOIN first_S_22 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_22 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_22_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_22_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_22_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_22_mean, 
    v.S_22_min,
    v.S_22_max, 
    v.S_22_range,
    v.S_22_sum,
    ISNULL(v.S_22_count, 0) AS S_22_count,
    v.S_22_first, 
    v.S_22_last,
    v.S_22_delta_mean,
    v.S_22_delta_max,
    v.S_22_delta_min,
    v.S_22_delta_pd,
    v.S_22_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_23_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_23 
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
    WHERE S_23 IS NOT NULL
    GROUP BY customer_ID
),
first_S_23 AS
(
    SELECT
        f.customer_ID, s.S_23 AS S_23_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_23 AS
(
    SELECT
        f.customer_ID, s.S_23 AS S_23_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_23_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_23_span
    FROM
        first_last
),
S_23_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_23,
        s.S_23 - LAG(s.S_23, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_23_delta
    FROM
        subset s
),
S_23_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_23_delta
    FROM
        S_23_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_23_delta_per_day AS
(
    SELECT
        customer_ID,
        S_23_delta / date_delta AS S_23_delta_per_day
    FROM
        S_23_delta
),
S_23_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_23_delta_per_day) AS S_23_delta_pd
    FROM
        S_23_delta_per_day
    GROUP BY
        customer_ID
),      
S_23_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_23_delta) AS S_23_delta_mean,
        MAX(S_23_delta) AS S_23_delta_max,
        MIN(S_23_delta) AS S_23_delta_min
    FROM
        S_23_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_23) AS S_23_mean,
        MIN(S_23) AS S_23_min, 
        MAX(S_23) AS S_23_max, 
        SUM(S_23) AS S_23_sum,
        COUNT(S_23) AS S_23_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_23_mean,
        a.S_23_min, 
        a.S_23_max, 
        a.S_23_sum,
        a.S_23_max - a.S_23_min AS S_23_range,
        a.S_23_count,
        f.S_23_first,
        l.S_23_last,
        d.S_23_delta_mean,
        d.S_23_delta_max,
        d.S_23_delta_min,
        pd.S_23_delta_pd,
        cs.S_23_span
    FROM
        aggs a
        LEFT JOIN first_S_23 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_23 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_23_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_23_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_23_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_23_mean, 
    v.S_23_min,
    v.S_23_max, 
    v.S_23_range,
    v.S_23_sum,
    ISNULL(v.S_23_count, 0) AS S_23_count,
    v.S_23_first, 
    v.S_23_last,
    v.S_23_delta_mean,
    v.S_23_delta_max,
    v.S_23_delta_min,
    v.S_23_delta_pd,
    v.S_23_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_24_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_24 
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
    WHERE S_24 IS NOT NULL
    GROUP BY customer_ID
),
first_S_24 AS
(
    SELECT
        f.customer_ID, s.S_24 AS S_24_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_24 AS
(
    SELECT
        f.customer_ID, s.S_24 AS S_24_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_24_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_24_span
    FROM
        first_last
),
S_24_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_24,
        s.S_24 - LAG(s.S_24, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_24_delta
    FROM
        subset s
),
S_24_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_24_delta
    FROM
        S_24_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_24_delta_per_day AS
(
    SELECT
        customer_ID,
        S_24_delta / date_delta AS S_24_delta_per_day
    FROM
        S_24_delta
),
S_24_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_24_delta_per_day) AS S_24_delta_pd
    FROM
        S_24_delta_per_day
    GROUP BY
        customer_ID
),      
S_24_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_24_delta) AS S_24_delta_mean,
        MAX(S_24_delta) AS S_24_delta_max,
        MIN(S_24_delta) AS S_24_delta_min
    FROM
        S_24_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_24) AS S_24_mean,
        MIN(S_24) AS S_24_min, 
        MAX(S_24) AS S_24_max, 
        SUM(S_24) AS S_24_sum,
        COUNT(S_24) AS S_24_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_24_mean,
        a.S_24_min, 
        a.S_24_max, 
        a.S_24_sum,
        a.S_24_max - a.S_24_min AS S_24_range,
        a.S_24_count,
        f.S_24_first,
        l.S_24_last,
        d.S_24_delta_mean,
        d.S_24_delta_max,
        d.S_24_delta_min,
        pd.S_24_delta_pd,
        cs.S_24_span
    FROM
        aggs a
        LEFT JOIN first_S_24 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_24 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_24_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_24_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_24_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_24_mean, 
    v.S_24_min,
    v.S_24_max, 
    v.S_24_range,
    v.S_24_sum,
    ISNULL(v.S_24_count, 0) AS S_24_count,
    v.S_24_first, 
    v.S_24_last,
    v.S_24_delta_mean,
    v.S_24_delta_max,
    v.S_24_delta_min,
    v.S_24_delta_pd,
    v.S_24_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_25_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_25 
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
    WHERE S_25 IS NOT NULL
    GROUP BY customer_ID
),
first_S_25 AS
(
    SELECT
        f.customer_ID, s.S_25 AS S_25_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_25 AS
(
    SELECT
        f.customer_ID, s.S_25 AS S_25_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_25_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_25_span
    FROM
        first_last
),
S_25_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_25,
        s.S_25 - LAG(s.S_25, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_25_delta
    FROM
        subset s
),
S_25_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_25_delta
    FROM
        S_25_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_25_delta_per_day AS
(
    SELECT
        customer_ID,
        S_25_delta / date_delta AS S_25_delta_per_day
    FROM
        S_25_delta
),
S_25_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_25_delta_per_day) AS S_25_delta_pd
    FROM
        S_25_delta_per_day
    GROUP BY
        customer_ID
),      
S_25_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_25_delta) AS S_25_delta_mean,
        MAX(S_25_delta) AS S_25_delta_max,
        MIN(S_25_delta) AS S_25_delta_min
    FROM
        S_25_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_25) AS S_25_mean,
        MIN(S_25) AS S_25_min, 
        MAX(S_25) AS S_25_max, 
        SUM(S_25) AS S_25_sum,
        COUNT(S_25) AS S_25_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_25_mean,
        a.S_25_min, 
        a.S_25_max, 
        a.S_25_sum,
        a.S_25_max - a.S_25_min AS S_25_range,
        a.S_25_count,
        f.S_25_first,
        l.S_25_last,
        d.S_25_delta_mean,
        d.S_25_delta_max,
        d.S_25_delta_min,
        pd.S_25_delta_pd,
        cs.S_25_span
    FROM
        aggs a
        LEFT JOIN first_S_25 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_25 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_25_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_25_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_25_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_25_mean, 
    v.S_25_min,
    v.S_25_max, 
    v.S_25_range,
    v.S_25_sum,
    ISNULL(v.S_25_count, 0) AS S_25_count,
    v.S_25_first, 
    v.S_25_last,
    v.S_25_delta_mean,
    v.S_25_delta_max,
    v.S_25_delta_min,
    v.S_25_delta_pd,
    v.S_25_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_26_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_26 
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
    WHERE S_26 IS NOT NULL
    GROUP BY customer_ID
),
first_S_26 AS
(
    SELECT
        f.customer_ID, s.S_26 AS S_26_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_26 AS
(
    SELECT
        f.customer_ID, s.S_26 AS S_26_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_26_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_26_span
    FROM
        first_last
),
S_26_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_26,
        s.S_26 - LAG(s.S_26, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_26_delta
    FROM
        subset s
),
S_26_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_26_delta
    FROM
        S_26_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_26_delta_per_day AS
(
    SELECT
        customer_ID,
        S_26_delta / date_delta AS S_26_delta_per_day
    FROM
        S_26_delta
),
S_26_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_26_delta_per_day) AS S_26_delta_pd
    FROM
        S_26_delta_per_day
    GROUP BY
        customer_ID
),      
S_26_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_26_delta) AS S_26_delta_mean,
        MAX(S_26_delta) AS S_26_delta_max,
        MIN(S_26_delta) AS S_26_delta_min
    FROM
        S_26_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_26) AS S_26_mean,
        MIN(S_26) AS S_26_min, 
        MAX(S_26) AS S_26_max, 
        SUM(S_26) AS S_26_sum,
        COUNT(S_26) AS S_26_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_26_mean,
        a.S_26_min, 
        a.S_26_max, 
        a.S_26_sum,
        a.S_26_max - a.S_26_min AS S_26_range,
        a.S_26_count,
        f.S_26_first,
        l.S_26_last,
        d.S_26_delta_mean,
        d.S_26_delta_max,
        d.S_26_delta_min,
        pd.S_26_delta_pd,
        cs.S_26_span
    FROM
        aggs a
        LEFT JOIN first_S_26 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_26 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_26_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_26_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_26_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_26_mean, 
    v.S_26_min,
    v.S_26_max, 
    v.S_26_range,
    v.S_26_sum,
    ISNULL(v.S_26_count, 0) AS S_26_count,
    v.S_26_first, 
    v.S_26_last,
    v.S_26_delta_mean,
    v.S_26_delta_max,
    v.S_26_delta_min,
    v.S_26_delta_pd,
    v.S_26_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_102_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_102 
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
    WHERE D_102 IS NOT NULL
    GROUP BY customer_ID
),
first_D_102 AS
(
    SELECT
        f.customer_ID, s.D_102 AS D_102_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_102 AS
(
    SELECT
        f.customer_ID, s.D_102 AS D_102_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_102_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_102_span
    FROM
        first_last
),
D_102_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_102,
        s.D_102 - LAG(s.D_102, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_102_delta
    FROM
        subset s
),
D_102_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_102_delta
    FROM
        D_102_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_102_delta_per_day AS
(
    SELECT
        customer_ID,
        D_102_delta / date_delta AS D_102_delta_per_day
    FROM
        D_102_delta
),
D_102_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_102_delta_per_day) AS D_102_delta_pd
    FROM
        D_102_delta_per_day
    GROUP BY
        customer_ID
),      
D_102_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_102_delta) AS D_102_delta_mean,
        MAX(D_102_delta) AS D_102_delta_max,
        MIN(D_102_delta) AS D_102_delta_min
    FROM
        D_102_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_102) AS D_102_mean,
        MIN(D_102) AS D_102_min, 
        MAX(D_102) AS D_102_max, 
        SUM(D_102) AS D_102_sum,
        COUNT(D_102) AS D_102_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_102_mean,
        a.D_102_min, 
        a.D_102_max, 
        a.D_102_sum,
        a.D_102_max - a.D_102_min AS D_102_range,
        a.D_102_count,
        f.D_102_first,
        l.D_102_last,
        d.D_102_delta_mean,
        d.D_102_delta_max,
        d.D_102_delta_min,
        pd.D_102_delta_pd,
        cs.D_102_span
    FROM
        aggs a
        LEFT JOIN first_D_102 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_102 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_102_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_102_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_102_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_102_mean, 
    v.D_102_min,
    v.D_102_max, 
    v.D_102_range,
    v.D_102_sum,
    ISNULL(v.D_102_count, 0) AS D_102_count,
    v.D_102_first, 
    v.D_102_last,
    v.D_102_delta_mean,
    v.D_102_delta_max,
    v.D_102_delta_min,
    v.D_102_delta_pd,
    v.D_102_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_103_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_103 
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
    WHERE D_103 IS NOT NULL
    GROUP BY customer_ID
),
first_D_103 AS
(
    SELECT
        f.customer_ID, s.D_103 AS D_103_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_103 AS
(
    SELECT
        f.customer_ID, s.D_103 AS D_103_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_103_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_103_span
    FROM
        first_last
),
D_103_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_103,
        s.D_103 - LAG(s.D_103, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_103_delta
    FROM
        subset s
),
D_103_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_103_delta
    FROM
        D_103_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_103_delta_per_day AS
(
    SELECT
        customer_ID,
        D_103_delta / date_delta AS D_103_delta_per_day
    FROM
        D_103_delta
),
D_103_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_103_delta_per_day) AS D_103_delta_pd
    FROM
        D_103_delta_per_day
    GROUP BY
        customer_ID
),      
D_103_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_103_delta) AS D_103_delta_mean,
        MAX(D_103_delta) AS D_103_delta_max,
        MIN(D_103_delta) AS D_103_delta_min
    FROM
        D_103_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_103) AS D_103_mean,
        MIN(D_103) AS D_103_min, 
        MAX(D_103) AS D_103_max, 
        SUM(D_103) AS D_103_sum,
        COUNT(D_103) AS D_103_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_103_mean,
        a.D_103_min, 
        a.D_103_max, 
        a.D_103_sum,
        a.D_103_max - a.D_103_min AS D_103_range,
        a.D_103_count,
        f.D_103_first,
        l.D_103_last,
        d.D_103_delta_mean,
        d.D_103_delta_max,
        d.D_103_delta_min,
        pd.D_103_delta_pd,
        cs.D_103_span
    FROM
        aggs a
        LEFT JOIN first_D_103 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_103 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_103_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_103_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_103_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_103_mean, 
    v.D_103_min,
    v.D_103_max, 
    v.D_103_range,
    v.D_103_sum,
    ISNULL(v.D_103_count, 0) AS D_103_count,
    v.D_103_first, 
    v.D_103_last,
    v.D_103_delta_mean,
    v.D_103_delta_max,
    v.D_103_delta_min,
    v.D_103_delta_pd,
    v.D_103_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_104_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_104 
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
    WHERE D_104 IS NOT NULL
    GROUP BY customer_ID
),
first_D_104 AS
(
    SELECT
        f.customer_ID, s.D_104 AS D_104_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_104 AS
(
    SELECT
        f.customer_ID, s.D_104 AS D_104_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_104_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_104_span
    FROM
        first_last
),
D_104_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_104,
        s.D_104 - LAG(s.D_104, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_104_delta
    FROM
        subset s
),
D_104_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_104_delta
    FROM
        D_104_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_104_delta_per_day AS
(
    SELECT
        customer_ID,
        D_104_delta / date_delta AS D_104_delta_per_day
    FROM
        D_104_delta
),
D_104_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_104_delta_per_day) AS D_104_delta_pd
    FROM
        D_104_delta_per_day
    GROUP BY
        customer_ID
),      
D_104_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_104_delta) AS D_104_delta_mean,
        MAX(D_104_delta) AS D_104_delta_max,
        MIN(D_104_delta) AS D_104_delta_min
    FROM
        D_104_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_104) AS D_104_mean,
        MIN(D_104) AS D_104_min, 
        MAX(D_104) AS D_104_max, 
        SUM(D_104) AS D_104_sum,
        COUNT(D_104) AS D_104_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_104_mean,
        a.D_104_min, 
        a.D_104_max, 
        a.D_104_sum,
        a.D_104_max - a.D_104_min AS D_104_range,
        a.D_104_count,
        f.D_104_first,
        l.D_104_last,
        d.D_104_delta_mean,
        d.D_104_delta_max,
        d.D_104_delta_min,
        pd.D_104_delta_pd,
        cs.D_104_span
    FROM
        aggs a
        LEFT JOIN first_D_104 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_104 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_104_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_104_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_104_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_104_mean, 
    v.D_104_min,
    v.D_104_max, 
    v.D_104_range,
    v.D_104_sum,
    ISNULL(v.D_104_count, 0) AS D_104_count,
    v.D_104_first, 
    v.D_104_last,
    v.D_104_delta_mean,
    v.D_104_delta_max,
    v.D_104_delta_min,
    v.D_104_delta_pd,
    v.D_104_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_105_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_105 
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
    WHERE D_105 IS NOT NULL
    GROUP BY customer_ID
),
first_D_105 AS
(
    SELECT
        f.customer_ID, s.D_105 AS D_105_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_105 AS
(
    SELECT
        f.customer_ID, s.D_105 AS D_105_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_105_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_105_span
    FROM
        first_last
),
D_105_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_105,
        s.D_105 - LAG(s.D_105, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_105_delta
    FROM
        subset s
),
D_105_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_105_delta
    FROM
        D_105_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_105_delta_per_day AS
(
    SELECT
        customer_ID,
        D_105_delta / date_delta AS D_105_delta_per_day
    FROM
        D_105_delta
),
D_105_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_105_delta_per_day) AS D_105_delta_pd
    FROM
        D_105_delta_per_day
    GROUP BY
        customer_ID
),      
D_105_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_105_delta) AS D_105_delta_mean,
        MAX(D_105_delta) AS D_105_delta_max,
        MIN(D_105_delta) AS D_105_delta_min
    FROM
        D_105_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_105) AS D_105_mean,
        MIN(D_105) AS D_105_min, 
        MAX(D_105) AS D_105_max, 
        SUM(D_105) AS D_105_sum,
        COUNT(D_105) AS D_105_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_105_mean,
        a.D_105_min, 
        a.D_105_max, 
        a.D_105_sum,
        a.D_105_max - a.D_105_min AS D_105_range,
        a.D_105_count,
        f.D_105_first,
        l.D_105_last,
        d.D_105_delta_mean,
        d.D_105_delta_max,
        d.D_105_delta_min,
        pd.D_105_delta_pd,
        cs.D_105_span
    FROM
        aggs a
        LEFT JOIN first_D_105 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_105 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_105_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_105_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_105_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_105_mean, 
    v.D_105_min,
    v.D_105_max, 
    v.D_105_range,
    v.D_105_sum,
    ISNULL(v.D_105_count, 0) AS D_105_count,
    v.D_105_first, 
    v.D_105_last,
    v.D_105_delta_mean,
    v.D_105_delta_max,
    v.D_105_delta_min,
    v.D_105_delta_pd,
    v.D_105_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_106_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_106 
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
    WHERE D_106 IS NOT NULL
    GROUP BY customer_ID
),
first_D_106 AS
(
    SELECT
        f.customer_ID, s.D_106 AS D_106_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_106 AS
(
    SELECT
        f.customer_ID, s.D_106 AS D_106_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_106_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_106_span
    FROM
        first_last
),
D_106_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_106,
        s.D_106 - LAG(s.D_106, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_106_delta
    FROM
        subset s
),
D_106_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_106_delta
    FROM
        D_106_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_106_delta_per_day AS
(
    SELECT
        customer_ID,
        D_106_delta / date_delta AS D_106_delta_per_day
    FROM
        D_106_delta
),
D_106_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_106_delta_per_day) AS D_106_delta_pd
    FROM
        D_106_delta_per_day
    GROUP BY
        customer_ID
),      
D_106_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_106_delta) AS D_106_delta_mean,
        MAX(D_106_delta) AS D_106_delta_max,
        MIN(D_106_delta) AS D_106_delta_min
    FROM
        D_106_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_106) AS D_106_mean,
        MIN(D_106) AS D_106_min, 
        MAX(D_106) AS D_106_max, 
        SUM(D_106) AS D_106_sum,
        COUNT(D_106) AS D_106_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_106_mean,
        a.D_106_min, 
        a.D_106_max, 
        a.D_106_sum,
        a.D_106_max - a.D_106_min AS D_106_range,
        a.D_106_count,
        f.D_106_first,
        l.D_106_last,
        d.D_106_delta_mean,
        d.D_106_delta_max,
        d.D_106_delta_min,
        pd.D_106_delta_pd,
        cs.D_106_span
    FROM
        aggs a
        LEFT JOIN first_D_106 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_106 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_106_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_106_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_106_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_106_mean, 
    v.D_106_min,
    v.D_106_max, 
    v.D_106_range,
    v.D_106_sum,
    ISNULL(v.D_106_count, 0) AS D_106_count,
    v.D_106_first, 
    v.D_106_last,
    v.D_106_delta_mean,
    v.D_106_delta_max,
    v.D_106_delta_min,
    v.D_106_delta_pd,
    v.D_106_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_107_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_107 
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
    WHERE D_107 IS NOT NULL
    GROUP BY customer_ID
),
first_D_107 AS
(
    SELECT
        f.customer_ID, s.D_107 AS D_107_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_107 AS
(
    SELECT
        f.customer_ID, s.D_107 AS D_107_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_107_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_107_span
    FROM
        first_last
),
D_107_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_107,
        s.D_107 - LAG(s.D_107, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_107_delta
    FROM
        subset s
),
D_107_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_107_delta
    FROM
        D_107_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_107_delta_per_day AS
(
    SELECT
        customer_ID,
        D_107_delta / date_delta AS D_107_delta_per_day
    FROM
        D_107_delta
),
D_107_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_107_delta_per_day) AS D_107_delta_pd
    FROM
        D_107_delta_per_day
    GROUP BY
        customer_ID
),      
D_107_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_107_delta) AS D_107_delta_mean,
        MAX(D_107_delta) AS D_107_delta_max,
        MIN(D_107_delta) AS D_107_delta_min
    FROM
        D_107_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_107) AS D_107_mean,
        MIN(D_107) AS D_107_min, 
        MAX(D_107) AS D_107_max, 
        SUM(D_107) AS D_107_sum,
        COUNT(D_107) AS D_107_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_107_mean,
        a.D_107_min, 
        a.D_107_max, 
        a.D_107_sum,
        a.D_107_max - a.D_107_min AS D_107_range,
        a.D_107_count,
        f.D_107_first,
        l.D_107_last,
        d.D_107_delta_mean,
        d.D_107_delta_max,
        d.D_107_delta_min,
        pd.D_107_delta_pd,
        cs.D_107_span
    FROM
        aggs a
        LEFT JOIN first_D_107 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_107 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_107_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_107_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_107_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_107_mean, 
    v.D_107_min,
    v.D_107_max, 
    v.D_107_range,
    v.D_107_sum,
    ISNULL(v.D_107_count, 0) AS D_107_count,
    v.D_107_first, 
    v.D_107_last,
    v.D_107_delta_mean,
    v.D_107_delta_max,
    v.D_107_delta_min,
    v.D_107_delta_pd,
    v.D_107_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_36_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_36 
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
    WHERE B_36 IS NOT NULL
    GROUP BY customer_ID
),
first_B_36 AS
(
    SELECT
        f.customer_ID, s.B_36 AS B_36_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_36 AS
(
    SELECT
        f.customer_ID, s.B_36 AS B_36_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_36_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_36_span
    FROM
        first_last
),
B_36_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_36,
        s.B_36 - LAG(s.B_36, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_36_delta
    FROM
        subset s
),
B_36_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_36_delta
    FROM
        B_36_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_36_delta_per_day AS
(
    SELECT
        customer_ID,
        B_36_delta / date_delta AS B_36_delta_per_day
    FROM
        B_36_delta
),
B_36_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_36_delta_per_day) AS B_36_delta_pd
    FROM
        B_36_delta_per_day
    GROUP BY
        customer_ID
),      
B_36_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_36_delta) AS B_36_delta_mean,
        MAX(B_36_delta) AS B_36_delta_max,
        MIN(B_36_delta) AS B_36_delta_min
    FROM
        B_36_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_36) AS B_36_mean,
        MIN(B_36) AS B_36_min, 
        MAX(B_36) AS B_36_max, 
        SUM(B_36) AS B_36_sum,
        COUNT(B_36) AS B_36_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_36_mean,
        a.B_36_min, 
        a.B_36_max, 
        a.B_36_sum,
        a.B_36_max - a.B_36_min AS B_36_range,
        a.B_36_count,
        f.B_36_first,
        l.B_36_last,
        d.B_36_delta_mean,
        d.B_36_delta_max,
        d.B_36_delta_min,
        pd.B_36_delta_pd,
        cs.B_36_span
    FROM
        aggs a
        LEFT JOIN first_B_36 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_36 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_36_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_36_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_36_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_36_mean, 
    v.B_36_min,
    v.B_36_max, 
    v.B_36_range,
    v.B_36_sum,
    ISNULL(v.B_36_count, 0) AS B_36_count,
    v.B_36_first, 
    v.B_36_last,
    v.B_36_delta_mean,
    v.B_36_delta_max,
    v.B_36_delta_min,
    v.B_36_delta_pd,
    v.B_36_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_37_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_37 
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
    WHERE B_37 IS NOT NULL
    GROUP BY customer_ID
),
first_B_37 AS
(
    SELECT
        f.customer_ID, s.B_37 AS B_37_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_37 AS
(
    SELECT
        f.customer_ID, s.B_37 AS B_37_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_37_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_37_span
    FROM
        first_last
),
B_37_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_37,
        s.B_37 - LAG(s.B_37, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_37_delta
    FROM
        subset s
),
B_37_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_37_delta
    FROM
        B_37_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_37_delta_per_day AS
(
    SELECT
        customer_ID,
        B_37_delta / date_delta AS B_37_delta_per_day
    FROM
        B_37_delta
),
B_37_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_37_delta_per_day) AS B_37_delta_pd
    FROM
        B_37_delta_per_day
    GROUP BY
        customer_ID
),      
B_37_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_37_delta) AS B_37_delta_mean,
        MAX(B_37_delta) AS B_37_delta_max,
        MIN(B_37_delta) AS B_37_delta_min
    FROM
        B_37_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_37) AS B_37_mean,
        MIN(B_37) AS B_37_min, 
        MAX(B_37) AS B_37_max, 
        SUM(B_37) AS B_37_sum,
        COUNT(B_37) AS B_37_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_37_mean,
        a.B_37_min, 
        a.B_37_max, 
        a.B_37_sum,
        a.B_37_max - a.B_37_min AS B_37_range,
        a.B_37_count,
        f.B_37_first,
        l.B_37_last,
        d.B_37_delta_mean,
        d.B_37_delta_max,
        d.B_37_delta_min,
        pd.B_37_delta_pd,
        cs.B_37_span
    FROM
        aggs a
        LEFT JOIN first_B_37 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_37 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_37_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_37_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_37_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_37_mean, 
    v.B_37_min,
    v.B_37_max, 
    v.B_37_range,
    v.B_37_sum,
    ISNULL(v.B_37_count, 0) AS B_37_count,
    v.B_37_first, 
    v.B_37_last,
    v.B_37_delta_mean,
    v.B_37_delta_max,
    v.B_37_delta_min,
    v.B_37_delta_pd,
    v.B_37_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_26_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_26 
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
    WHERE R_26 IS NOT NULL
    GROUP BY customer_ID
),
first_R_26 AS
(
    SELECT
        f.customer_ID, s.R_26 AS R_26_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_26 AS
(
    SELECT
        f.customer_ID, s.R_26 AS R_26_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_26_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_26_span
    FROM
        first_last
),
R_26_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_26,
        s.R_26 - LAG(s.R_26, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_26_delta
    FROM
        subset s
),
R_26_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_26_delta
    FROM
        R_26_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_26_delta_per_day AS
(
    SELECT
        customer_ID,
        R_26_delta / date_delta AS R_26_delta_per_day
    FROM
        R_26_delta
),
R_26_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_26_delta_per_day) AS R_26_delta_pd
    FROM
        R_26_delta_per_day
    GROUP BY
        customer_ID
),      
R_26_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_26_delta) AS R_26_delta_mean,
        MAX(R_26_delta) AS R_26_delta_max,
        MIN(R_26_delta) AS R_26_delta_min
    FROM
        R_26_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_26) AS R_26_mean,
        MIN(R_26) AS R_26_min, 
        MAX(R_26) AS R_26_max, 
        SUM(R_26) AS R_26_sum,
        COUNT(R_26) AS R_26_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_26_mean,
        a.R_26_min, 
        a.R_26_max, 
        a.R_26_sum,
        a.R_26_max - a.R_26_min AS R_26_range,
        a.R_26_count,
        f.R_26_first,
        l.R_26_last,
        d.R_26_delta_mean,
        d.R_26_delta_max,
        d.R_26_delta_min,
        pd.R_26_delta_pd,
        cs.R_26_span
    FROM
        aggs a
        LEFT JOIN first_R_26 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_26 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_26_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_26_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_26_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_26_mean, 
    v.R_26_min,
    v.R_26_max, 
    v.R_26_range,
    v.R_26_sum,
    ISNULL(v.R_26_count, 0) AS R_26_count,
    v.R_26_first, 
    v.R_26_last,
    v.R_26_delta_mean,
    v.R_26_delta_max,
    v.R_26_delta_min,
    v.R_26_delta_pd,
    v.R_26_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_27_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_27 
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
    WHERE R_27 IS NOT NULL
    GROUP BY customer_ID
),
first_R_27 AS
(
    SELECT
        f.customer_ID, s.R_27 AS R_27_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_27 AS
(
    SELECT
        f.customer_ID, s.R_27 AS R_27_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_27_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_27_span
    FROM
        first_last
),
R_27_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_27,
        s.R_27 - LAG(s.R_27, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_27_delta
    FROM
        subset s
),
R_27_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_27_delta
    FROM
        R_27_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_27_delta_per_day AS
(
    SELECT
        customer_ID,
        R_27_delta / date_delta AS R_27_delta_per_day
    FROM
        R_27_delta
),
R_27_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_27_delta_per_day) AS R_27_delta_pd
    FROM
        R_27_delta_per_day
    GROUP BY
        customer_ID
),      
R_27_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_27_delta) AS R_27_delta_mean,
        MAX(R_27_delta) AS R_27_delta_max,
        MIN(R_27_delta) AS R_27_delta_min
    FROM
        R_27_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_27) AS R_27_mean,
        MIN(R_27) AS R_27_min, 
        MAX(R_27) AS R_27_max, 
        SUM(R_27) AS R_27_sum,
        COUNT(R_27) AS R_27_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_27_mean,
        a.R_27_min, 
        a.R_27_max, 
        a.R_27_sum,
        a.R_27_max - a.R_27_min AS R_27_range,
        a.R_27_count,
        f.R_27_first,
        l.R_27_last,
        d.R_27_delta_mean,
        d.R_27_delta_max,
        d.R_27_delta_min,
        pd.R_27_delta_pd,
        cs.R_27_span
    FROM
        aggs a
        LEFT JOIN first_R_27 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_27 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_27_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_27_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_27_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_27_mean, 
    v.R_27_min,
    v.R_27_max, 
    v.R_27_range,
    v.R_27_sum,
    ISNULL(v.R_27_count, 0) AS R_27_count,
    v.R_27_first, 
    v.R_27_last,
    v.R_27_delta_mean,
    v.R_27_delta_max,
    v.R_27_delta_min,
    v.R_27_delta_pd,
    v.R_27_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_108_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_108 
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
    WHERE D_108 IS NOT NULL
    GROUP BY customer_ID
),
first_D_108 AS
(
    SELECT
        f.customer_ID, s.D_108 AS D_108_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_108 AS
(
    SELECT
        f.customer_ID, s.D_108 AS D_108_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_108_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_108_span
    FROM
        first_last
),
D_108_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_108,
        s.D_108 - LAG(s.D_108, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_108_delta
    FROM
        subset s
),
D_108_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_108_delta
    FROM
        D_108_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_108_delta_per_day AS
(
    SELECT
        customer_ID,
        D_108_delta / date_delta AS D_108_delta_per_day
    FROM
        D_108_delta
),
D_108_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_108_delta_per_day) AS D_108_delta_pd
    FROM
        D_108_delta_per_day
    GROUP BY
        customer_ID
),      
D_108_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_108_delta) AS D_108_delta_mean,
        MAX(D_108_delta) AS D_108_delta_max,
        MIN(D_108_delta) AS D_108_delta_min
    FROM
        D_108_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_108) AS D_108_mean,
        MIN(D_108) AS D_108_min, 
        MAX(D_108) AS D_108_max, 
        SUM(D_108) AS D_108_sum,
        COUNT(D_108) AS D_108_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_108_mean,
        a.D_108_min, 
        a.D_108_max, 
        a.D_108_sum,
        a.D_108_max - a.D_108_min AS D_108_range,
        a.D_108_count,
        f.D_108_first,
        l.D_108_last,
        d.D_108_delta_mean,
        d.D_108_delta_max,
        d.D_108_delta_min,
        pd.D_108_delta_pd,
        cs.D_108_span
    FROM
        aggs a
        LEFT JOIN first_D_108 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_108 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_108_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_108_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_108_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_108_mean, 
    v.D_108_min,
    v.D_108_max, 
    v.D_108_range,
    v.D_108_sum,
    ISNULL(v.D_108_count, 0) AS D_108_count,
    v.D_108_first, 
    v.D_108_last,
    v.D_108_delta_mean,
    v.D_108_delta_max,
    v.D_108_delta_min,
    v.D_108_delta_pd,
    v.D_108_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_109_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_109 
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
    WHERE D_109 IS NOT NULL
    GROUP BY customer_ID
),
first_D_109 AS
(
    SELECT
        f.customer_ID, s.D_109 AS D_109_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_109 AS
(
    SELECT
        f.customer_ID, s.D_109 AS D_109_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_109_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_109_span
    FROM
        first_last
),
D_109_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_109,
        s.D_109 - LAG(s.D_109, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_109_delta
    FROM
        subset s
),
D_109_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_109_delta
    FROM
        D_109_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_109_delta_per_day AS
(
    SELECT
        customer_ID,
        D_109_delta / date_delta AS D_109_delta_per_day
    FROM
        D_109_delta
),
D_109_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_109_delta_per_day) AS D_109_delta_pd
    FROM
        D_109_delta_per_day
    GROUP BY
        customer_ID
),      
D_109_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_109_delta) AS D_109_delta_mean,
        MAX(D_109_delta) AS D_109_delta_max,
        MIN(D_109_delta) AS D_109_delta_min
    FROM
        D_109_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_109) AS D_109_mean,
        MIN(D_109) AS D_109_min, 
        MAX(D_109) AS D_109_max, 
        SUM(D_109) AS D_109_sum,
        COUNT(D_109) AS D_109_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_109_mean,
        a.D_109_min, 
        a.D_109_max, 
        a.D_109_sum,
        a.D_109_max - a.D_109_min AS D_109_range,
        a.D_109_count,
        f.D_109_first,
        l.D_109_last,
        d.D_109_delta_mean,
        d.D_109_delta_max,
        d.D_109_delta_min,
        pd.D_109_delta_pd,
        cs.D_109_span
    FROM
        aggs a
        LEFT JOIN first_D_109 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_109 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_109_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_109_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_109_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_109_mean, 
    v.D_109_min,
    v.D_109_max, 
    v.D_109_range,
    v.D_109_sum,
    ISNULL(v.D_109_count, 0) AS D_109_count,
    v.D_109_first, 
    v.D_109_last,
    v.D_109_delta_mean,
    v.D_109_delta_max,
    v.D_109_delta_min,
    v.D_109_delta_pd,
    v.D_109_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_110_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_110 
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
    WHERE D_110 IS NOT NULL
    GROUP BY customer_ID
),
first_D_110 AS
(
    SELECT
        f.customer_ID, s.D_110 AS D_110_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_110 AS
(
    SELECT
        f.customer_ID, s.D_110 AS D_110_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_110_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_110_span
    FROM
        first_last
),
D_110_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_110,
        s.D_110 - LAG(s.D_110, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_110_delta
    FROM
        subset s
),
D_110_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_110_delta
    FROM
        D_110_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_110_delta_per_day AS
(
    SELECT
        customer_ID,
        D_110_delta / date_delta AS D_110_delta_per_day
    FROM
        D_110_delta
),
D_110_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_110_delta_per_day) AS D_110_delta_pd
    FROM
        D_110_delta_per_day
    GROUP BY
        customer_ID
),      
D_110_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_110_delta) AS D_110_delta_mean,
        MAX(D_110_delta) AS D_110_delta_max,
        MIN(D_110_delta) AS D_110_delta_min
    FROM
        D_110_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_110) AS D_110_mean,
        MIN(D_110) AS D_110_min, 
        MAX(D_110) AS D_110_max, 
        SUM(D_110) AS D_110_sum,
        COUNT(D_110) AS D_110_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_110_mean,
        a.D_110_min, 
        a.D_110_max, 
        a.D_110_sum,
        a.D_110_max - a.D_110_min AS D_110_range,
        a.D_110_count,
        f.D_110_first,
        l.D_110_last,
        d.D_110_delta_mean,
        d.D_110_delta_max,
        d.D_110_delta_min,
        pd.D_110_delta_pd,
        cs.D_110_span
    FROM
        aggs a
        LEFT JOIN first_D_110 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_110 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_110_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_110_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_110_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_110_mean, 
    v.D_110_min,
    v.D_110_max, 
    v.D_110_range,
    v.D_110_sum,
    ISNULL(v.D_110_count, 0) AS D_110_count,
    v.D_110_first, 
    v.D_110_last,
    v.D_110_delta_mean,
    v.D_110_delta_max,
    v.D_110_delta_min,
    v.D_110_delta_pd,
    v.D_110_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_111_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_111 
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
    WHERE D_111 IS NOT NULL
    GROUP BY customer_ID
),
first_D_111 AS
(
    SELECT
        f.customer_ID, s.D_111 AS D_111_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_111 AS
(
    SELECT
        f.customer_ID, s.D_111 AS D_111_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_111_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_111_span
    FROM
        first_last
),
D_111_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_111,
        s.D_111 - LAG(s.D_111, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_111_delta
    FROM
        subset s
),
D_111_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_111_delta
    FROM
        D_111_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_111_delta_per_day AS
(
    SELECT
        customer_ID,
        D_111_delta / date_delta AS D_111_delta_per_day
    FROM
        D_111_delta
),
D_111_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_111_delta_per_day) AS D_111_delta_pd
    FROM
        D_111_delta_per_day
    GROUP BY
        customer_ID
),      
D_111_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_111_delta) AS D_111_delta_mean,
        MAX(D_111_delta) AS D_111_delta_max,
        MIN(D_111_delta) AS D_111_delta_min
    FROM
        D_111_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_111) AS D_111_mean,
        MIN(D_111) AS D_111_min, 
        MAX(D_111) AS D_111_max, 
        SUM(D_111) AS D_111_sum,
        COUNT(D_111) AS D_111_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_111_mean,
        a.D_111_min, 
        a.D_111_max, 
        a.D_111_sum,
        a.D_111_max - a.D_111_min AS D_111_range,
        a.D_111_count,
        f.D_111_first,
        l.D_111_last,
        d.D_111_delta_mean,
        d.D_111_delta_max,
        d.D_111_delta_min,
        pd.D_111_delta_pd,
        cs.D_111_span
    FROM
        aggs a
        LEFT JOIN first_D_111 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_111 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_111_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_111_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_111_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_111_mean, 
    v.D_111_min,
    v.D_111_max, 
    v.D_111_range,
    v.D_111_sum,
    ISNULL(v.D_111_count, 0) AS D_111_count,
    v.D_111_first, 
    v.D_111_last,
    v.D_111_delta_mean,
    v.D_111_delta_max,
    v.D_111_delta_min,
    v.D_111_delta_pd,
    v.D_111_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_39_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_39 
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
    WHERE B_39 IS NOT NULL
    GROUP BY customer_ID
),
first_B_39 AS
(
    SELECT
        f.customer_ID, s.B_39 AS B_39_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_39 AS
(
    SELECT
        f.customer_ID, s.B_39 AS B_39_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_39_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_39_span
    FROM
        first_last
),
B_39_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_39,
        s.B_39 - LAG(s.B_39, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_39_delta
    FROM
        subset s
),
B_39_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_39_delta
    FROM
        B_39_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_39_delta_per_day AS
(
    SELECT
        customer_ID,
        B_39_delta / date_delta AS B_39_delta_per_day
    FROM
        B_39_delta
),
B_39_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_39_delta_per_day) AS B_39_delta_pd
    FROM
        B_39_delta_per_day
    GROUP BY
        customer_ID
),      
B_39_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_39_delta) AS B_39_delta_mean,
        MAX(B_39_delta) AS B_39_delta_max,
        MIN(B_39_delta) AS B_39_delta_min
    FROM
        B_39_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_39) AS B_39_mean,
        MIN(B_39) AS B_39_min, 
        MAX(B_39) AS B_39_max, 
        SUM(B_39) AS B_39_sum,
        COUNT(B_39) AS B_39_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_39_mean,
        a.B_39_min, 
        a.B_39_max, 
        a.B_39_sum,
        a.B_39_max - a.B_39_min AS B_39_range,
        a.B_39_count,
        f.B_39_first,
        l.B_39_last,
        d.B_39_delta_mean,
        d.B_39_delta_max,
        d.B_39_delta_min,
        pd.B_39_delta_pd,
        cs.B_39_span
    FROM
        aggs a
        LEFT JOIN first_B_39 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_39 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_39_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_39_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_39_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_39_mean, 
    v.B_39_min,
    v.B_39_max, 
    v.B_39_range,
    v.B_39_sum,
    ISNULL(v.B_39_count, 0) AS B_39_count,
    v.B_39_first, 
    v.B_39_last,
    v.B_39_delta_mean,
    v.B_39_delta_max,
    v.B_39_delta_min,
    v.B_39_delta_pd,
    v.B_39_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_112_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_112 
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
    WHERE D_112 IS NOT NULL
    GROUP BY customer_ID
),
first_D_112 AS
(
    SELECT
        f.customer_ID, s.D_112 AS D_112_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_112 AS
(
    SELECT
        f.customer_ID, s.D_112 AS D_112_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_112_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_112_span
    FROM
        first_last
),
D_112_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_112,
        s.D_112 - LAG(s.D_112, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_112_delta
    FROM
        subset s
),
D_112_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_112_delta
    FROM
        D_112_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_112_delta_per_day AS
(
    SELECT
        customer_ID,
        D_112_delta / date_delta AS D_112_delta_per_day
    FROM
        D_112_delta
),
D_112_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_112_delta_per_day) AS D_112_delta_pd
    FROM
        D_112_delta_per_day
    GROUP BY
        customer_ID
),      
D_112_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_112_delta) AS D_112_delta_mean,
        MAX(D_112_delta) AS D_112_delta_max,
        MIN(D_112_delta) AS D_112_delta_min
    FROM
        D_112_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_112) AS D_112_mean,
        MIN(D_112) AS D_112_min, 
        MAX(D_112) AS D_112_max, 
        SUM(D_112) AS D_112_sum,
        COUNT(D_112) AS D_112_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_112_mean,
        a.D_112_min, 
        a.D_112_max, 
        a.D_112_sum,
        a.D_112_max - a.D_112_min AS D_112_range,
        a.D_112_count,
        f.D_112_first,
        l.D_112_last,
        d.D_112_delta_mean,
        d.D_112_delta_max,
        d.D_112_delta_min,
        pd.D_112_delta_pd,
        cs.D_112_span
    FROM
        aggs a
        LEFT JOIN first_D_112 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_112 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_112_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_112_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_112_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_112_mean, 
    v.D_112_min,
    v.D_112_max, 
    v.D_112_range,
    v.D_112_sum,
    ISNULL(v.D_112_count, 0) AS D_112_count,
    v.D_112_first, 
    v.D_112_last,
    v.D_112_delta_mean,
    v.D_112_delta_max,
    v.D_112_delta_min,
    v.D_112_delta_pd,
    v.D_112_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_40_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_40 
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
    WHERE B_40 IS NOT NULL
    GROUP BY customer_ID
),
first_B_40 AS
(
    SELECT
        f.customer_ID, s.B_40 AS B_40_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_40 AS
(
    SELECT
        f.customer_ID, s.B_40 AS B_40_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_40_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_40_span
    FROM
        first_last
),
B_40_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_40,
        s.B_40 - LAG(s.B_40, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_40_delta
    FROM
        subset s
),
B_40_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_40_delta
    FROM
        B_40_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_40_delta_per_day AS
(
    SELECT
        customer_ID,
        B_40_delta / date_delta AS B_40_delta_per_day
    FROM
        B_40_delta
),
B_40_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_40_delta_per_day) AS B_40_delta_pd
    FROM
        B_40_delta_per_day
    GROUP BY
        customer_ID
),      
B_40_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_40_delta) AS B_40_delta_mean,
        MAX(B_40_delta) AS B_40_delta_max,
        MIN(B_40_delta) AS B_40_delta_min
    FROM
        B_40_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_40) AS B_40_mean,
        MIN(B_40) AS B_40_min, 
        MAX(B_40) AS B_40_max, 
        SUM(B_40) AS B_40_sum,
        COUNT(B_40) AS B_40_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_40_mean,
        a.B_40_min, 
        a.B_40_max, 
        a.B_40_sum,
        a.B_40_max - a.B_40_min AS B_40_range,
        a.B_40_count,
        f.B_40_first,
        l.B_40_last,
        d.B_40_delta_mean,
        d.B_40_delta_max,
        d.B_40_delta_min,
        pd.B_40_delta_pd,
        cs.B_40_span
    FROM
        aggs a
        LEFT JOIN first_B_40 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_40 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_40_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_40_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_40_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_40_mean, 
    v.B_40_min,
    v.B_40_max, 
    v.B_40_range,
    v.B_40_sum,
    ISNULL(v.B_40_count, 0) AS B_40_count,
    v.B_40_first, 
    v.B_40_last,
    v.B_40_delta_mean,
    v.B_40_delta_max,
    v.B_40_delta_min,
    v.B_40_delta_pd,
    v.B_40_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_S_27_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.S_27 
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
    WHERE S_27 IS NOT NULL
    GROUP BY customer_ID
),
first_S_27 AS
(
    SELECT
        f.customer_ID, s.S_27 AS S_27_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_S_27 AS
(
    SELECT
        f.customer_ID, s.S_27 AS S_27_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
S_27_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS S_27_span
    FROM
        first_last
),
S_27_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.S_27,
        s.S_27 - LAG(s.S_27, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS S_27_delta
    FROM
        subset s
),
S_27_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.S_27_delta
    FROM
        S_27_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
S_27_delta_per_day AS
(
    SELECT
        customer_ID,
        S_27_delta / date_delta AS S_27_delta_per_day
    FROM
        S_27_delta
),
S_27_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(S_27_delta_per_day) AS S_27_delta_pd
    FROM
        S_27_delta_per_day
    GROUP BY
        customer_ID
),      
S_27_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(S_27_delta) AS S_27_delta_mean,
        MAX(S_27_delta) AS S_27_delta_max,
        MIN(S_27_delta) AS S_27_delta_min
    FROM
        S_27_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(S_27) AS S_27_mean,
        MIN(S_27) AS S_27_min, 
        MAX(S_27) AS S_27_max, 
        SUM(S_27) AS S_27_sum,
        COUNT(S_27) AS S_27_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.S_27_mean,
        a.S_27_min, 
        a.S_27_max, 
        a.S_27_sum,
        a.S_27_max - a.S_27_min AS S_27_range,
        a.S_27_count,
        f.S_27_first,
        l.S_27_last,
        d.S_27_delta_mean,
        d.S_27_delta_max,
        d.S_27_delta_min,
        pd.S_27_delta_pd,
        cs.S_27_span
    FROM
        aggs a
        LEFT JOIN first_S_27 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_S_27 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN S_27_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN S_27_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN S_27_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.S_27_mean, 
    v.S_27_min,
    v.S_27_max, 
    v.S_27_range,
    v.S_27_sum,
    ISNULL(v.S_27_count, 0) AS S_27_count,
    v.S_27_first, 
    v.S_27_last,
    v.S_27_delta_mean,
    v.S_27_delta_max,
    v.S_27_delta_min,
    v.S_27_delta_pd,
    v.S_27_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_113_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_113 
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
    WHERE D_113 IS NOT NULL
    GROUP BY customer_ID
),
first_D_113 AS
(
    SELECT
        f.customer_ID, s.D_113 AS D_113_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_113 AS
(
    SELECT
        f.customer_ID, s.D_113 AS D_113_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_113_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_113_span
    FROM
        first_last
),
D_113_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_113,
        s.D_113 - LAG(s.D_113, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_113_delta
    FROM
        subset s
),
D_113_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_113_delta
    FROM
        D_113_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_113_delta_per_day AS
(
    SELECT
        customer_ID,
        D_113_delta / date_delta AS D_113_delta_per_day
    FROM
        D_113_delta
),
D_113_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_113_delta_per_day) AS D_113_delta_pd
    FROM
        D_113_delta_per_day
    GROUP BY
        customer_ID
),      
D_113_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_113_delta) AS D_113_delta_mean,
        MAX(D_113_delta) AS D_113_delta_max,
        MIN(D_113_delta) AS D_113_delta_min
    FROM
        D_113_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_113) AS D_113_mean,
        MIN(D_113) AS D_113_min, 
        MAX(D_113) AS D_113_max, 
        SUM(D_113) AS D_113_sum,
        COUNT(D_113) AS D_113_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_113_mean,
        a.D_113_min, 
        a.D_113_max, 
        a.D_113_sum,
        a.D_113_max - a.D_113_min AS D_113_range,
        a.D_113_count,
        f.D_113_first,
        l.D_113_last,
        d.D_113_delta_mean,
        d.D_113_delta_max,
        d.D_113_delta_min,
        pd.D_113_delta_pd,
        cs.D_113_span
    FROM
        aggs a
        LEFT JOIN first_D_113 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_113 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_113_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_113_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_113_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_113_mean, 
    v.D_113_min,
    v.D_113_max, 
    v.D_113_range,
    v.D_113_sum,
    ISNULL(v.D_113_count, 0) AS D_113_count,
    v.D_113_first, 
    v.D_113_last,
    v.D_113_delta_mean,
    v.D_113_delta_max,
    v.D_113_delta_min,
    v.D_113_delta_pd,
    v.D_113_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_115_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_115 
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
    WHERE D_115 IS NOT NULL
    GROUP BY customer_ID
),
first_D_115 AS
(
    SELECT
        f.customer_ID, s.D_115 AS D_115_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_115 AS
(
    SELECT
        f.customer_ID, s.D_115 AS D_115_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_115_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_115_span
    FROM
        first_last
),
D_115_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_115,
        s.D_115 - LAG(s.D_115, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_115_delta
    FROM
        subset s
),
D_115_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_115_delta
    FROM
        D_115_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_115_delta_per_day AS
(
    SELECT
        customer_ID,
        D_115_delta / date_delta AS D_115_delta_per_day
    FROM
        D_115_delta
),
D_115_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_115_delta_per_day) AS D_115_delta_pd
    FROM
        D_115_delta_per_day
    GROUP BY
        customer_ID
),      
D_115_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_115_delta) AS D_115_delta_mean,
        MAX(D_115_delta) AS D_115_delta_max,
        MIN(D_115_delta) AS D_115_delta_min
    FROM
        D_115_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_115) AS D_115_mean,
        MIN(D_115) AS D_115_min, 
        MAX(D_115) AS D_115_max, 
        SUM(D_115) AS D_115_sum,
        COUNT(D_115) AS D_115_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_115_mean,
        a.D_115_min, 
        a.D_115_max, 
        a.D_115_sum,
        a.D_115_max - a.D_115_min AS D_115_range,
        a.D_115_count,
        f.D_115_first,
        l.D_115_last,
        d.D_115_delta_mean,
        d.D_115_delta_max,
        d.D_115_delta_min,
        pd.D_115_delta_pd,
        cs.D_115_span
    FROM
        aggs a
        LEFT JOIN first_D_115 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_115 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_115_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_115_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_115_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_115_mean, 
    v.D_115_min,
    v.D_115_max, 
    v.D_115_range,
    v.D_115_sum,
    ISNULL(v.D_115_count, 0) AS D_115_count,
    v.D_115_first, 
    v.D_115_last,
    v.D_115_delta_mean,
    v.D_115_delta_max,
    v.D_115_delta_min,
    v.D_115_delta_pd,
    v.D_115_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_118_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_118 
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
    WHERE D_118 IS NOT NULL
    GROUP BY customer_ID
),
first_D_118 AS
(
    SELECT
        f.customer_ID, s.D_118 AS D_118_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_118 AS
(
    SELECT
        f.customer_ID, s.D_118 AS D_118_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_118_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_118_span
    FROM
        first_last
),
D_118_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_118,
        s.D_118 - LAG(s.D_118, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_118_delta
    FROM
        subset s
),
D_118_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_118_delta
    FROM
        D_118_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_118_delta_per_day AS
(
    SELECT
        customer_ID,
        D_118_delta / date_delta AS D_118_delta_per_day
    FROM
        D_118_delta
),
D_118_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_118_delta_per_day) AS D_118_delta_pd
    FROM
        D_118_delta_per_day
    GROUP BY
        customer_ID
),      
D_118_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_118_delta) AS D_118_delta_mean,
        MAX(D_118_delta) AS D_118_delta_max,
        MIN(D_118_delta) AS D_118_delta_min
    FROM
        D_118_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_118) AS D_118_mean,
        MIN(D_118) AS D_118_min, 
        MAX(D_118) AS D_118_max, 
        SUM(D_118) AS D_118_sum,
        COUNT(D_118) AS D_118_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_118_mean,
        a.D_118_min, 
        a.D_118_max, 
        a.D_118_sum,
        a.D_118_max - a.D_118_min AS D_118_range,
        a.D_118_count,
        f.D_118_first,
        l.D_118_last,
        d.D_118_delta_mean,
        d.D_118_delta_max,
        d.D_118_delta_min,
        pd.D_118_delta_pd,
        cs.D_118_span
    FROM
        aggs a
        LEFT JOIN first_D_118 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_118 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_118_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_118_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_118_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_118_mean, 
    v.D_118_min,
    v.D_118_max, 
    v.D_118_range,
    v.D_118_sum,
    ISNULL(v.D_118_count, 0) AS D_118_count,
    v.D_118_first, 
    v.D_118_last,
    v.D_118_delta_mean,
    v.D_118_delta_max,
    v.D_118_delta_min,
    v.D_118_delta_pd,
    v.D_118_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_119_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_119 
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
    WHERE D_119 IS NOT NULL
    GROUP BY customer_ID
),
first_D_119 AS
(
    SELECT
        f.customer_ID, s.D_119 AS D_119_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_119 AS
(
    SELECT
        f.customer_ID, s.D_119 AS D_119_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_119_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_119_span
    FROM
        first_last
),
D_119_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_119,
        s.D_119 - LAG(s.D_119, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_119_delta
    FROM
        subset s
),
D_119_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_119_delta
    FROM
        D_119_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_119_delta_per_day AS
(
    SELECT
        customer_ID,
        D_119_delta / date_delta AS D_119_delta_per_day
    FROM
        D_119_delta
),
D_119_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_119_delta_per_day) AS D_119_delta_pd
    FROM
        D_119_delta_per_day
    GROUP BY
        customer_ID
),      
D_119_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_119_delta) AS D_119_delta_mean,
        MAX(D_119_delta) AS D_119_delta_max,
        MIN(D_119_delta) AS D_119_delta_min
    FROM
        D_119_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_119) AS D_119_mean,
        MIN(D_119) AS D_119_min, 
        MAX(D_119) AS D_119_max, 
        SUM(D_119) AS D_119_sum,
        COUNT(D_119) AS D_119_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_119_mean,
        a.D_119_min, 
        a.D_119_max, 
        a.D_119_sum,
        a.D_119_max - a.D_119_min AS D_119_range,
        a.D_119_count,
        f.D_119_first,
        l.D_119_last,
        d.D_119_delta_mean,
        d.D_119_delta_max,
        d.D_119_delta_min,
        pd.D_119_delta_pd,
        cs.D_119_span
    FROM
        aggs a
        LEFT JOIN first_D_119 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_119 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_119_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_119_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_119_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_119_mean, 
    v.D_119_min,
    v.D_119_max, 
    v.D_119_range,
    v.D_119_sum,
    ISNULL(v.D_119_count, 0) AS D_119_count,
    v.D_119_first, 
    v.D_119_last,
    v.D_119_delta_mean,
    v.D_119_delta_max,
    v.D_119_delta_min,
    v.D_119_delta_pd,
    v.D_119_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_121_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_121 
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
    WHERE D_121 IS NOT NULL
    GROUP BY customer_ID
),
first_D_121 AS
(
    SELECT
        f.customer_ID, s.D_121 AS D_121_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_121 AS
(
    SELECT
        f.customer_ID, s.D_121 AS D_121_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_121_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_121_span
    FROM
        first_last
),
D_121_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_121,
        s.D_121 - LAG(s.D_121, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_121_delta
    FROM
        subset s
),
D_121_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_121_delta
    FROM
        D_121_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_121_delta_per_day AS
(
    SELECT
        customer_ID,
        D_121_delta / date_delta AS D_121_delta_per_day
    FROM
        D_121_delta
),
D_121_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_121_delta_per_day) AS D_121_delta_pd
    FROM
        D_121_delta_per_day
    GROUP BY
        customer_ID
),      
D_121_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_121_delta) AS D_121_delta_mean,
        MAX(D_121_delta) AS D_121_delta_max,
        MIN(D_121_delta) AS D_121_delta_min
    FROM
        D_121_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_121) AS D_121_mean,
        MIN(D_121) AS D_121_min, 
        MAX(D_121) AS D_121_max, 
        SUM(D_121) AS D_121_sum,
        COUNT(D_121) AS D_121_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_121_mean,
        a.D_121_min, 
        a.D_121_max, 
        a.D_121_sum,
        a.D_121_max - a.D_121_min AS D_121_range,
        a.D_121_count,
        f.D_121_first,
        l.D_121_last,
        d.D_121_delta_mean,
        d.D_121_delta_max,
        d.D_121_delta_min,
        pd.D_121_delta_pd,
        cs.D_121_span
    FROM
        aggs a
        LEFT JOIN first_D_121 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_121 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_121_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_121_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_121_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_121_mean, 
    v.D_121_min,
    v.D_121_max, 
    v.D_121_range,
    v.D_121_sum,
    ISNULL(v.D_121_count, 0) AS D_121_count,
    v.D_121_first, 
    v.D_121_last,
    v.D_121_delta_mean,
    v.D_121_delta_max,
    v.D_121_delta_min,
    v.D_121_delta_pd,
    v.D_121_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_122_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_122 
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
    WHERE D_122 IS NOT NULL
    GROUP BY customer_ID
),
first_D_122 AS
(
    SELECT
        f.customer_ID, s.D_122 AS D_122_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_122 AS
(
    SELECT
        f.customer_ID, s.D_122 AS D_122_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_122_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_122_span
    FROM
        first_last
),
D_122_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_122,
        s.D_122 - LAG(s.D_122, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_122_delta
    FROM
        subset s
),
D_122_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_122_delta
    FROM
        D_122_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_122_delta_per_day AS
(
    SELECT
        customer_ID,
        D_122_delta / date_delta AS D_122_delta_per_day
    FROM
        D_122_delta
),
D_122_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_122_delta_per_day) AS D_122_delta_pd
    FROM
        D_122_delta_per_day
    GROUP BY
        customer_ID
),      
D_122_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_122_delta) AS D_122_delta_mean,
        MAX(D_122_delta) AS D_122_delta_max,
        MIN(D_122_delta) AS D_122_delta_min
    FROM
        D_122_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_122) AS D_122_mean,
        MIN(D_122) AS D_122_min, 
        MAX(D_122) AS D_122_max, 
        SUM(D_122) AS D_122_sum,
        COUNT(D_122) AS D_122_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_122_mean,
        a.D_122_min, 
        a.D_122_max, 
        a.D_122_sum,
        a.D_122_max - a.D_122_min AS D_122_range,
        a.D_122_count,
        f.D_122_first,
        l.D_122_last,
        d.D_122_delta_mean,
        d.D_122_delta_max,
        d.D_122_delta_min,
        pd.D_122_delta_pd,
        cs.D_122_span
    FROM
        aggs a
        LEFT JOIN first_D_122 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_122 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_122_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_122_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_122_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_122_mean, 
    v.D_122_min,
    v.D_122_max, 
    v.D_122_range,
    v.D_122_sum,
    ISNULL(v.D_122_count, 0) AS D_122_count,
    v.D_122_first, 
    v.D_122_last,
    v.D_122_delta_mean,
    v.D_122_delta_max,
    v.D_122_delta_min,
    v.D_122_delta_pd,
    v.D_122_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_123_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_123 
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
    WHERE D_123 IS NOT NULL
    GROUP BY customer_ID
),
first_D_123 AS
(
    SELECT
        f.customer_ID, s.D_123 AS D_123_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_123 AS
(
    SELECT
        f.customer_ID, s.D_123 AS D_123_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_123_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_123_span
    FROM
        first_last
),
D_123_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_123,
        s.D_123 - LAG(s.D_123, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_123_delta
    FROM
        subset s
),
D_123_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_123_delta
    FROM
        D_123_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_123_delta_per_day AS
(
    SELECT
        customer_ID,
        D_123_delta / date_delta AS D_123_delta_per_day
    FROM
        D_123_delta
),
D_123_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_123_delta_per_day) AS D_123_delta_pd
    FROM
        D_123_delta_per_day
    GROUP BY
        customer_ID
),      
D_123_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_123_delta) AS D_123_delta_mean,
        MAX(D_123_delta) AS D_123_delta_max,
        MIN(D_123_delta) AS D_123_delta_min
    FROM
        D_123_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_123) AS D_123_mean,
        MIN(D_123) AS D_123_min, 
        MAX(D_123) AS D_123_max, 
        SUM(D_123) AS D_123_sum,
        COUNT(D_123) AS D_123_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_123_mean,
        a.D_123_min, 
        a.D_123_max, 
        a.D_123_sum,
        a.D_123_max - a.D_123_min AS D_123_range,
        a.D_123_count,
        f.D_123_first,
        l.D_123_last,
        d.D_123_delta_mean,
        d.D_123_delta_max,
        d.D_123_delta_min,
        pd.D_123_delta_pd,
        cs.D_123_span
    FROM
        aggs a
        LEFT JOIN first_D_123 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_123 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_123_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_123_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_123_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_123_mean, 
    v.D_123_min,
    v.D_123_max, 
    v.D_123_range,
    v.D_123_sum,
    ISNULL(v.D_123_count, 0) AS D_123_count,
    v.D_123_first, 
    v.D_123_last,
    v.D_123_delta_mean,
    v.D_123_delta_max,
    v.D_123_delta_min,
    v.D_123_delta_pd,
    v.D_123_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_124_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_124 
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
    WHERE D_124 IS NOT NULL
    GROUP BY customer_ID
),
first_D_124 AS
(
    SELECT
        f.customer_ID, s.D_124 AS D_124_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_124 AS
(
    SELECT
        f.customer_ID, s.D_124 AS D_124_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_124_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_124_span
    FROM
        first_last
),
D_124_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_124,
        s.D_124 - LAG(s.D_124, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_124_delta
    FROM
        subset s
),
D_124_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_124_delta
    FROM
        D_124_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_124_delta_per_day AS
(
    SELECT
        customer_ID,
        D_124_delta / date_delta AS D_124_delta_per_day
    FROM
        D_124_delta
),
D_124_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_124_delta_per_day) AS D_124_delta_pd
    FROM
        D_124_delta_per_day
    GROUP BY
        customer_ID
),      
D_124_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_124_delta) AS D_124_delta_mean,
        MAX(D_124_delta) AS D_124_delta_max,
        MIN(D_124_delta) AS D_124_delta_min
    FROM
        D_124_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_124) AS D_124_mean,
        MIN(D_124) AS D_124_min, 
        MAX(D_124) AS D_124_max, 
        SUM(D_124) AS D_124_sum,
        COUNT(D_124) AS D_124_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_124_mean,
        a.D_124_min, 
        a.D_124_max, 
        a.D_124_sum,
        a.D_124_max - a.D_124_min AS D_124_range,
        a.D_124_count,
        f.D_124_first,
        l.D_124_last,
        d.D_124_delta_mean,
        d.D_124_delta_max,
        d.D_124_delta_min,
        pd.D_124_delta_pd,
        cs.D_124_span
    FROM
        aggs a
        LEFT JOIN first_D_124 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_124 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_124_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_124_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_124_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_124_mean, 
    v.D_124_min,
    v.D_124_max, 
    v.D_124_range,
    v.D_124_sum,
    ISNULL(v.D_124_count, 0) AS D_124_count,
    v.D_124_first, 
    v.D_124_last,
    v.D_124_delta_mean,
    v.D_124_delta_max,
    v.D_124_delta_min,
    v.D_124_delta_pd,
    v.D_124_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_125_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_125 
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
    WHERE D_125 IS NOT NULL
    GROUP BY customer_ID
),
first_D_125 AS
(
    SELECT
        f.customer_ID, s.D_125 AS D_125_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_125 AS
(
    SELECT
        f.customer_ID, s.D_125 AS D_125_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_125_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_125_span
    FROM
        first_last
),
D_125_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_125,
        s.D_125 - LAG(s.D_125, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_125_delta
    FROM
        subset s
),
D_125_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_125_delta
    FROM
        D_125_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_125_delta_per_day AS
(
    SELECT
        customer_ID,
        D_125_delta / date_delta AS D_125_delta_per_day
    FROM
        D_125_delta
),
D_125_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_125_delta_per_day) AS D_125_delta_pd
    FROM
        D_125_delta_per_day
    GROUP BY
        customer_ID
),      
D_125_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_125_delta) AS D_125_delta_mean,
        MAX(D_125_delta) AS D_125_delta_max,
        MIN(D_125_delta) AS D_125_delta_min
    FROM
        D_125_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_125) AS D_125_mean,
        MIN(D_125) AS D_125_min, 
        MAX(D_125) AS D_125_max, 
        SUM(D_125) AS D_125_sum,
        COUNT(D_125) AS D_125_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_125_mean,
        a.D_125_min, 
        a.D_125_max, 
        a.D_125_sum,
        a.D_125_max - a.D_125_min AS D_125_range,
        a.D_125_count,
        f.D_125_first,
        l.D_125_last,
        d.D_125_delta_mean,
        d.D_125_delta_max,
        d.D_125_delta_min,
        pd.D_125_delta_pd,
        cs.D_125_span
    FROM
        aggs a
        LEFT JOIN first_D_125 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_125 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_125_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_125_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_125_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_125_mean, 
    v.D_125_min,
    v.D_125_max, 
    v.D_125_range,
    v.D_125_sum,
    ISNULL(v.D_125_count, 0) AS D_125_count,
    v.D_125_first, 
    v.D_125_last,
    v.D_125_delta_mean,
    v.D_125_delta_max,
    v.D_125_delta_min,
    v.D_125_delta_pd,
    v.D_125_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_127_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_127 
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
    WHERE D_127 IS NOT NULL
    GROUP BY customer_ID
),
first_D_127 AS
(
    SELECT
        f.customer_ID, s.D_127 AS D_127_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_127 AS
(
    SELECT
        f.customer_ID, s.D_127 AS D_127_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_127_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_127_span
    FROM
        first_last
),
D_127_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_127,
        s.D_127 - LAG(s.D_127, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_127_delta
    FROM
        subset s
),
D_127_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_127_delta
    FROM
        D_127_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_127_delta_per_day AS
(
    SELECT
        customer_ID,
        D_127_delta / date_delta AS D_127_delta_per_day
    FROM
        D_127_delta
),
D_127_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_127_delta_per_day) AS D_127_delta_pd
    FROM
        D_127_delta_per_day
    GROUP BY
        customer_ID
),      
D_127_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_127_delta) AS D_127_delta_mean,
        MAX(D_127_delta) AS D_127_delta_max,
        MIN(D_127_delta) AS D_127_delta_min
    FROM
        D_127_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_127) AS D_127_mean,
        MIN(D_127) AS D_127_min, 
        MAX(D_127) AS D_127_max, 
        SUM(D_127) AS D_127_sum,
        COUNT(D_127) AS D_127_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_127_mean,
        a.D_127_min, 
        a.D_127_max, 
        a.D_127_sum,
        a.D_127_max - a.D_127_min AS D_127_range,
        a.D_127_count,
        f.D_127_first,
        l.D_127_last,
        d.D_127_delta_mean,
        d.D_127_delta_max,
        d.D_127_delta_min,
        pd.D_127_delta_pd,
        cs.D_127_span
    FROM
        aggs a
        LEFT JOIN first_D_127 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_127 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_127_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_127_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_127_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_127_mean, 
    v.D_127_min,
    v.D_127_max, 
    v.D_127_range,
    v.D_127_sum,
    ISNULL(v.D_127_count, 0) AS D_127_count,
    v.D_127_first, 
    v.D_127_last,
    v.D_127_delta_mean,
    v.D_127_delta_max,
    v.D_127_delta_min,
    v.D_127_delta_pd,
    v.D_127_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_128_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_128 
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
    WHERE D_128 IS NOT NULL
    GROUP BY customer_ID
),
first_D_128 AS
(
    SELECT
        f.customer_ID, s.D_128 AS D_128_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_128 AS
(
    SELECT
        f.customer_ID, s.D_128 AS D_128_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_128_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_128_span
    FROM
        first_last
),
D_128_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_128,
        s.D_128 - LAG(s.D_128, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_128_delta
    FROM
        subset s
),
D_128_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_128_delta
    FROM
        D_128_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_128_delta_per_day AS
(
    SELECT
        customer_ID,
        D_128_delta / date_delta AS D_128_delta_per_day
    FROM
        D_128_delta
),
D_128_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_128_delta_per_day) AS D_128_delta_pd
    FROM
        D_128_delta_per_day
    GROUP BY
        customer_ID
),      
D_128_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_128_delta) AS D_128_delta_mean,
        MAX(D_128_delta) AS D_128_delta_max,
        MIN(D_128_delta) AS D_128_delta_min
    FROM
        D_128_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_128) AS D_128_mean,
        MIN(D_128) AS D_128_min, 
        MAX(D_128) AS D_128_max, 
        SUM(D_128) AS D_128_sum,
        COUNT(D_128) AS D_128_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_128_mean,
        a.D_128_min, 
        a.D_128_max, 
        a.D_128_sum,
        a.D_128_max - a.D_128_min AS D_128_range,
        a.D_128_count,
        f.D_128_first,
        l.D_128_last,
        d.D_128_delta_mean,
        d.D_128_delta_max,
        d.D_128_delta_min,
        pd.D_128_delta_pd,
        cs.D_128_span
    FROM
        aggs a
        LEFT JOIN first_D_128 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_128 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_128_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_128_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_128_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_128_mean, 
    v.D_128_min,
    v.D_128_max, 
    v.D_128_range,
    v.D_128_sum,
    ISNULL(v.D_128_count, 0) AS D_128_count,
    v.D_128_first, 
    v.D_128_last,
    v.D_128_delta_mean,
    v.D_128_delta_max,
    v.D_128_delta_min,
    v.D_128_delta_pd,
    v.D_128_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

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
GO

CREATE VIEW test_data_B_41_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_41 
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
    WHERE B_41 IS NOT NULL
    GROUP BY customer_ID
),
first_B_41 AS
(
    SELECT
        f.customer_ID, s.B_41 AS B_41_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_B_41 AS
(
    SELECT
        f.customer_ID, s.B_41 AS B_41_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
B_41_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS B_41_span
    FROM
        first_last
),
B_41_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.B_41,
        s.B_41 - LAG(s.B_41, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS B_41_delta
    FROM
        subset s
),
B_41_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.B_41_delta
    FROM
        B_41_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
B_41_delta_per_day AS
(
    SELECT
        customer_ID,
        B_41_delta / date_delta AS B_41_delta_per_day
    FROM
        B_41_delta
),
B_41_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(B_41_delta_per_day) AS B_41_delta_pd
    FROM
        B_41_delta_per_day
    GROUP BY
        customer_ID
),      
B_41_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(B_41_delta) AS B_41_delta_mean,
        MAX(B_41_delta) AS B_41_delta_max,
        MIN(B_41_delta) AS B_41_delta_min
    FROM
        B_41_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(B_41) AS B_41_mean,
        MIN(B_41) AS B_41_min, 
        MAX(B_41) AS B_41_max, 
        SUM(B_41) AS B_41_sum,
        COUNT(B_41) AS B_41_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.B_41_mean,
        a.B_41_min, 
        a.B_41_max, 
        a.B_41_sum,
        a.B_41_max - a.B_41_min AS B_41_range,
        a.B_41_count,
        f.B_41_first,
        l.B_41_last,
        d.B_41_delta_mean,
        d.B_41_delta_max,
        d.B_41_delta_min,
        pd.B_41_delta_pd,
        cs.B_41_span
    FROM
        aggs a
        LEFT JOIN first_B_41 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_B_41 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN B_41_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN B_41_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN B_41_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.B_41_mean, 
    v.B_41_min,
    v.B_41_max, 
    v.B_41_range,
    v.B_41_sum,
    ISNULL(v.B_41_count, 0) AS B_41_count,
    v.B_41_first, 
    v.B_41_last,
    v.B_41_delta_mean,
    v.B_41_delta_max,
    v.B_41_delta_min,
    v.B_41_delta_pd,
    v.B_41_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_B_42_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.B_42 
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
GO

CREATE VIEW test_data_D_130_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_130 
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
    WHERE D_130 IS NOT NULL
    GROUP BY customer_ID
),
first_D_130 AS
(
    SELECT
        f.customer_ID, s.D_130 AS D_130_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_130 AS
(
    SELECT
        f.customer_ID, s.D_130 AS D_130_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_130_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_130_span
    FROM
        first_last
),
D_130_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_130,
        s.D_130 - LAG(s.D_130, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_130_delta
    FROM
        subset s
),
D_130_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_130_delta
    FROM
        D_130_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_130_delta_per_day AS
(
    SELECT
        customer_ID,
        D_130_delta / date_delta AS D_130_delta_per_day
    FROM
        D_130_delta
),
D_130_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_130_delta_per_day) AS D_130_delta_pd
    FROM
        D_130_delta_per_day
    GROUP BY
        customer_ID
),      
D_130_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_130_delta) AS D_130_delta_mean,
        MAX(D_130_delta) AS D_130_delta_max,
        MIN(D_130_delta) AS D_130_delta_min
    FROM
        D_130_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_130) AS D_130_mean,
        MIN(D_130) AS D_130_min, 
        MAX(D_130) AS D_130_max, 
        SUM(D_130) AS D_130_sum,
        COUNT(D_130) AS D_130_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_130_mean,
        a.D_130_min, 
        a.D_130_max, 
        a.D_130_sum,
        a.D_130_max - a.D_130_min AS D_130_range,
        a.D_130_count,
        f.D_130_first,
        l.D_130_last,
        d.D_130_delta_mean,
        d.D_130_delta_max,
        d.D_130_delta_min,
        pd.D_130_delta_pd,
        cs.D_130_span
    FROM
        aggs a
        LEFT JOIN first_D_130 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_130 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_130_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_130_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_130_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_130_mean, 
    v.D_130_min,
    v.D_130_max, 
    v.D_130_range,
    v.D_130_sum,
    ISNULL(v.D_130_count, 0) AS D_130_count,
    v.D_130_first, 
    v.D_130_last,
    v.D_130_delta_mean,
    v.D_130_delta_max,
    v.D_130_delta_min,
    v.D_130_delta_pd,
    v.D_130_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_131_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_131 
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
    WHERE D_131 IS NOT NULL
    GROUP BY customer_ID
),
first_D_131 AS
(
    SELECT
        f.customer_ID, s.D_131 AS D_131_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_131 AS
(
    SELECT
        f.customer_ID, s.D_131 AS D_131_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_131_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_131_span
    FROM
        first_last
),
D_131_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_131,
        s.D_131 - LAG(s.D_131, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_131_delta
    FROM
        subset s
),
D_131_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_131_delta
    FROM
        D_131_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_131_delta_per_day AS
(
    SELECT
        customer_ID,
        D_131_delta / date_delta AS D_131_delta_per_day
    FROM
        D_131_delta
),
D_131_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_131_delta_per_day) AS D_131_delta_pd
    FROM
        D_131_delta_per_day
    GROUP BY
        customer_ID
),      
D_131_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_131_delta) AS D_131_delta_mean,
        MAX(D_131_delta) AS D_131_delta_max,
        MIN(D_131_delta) AS D_131_delta_min
    FROM
        D_131_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_131) AS D_131_mean,
        MIN(D_131) AS D_131_min, 
        MAX(D_131) AS D_131_max, 
        SUM(D_131) AS D_131_sum,
        COUNT(D_131) AS D_131_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_131_mean,
        a.D_131_min, 
        a.D_131_max, 
        a.D_131_sum,
        a.D_131_max - a.D_131_min AS D_131_range,
        a.D_131_count,
        f.D_131_first,
        l.D_131_last,
        d.D_131_delta_mean,
        d.D_131_delta_max,
        d.D_131_delta_min,
        pd.D_131_delta_pd,
        cs.D_131_span
    FROM
        aggs a
        LEFT JOIN first_D_131 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_131 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_131_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_131_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_131_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_131_mean, 
    v.D_131_min,
    v.D_131_max, 
    v.D_131_range,
    v.D_131_sum,
    ISNULL(v.D_131_count, 0) AS D_131_count,
    v.D_131_first, 
    v.D_131_last,
    v.D_131_delta_mean,
    v.D_131_delta_max,
    v.D_131_delta_min,
    v.D_131_delta_pd,
    v.D_131_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_132_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_132 
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
    WHERE D_132 IS NOT NULL
    GROUP BY customer_ID
),
first_D_132 AS
(
    SELECT
        f.customer_ID, s.D_132 AS D_132_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_132 AS
(
    SELECT
        f.customer_ID, s.D_132 AS D_132_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_132_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_132_span
    FROM
        first_last
),
D_132_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_132,
        s.D_132 - LAG(s.D_132, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_132_delta
    FROM
        subset s
),
D_132_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_132_delta
    FROM
        D_132_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_132_delta_per_day AS
(
    SELECT
        customer_ID,
        D_132_delta / date_delta AS D_132_delta_per_day
    FROM
        D_132_delta
),
D_132_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_132_delta_per_day) AS D_132_delta_pd
    FROM
        D_132_delta_per_day
    GROUP BY
        customer_ID
),      
D_132_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_132_delta) AS D_132_delta_mean,
        MAX(D_132_delta) AS D_132_delta_max,
        MIN(D_132_delta) AS D_132_delta_min
    FROM
        D_132_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_132) AS D_132_mean,
        MIN(D_132) AS D_132_min, 
        MAX(D_132) AS D_132_max, 
        SUM(D_132) AS D_132_sum,
        COUNT(D_132) AS D_132_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_132_mean,
        a.D_132_min, 
        a.D_132_max, 
        a.D_132_sum,
        a.D_132_max - a.D_132_min AS D_132_range,
        a.D_132_count,
        f.D_132_first,
        l.D_132_last,
        d.D_132_delta_mean,
        d.D_132_delta_max,
        d.D_132_delta_min,
        pd.D_132_delta_pd,
        cs.D_132_span
    FROM
        aggs a
        LEFT JOIN first_D_132 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_132 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_132_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_132_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_132_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_132_mean, 
    v.D_132_min,
    v.D_132_max, 
    v.D_132_range,
    v.D_132_sum,
    ISNULL(v.D_132_count, 0) AS D_132_count,
    v.D_132_first, 
    v.D_132_last,
    v.D_132_delta_mean,
    v.D_132_delta_max,
    v.D_132_delta_min,
    v.D_132_delta_pd,
    v.D_132_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_133_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_133 
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
    WHERE D_133 IS NOT NULL
    GROUP BY customer_ID
),
first_D_133 AS
(
    SELECT
        f.customer_ID, s.D_133 AS D_133_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_133 AS
(
    SELECT
        f.customer_ID, s.D_133 AS D_133_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_133_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_133_span
    FROM
        first_last
),
D_133_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_133,
        s.D_133 - LAG(s.D_133, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_133_delta
    FROM
        subset s
),
D_133_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_133_delta
    FROM
        D_133_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_133_delta_per_day AS
(
    SELECT
        customer_ID,
        D_133_delta / date_delta AS D_133_delta_per_day
    FROM
        D_133_delta
),
D_133_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_133_delta_per_day) AS D_133_delta_pd
    FROM
        D_133_delta_per_day
    GROUP BY
        customer_ID
),      
D_133_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_133_delta) AS D_133_delta_mean,
        MAX(D_133_delta) AS D_133_delta_max,
        MIN(D_133_delta) AS D_133_delta_min
    FROM
        D_133_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_133) AS D_133_mean,
        MIN(D_133) AS D_133_min, 
        MAX(D_133) AS D_133_max, 
        SUM(D_133) AS D_133_sum,
        COUNT(D_133) AS D_133_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_133_mean,
        a.D_133_min, 
        a.D_133_max, 
        a.D_133_sum,
        a.D_133_max - a.D_133_min AS D_133_range,
        a.D_133_count,
        f.D_133_first,
        l.D_133_last,
        d.D_133_delta_mean,
        d.D_133_delta_max,
        d.D_133_delta_min,
        pd.D_133_delta_pd,
        cs.D_133_span
    FROM
        aggs a
        LEFT JOIN first_D_133 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_133 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_133_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_133_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_133_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_133_mean, 
    v.D_133_min,
    v.D_133_max, 
    v.D_133_range,
    v.D_133_sum,
    ISNULL(v.D_133_count, 0) AS D_133_count,
    v.D_133_first, 
    v.D_133_last,
    v.D_133_delta_mean,
    v.D_133_delta_max,
    v.D_133_delta_min,
    v.D_133_delta_pd,
    v.D_133_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_R_28_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.R_28 
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
    WHERE R_28 IS NOT NULL
    GROUP BY customer_ID
),
first_R_28 AS
(
    SELECT
        f.customer_ID, s.R_28 AS R_28_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_R_28 AS
(
    SELECT
        f.customer_ID, s.R_28 AS R_28_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
R_28_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS R_28_span
    FROM
        first_last
),
R_28_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.R_28,
        s.R_28 - LAG(s.R_28, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS R_28_delta
    FROM
        subset s
),
R_28_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.R_28_delta
    FROM
        R_28_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
R_28_delta_per_day AS
(
    SELECT
        customer_ID,
        R_28_delta / date_delta AS R_28_delta_per_day
    FROM
        R_28_delta
),
R_28_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(R_28_delta_per_day) AS R_28_delta_pd
    FROM
        R_28_delta_per_day
    GROUP BY
        customer_ID
),      
R_28_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(R_28_delta) AS R_28_delta_mean,
        MAX(R_28_delta) AS R_28_delta_max,
        MIN(R_28_delta) AS R_28_delta_min
    FROM
        R_28_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(R_28) AS R_28_mean,
        MIN(R_28) AS R_28_min, 
        MAX(R_28) AS R_28_max, 
        SUM(R_28) AS R_28_sum,
        COUNT(R_28) AS R_28_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.R_28_mean,
        a.R_28_min, 
        a.R_28_max, 
        a.R_28_sum,
        a.R_28_max - a.R_28_min AS R_28_range,
        a.R_28_count,
        f.R_28_first,
        l.R_28_last,
        d.R_28_delta_mean,
        d.R_28_delta_max,
        d.R_28_delta_min,
        pd.R_28_delta_pd,
        cs.R_28_span
    FROM
        aggs a
        LEFT JOIN first_R_28 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_R_28 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN R_28_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN R_28_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN R_28_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.R_28_mean, 
    v.R_28_min,
    v.R_28_max, 
    v.R_28_range,
    v.R_28_sum,
    ISNULL(v.R_28_count, 0) AS R_28_count,
    v.R_28_first, 
    v.R_28_last,
    v.R_28_delta_mean,
    v.R_28_delta_max,
    v.R_28_delta_min,
    v.R_28_delta_pd,
    v.R_28_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_134_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_134 
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
    WHERE D_134 IS NOT NULL
    GROUP BY customer_ID
),
first_D_134 AS
(
    SELECT
        f.customer_ID, s.D_134 AS D_134_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_134 AS
(
    SELECT
        f.customer_ID, s.D_134 AS D_134_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_134_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_134_span
    FROM
        first_last
),
D_134_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_134,
        s.D_134 - LAG(s.D_134, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_134_delta
    FROM
        subset s
),
D_134_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_134_delta
    FROM
        D_134_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_134_delta_per_day AS
(
    SELECT
        customer_ID,
        D_134_delta / date_delta AS D_134_delta_per_day
    FROM
        D_134_delta
),
D_134_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_134_delta_per_day) AS D_134_delta_pd
    FROM
        D_134_delta_per_day
    GROUP BY
        customer_ID
),      
D_134_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_134_delta) AS D_134_delta_mean,
        MAX(D_134_delta) AS D_134_delta_max,
        MIN(D_134_delta) AS D_134_delta_min
    FROM
        D_134_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_134) AS D_134_mean,
        MIN(D_134) AS D_134_min, 
        MAX(D_134) AS D_134_max, 
        SUM(D_134) AS D_134_sum,
        COUNT(D_134) AS D_134_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_134_mean,
        a.D_134_min, 
        a.D_134_max, 
        a.D_134_sum,
        a.D_134_max - a.D_134_min AS D_134_range,
        a.D_134_count,
        f.D_134_first,
        l.D_134_last,
        d.D_134_delta_mean,
        d.D_134_delta_max,
        d.D_134_delta_min,
        pd.D_134_delta_pd,
        cs.D_134_span
    FROM
        aggs a
        LEFT JOIN first_D_134 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_134 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_134_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_134_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_134_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_134_mean, 
    v.D_134_min,
    v.D_134_max, 
    v.D_134_range,
    v.D_134_sum,
    ISNULL(v.D_134_count, 0) AS D_134_count,
    v.D_134_first, 
    v.D_134_last,
    v.D_134_delta_mean,
    v.D_134_delta_max,
    v.D_134_delta_min,
    v.D_134_delta_pd,
    v.D_134_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_135_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_135 
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
    WHERE D_135 IS NOT NULL
    GROUP BY customer_ID
),
first_D_135 AS
(
    SELECT
        f.customer_ID, s.D_135 AS D_135_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_135 AS
(
    SELECT
        f.customer_ID, s.D_135 AS D_135_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_135_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_135_span
    FROM
        first_last
),
D_135_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_135,
        s.D_135 - LAG(s.D_135, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_135_delta
    FROM
        subset s
),
D_135_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_135_delta
    FROM
        D_135_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_135_delta_per_day AS
(
    SELECT
        customer_ID,
        D_135_delta / date_delta AS D_135_delta_per_day
    FROM
        D_135_delta
),
D_135_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_135_delta_per_day) AS D_135_delta_pd
    FROM
        D_135_delta_per_day
    GROUP BY
        customer_ID
),      
D_135_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_135_delta) AS D_135_delta_mean,
        MAX(D_135_delta) AS D_135_delta_max,
        MIN(D_135_delta) AS D_135_delta_min
    FROM
        D_135_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_135) AS D_135_mean,
        MIN(D_135) AS D_135_min, 
        MAX(D_135) AS D_135_max, 
        SUM(D_135) AS D_135_sum,
        COUNT(D_135) AS D_135_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_135_mean,
        a.D_135_min, 
        a.D_135_max, 
        a.D_135_sum,
        a.D_135_max - a.D_135_min AS D_135_range,
        a.D_135_count,
        f.D_135_first,
        l.D_135_last,
        d.D_135_delta_mean,
        d.D_135_delta_max,
        d.D_135_delta_min,
        pd.D_135_delta_pd,
        cs.D_135_span
    FROM
        aggs a
        LEFT JOIN first_D_135 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_135 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_135_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_135_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_135_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_135_mean, 
    v.D_135_min,
    v.D_135_max, 
    v.D_135_range,
    v.D_135_sum,
    ISNULL(v.D_135_count, 0) AS D_135_count,
    v.D_135_first, 
    v.D_135_last,
    v.D_135_delta_mean,
    v.D_135_delta_max,
    v.D_135_delta_min,
    v.D_135_delta_pd,
    v.D_135_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_136_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_136 
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
    WHERE D_136 IS NOT NULL
    GROUP BY customer_ID
),
first_D_136 AS
(
    SELECT
        f.customer_ID, s.D_136 AS D_136_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_136 AS
(
    SELECT
        f.customer_ID, s.D_136 AS D_136_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_136_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_136_span
    FROM
        first_last
),
D_136_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_136,
        s.D_136 - LAG(s.D_136, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_136_delta
    FROM
        subset s
),
D_136_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_136_delta
    FROM
        D_136_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_136_delta_per_day AS
(
    SELECT
        customer_ID,
        D_136_delta / date_delta AS D_136_delta_per_day
    FROM
        D_136_delta
),
D_136_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_136_delta_per_day) AS D_136_delta_pd
    FROM
        D_136_delta_per_day
    GROUP BY
        customer_ID
),      
D_136_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_136_delta) AS D_136_delta_mean,
        MAX(D_136_delta) AS D_136_delta_max,
        MIN(D_136_delta) AS D_136_delta_min
    FROM
        D_136_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_136) AS D_136_mean,
        MIN(D_136) AS D_136_min, 
        MAX(D_136) AS D_136_max, 
        SUM(D_136) AS D_136_sum,
        COUNT(D_136) AS D_136_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_136_mean,
        a.D_136_min, 
        a.D_136_max, 
        a.D_136_sum,
        a.D_136_max - a.D_136_min AS D_136_range,
        a.D_136_count,
        f.D_136_first,
        l.D_136_last,
        d.D_136_delta_mean,
        d.D_136_delta_max,
        d.D_136_delta_min,
        pd.D_136_delta_pd,
        cs.D_136_span
    FROM
        aggs a
        LEFT JOIN first_D_136 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_136 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_136_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_136_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_136_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_136_mean, 
    v.D_136_min,
    v.D_136_max, 
    v.D_136_range,
    v.D_136_sum,
    ISNULL(v.D_136_count, 0) AS D_136_count,
    v.D_136_first, 
    v.D_136_last,
    v.D_136_delta_mean,
    v.D_136_delta_max,
    v.D_136_delta_min,
    v.D_136_delta_pd,
    v.D_136_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_137_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_137 
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
    WHERE D_137 IS NOT NULL
    GROUP BY customer_ID
),
first_D_137 AS
(
    SELECT
        f.customer_ID, s.D_137 AS D_137_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_137 AS
(
    SELECT
        f.customer_ID, s.D_137 AS D_137_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_137_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_137_span
    FROM
        first_last
),
D_137_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_137,
        s.D_137 - LAG(s.D_137, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_137_delta
    FROM
        subset s
),
D_137_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_137_delta
    FROM
        D_137_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_137_delta_per_day AS
(
    SELECT
        customer_ID,
        D_137_delta / date_delta AS D_137_delta_per_day
    FROM
        D_137_delta
),
D_137_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_137_delta_per_day) AS D_137_delta_pd
    FROM
        D_137_delta_per_day
    GROUP BY
        customer_ID
),      
D_137_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_137_delta) AS D_137_delta_mean,
        MAX(D_137_delta) AS D_137_delta_max,
        MIN(D_137_delta) AS D_137_delta_min
    FROM
        D_137_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_137) AS D_137_mean,
        MIN(D_137) AS D_137_min, 
        MAX(D_137) AS D_137_max, 
        SUM(D_137) AS D_137_sum,
        COUNT(D_137) AS D_137_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_137_mean,
        a.D_137_min, 
        a.D_137_max, 
        a.D_137_sum,
        a.D_137_max - a.D_137_min AS D_137_range,
        a.D_137_count,
        f.D_137_first,
        l.D_137_last,
        d.D_137_delta_mean,
        d.D_137_delta_max,
        d.D_137_delta_min,
        pd.D_137_delta_pd,
        cs.D_137_span
    FROM
        aggs a
        LEFT JOIN first_D_137 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_137 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_137_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_137_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_137_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_137_mean, 
    v.D_137_min,
    v.D_137_max, 
    v.D_137_range,
    v.D_137_sum,
    ISNULL(v.D_137_count, 0) AS D_137_count,
    v.D_137_first, 
    v.D_137_last,
    v.D_137_delta_mean,
    v.D_137_delta_max,
    v.D_137_delta_min,
    v.D_137_delta_pd,
    v.D_137_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_138_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_138 
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
    WHERE D_138 IS NOT NULL
    GROUP BY customer_ID
),
first_D_138 AS
(
    SELECT
        f.customer_ID, s.D_138 AS D_138_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_138 AS
(
    SELECT
        f.customer_ID, s.D_138 AS D_138_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_138_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_138_span
    FROM
        first_last
),
D_138_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_138,
        s.D_138 - LAG(s.D_138, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_138_delta
    FROM
        subset s
),
D_138_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_138_delta
    FROM
        D_138_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_138_delta_per_day AS
(
    SELECT
        customer_ID,
        D_138_delta / date_delta AS D_138_delta_per_day
    FROM
        D_138_delta
),
D_138_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_138_delta_per_day) AS D_138_delta_pd
    FROM
        D_138_delta_per_day
    GROUP BY
        customer_ID
),      
D_138_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_138_delta) AS D_138_delta_mean,
        MAX(D_138_delta) AS D_138_delta_max,
        MIN(D_138_delta) AS D_138_delta_min
    FROM
        D_138_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_138) AS D_138_mean,
        MIN(D_138) AS D_138_min, 
        MAX(D_138) AS D_138_max, 
        SUM(D_138) AS D_138_sum,
        COUNT(D_138) AS D_138_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_138_mean,
        a.D_138_min, 
        a.D_138_max, 
        a.D_138_sum,
        a.D_138_max - a.D_138_min AS D_138_range,
        a.D_138_count,
        f.D_138_first,
        l.D_138_last,
        d.D_138_delta_mean,
        d.D_138_delta_max,
        d.D_138_delta_min,
        pd.D_138_delta_pd,
        cs.D_138_span
    FROM
        aggs a
        LEFT JOIN first_D_138 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_138 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_138_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_138_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_138_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_138_mean, 
    v.D_138_min,
    v.D_138_max, 
    v.D_138_range,
    v.D_138_sum,
    ISNULL(v.D_138_count, 0) AS D_138_count,
    v.D_138_first, 
    v.D_138_last,
    v.D_138_delta_mean,
    v.D_138_delta_max,
    v.D_138_delta_min,
    v.D_138_delta_pd,
    v.D_138_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_139_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_139 
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
    WHERE D_139 IS NOT NULL
    GROUP BY customer_ID
),
first_D_139 AS
(
    SELECT
        f.customer_ID, s.D_139 AS D_139_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_139 AS
(
    SELECT
        f.customer_ID, s.D_139 AS D_139_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_139_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_139_span
    FROM
        first_last
),
D_139_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_139,
        s.D_139 - LAG(s.D_139, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_139_delta
    FROM
        subset s
),
D_139_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_139_delta
    FROM
        D_139_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_139_delta_per_day AS
(
    SELECT
        customer_ID,
        D_139_delta / date_delta AS D_139_delta_per_day
    FROM
        D_139_delta
),
D_139_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_139_delta_per_day) AS D_139_delta_pd
    FROM
        D_139_delta_per_day
    GROUP BY
        customer_ID
),      
D_139_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_139_delta) AS D_139_delta_mean,
        MAX(D_139_delta) AS D_139_delta_max,
        MIN(D_139_delta) AS D_139_delta_min
    FROM
        D_139_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_139) AS D_139_mean,
        MIN(D_139) AS D_139_min, 
        MAX(D_139) AS D_139_max, 
        SUM(D_139) AS D_139_sum,
        COUNT(D_139) AS D_139_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_139_mean,
        a.D_139_min, 
        a.D_139_max, 
        a.D_139_sum,
        a.D_139_max - a.D_139_min AS D_139_range,
        a.D_139_count,
        f.D_139_first,
        l.D_139_last,
        d.D_139_delta_mean,
        d.D_139_delta_max,
        d.D_139_delta_min,
        pd.D_139_delta_pd,
        cs.D_139_span
    FROM
        aggs a
        LEFT JOIN first_D_139 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_139 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_139_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_139_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_139_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_139_mean, 
    v.D_139_min,
    v.D_139_max, 
    v.D_139_range,
    v.D_139_sum,
    ISNULL(v.D_139_count, 0) AS D_139_count,
    v.D_139_first, 
    v.D_139_last,
    v.D_139_delta_mean,
    v.D_139_delta_max,
    v.D_139_delta_min,
    v.D_139_delta_pd,
    v.D_139_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_140_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_140 
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
    WHERE D_140 IS NOT NULL
    GROUP BY customer_ID
),
first_D_140 AS
(
    SELECT
        f.customer_ID, s.D_140 AS D_140_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_140 AS
(
    SELECT
        f.customer_ID, s.D_140 AS D_140_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_140_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_140_span
    FROM
        first_last
),
D_140_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_140,
        s.D_140 - LAG(s.D_140, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_140_delta
    FROM
        subset s
),
D_140_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_140_delta
    FROM
        D_140_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_140_delta_per_day AS
(
    SELECT
        customer_ID,
        D_140_delta / date_delta AS D_140_delta_per_day
    FROM
        D_140_delta
),
D_140_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_140_delta_per_day) AS D_140_delta_pd
    FROM
        D_140_delta_per_day
    GROUP BY
        customer_ID
),      
D_140_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_140_delta) AS D_140_delta_mean,
        MAX(D_140_delta) AS D_140_delta_max,
        MIN(D_140_delta) AS D_140_delta_min
    FROM
        D_140_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_140) AS D_140_mean,
        MIN(D_140) AS D_140_min, 
        MAX(D_140) AS D_140_max, 
        SUM(D_140) AS D_140_sum,
        COUNT(D_140) AS D_140_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_140_mean,
        a.D_140_min, 
        a.D_140_max, 
        a.D_140_sum,
        a.D_140_max - a.D_140_min AS D_140_range,
        a.D_140_count,
        f.D_140_first,
        l.D_140_last,
        d.D_140_delta_mean,
        d.D_140_delta_max,
        d.D_140_delta_min,
        pd.D_140_delta_pd,
        cs.D_140_span
    FROM
        aggs a
        LEFT JOIN first_D_140 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_140 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_140_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_140_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_140_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_140_mean, 
    v.D_140_min,
    v.D_140_max, 
    v.D_140_range,
    v.D_140_sum,
    ISNULL(v.D_140_count, 0) AS D_140_count,
    v.D_140_first, 
    v.D_140_last,
    v.D_140_delta_mean,
    v.D_140_delta_max,
    v.D_140_delta_min,
    v.D_140_delta_pd,
    v.D_140_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_141_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_141 
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
    WHERE D_141 IS NOT NULL
    GROUP BY customer_ID
),
first_D_141 AS
(
    SELECT
        f.customer_ID, s.D_141 AS D_141_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_141 AS
(
    SELECT
        f.customer_ID, s.D_141 AS D_141_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_141_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_141_span
    FROM
        first_last
),
D_141_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_141,
        s.D_141 - LAG(s.D_141, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_141_delta
    FROM
        subset s
),
D_141_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_141_delta
    FROM
        D_141_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_141_delta_per_day AS
(
    SELECT
        customer_ID,
        D_141_delta / date_delta AS D_141_delta_per_day
    FROM
        D_141_delta
),
D_141_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_141_delta_per_day) AS D_141_delta_pd
    FROM
        D_141_delta_per_day
    GROUP BY
        customer_ID
),      
D_141_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_141_delta) AS D_141_delta_mean,
        MAX(D_141_delta) AS D_141_delta_max,
        MIN(D_141_delta) AS D_141_delta_min
    FROM
        D_141_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_141) AS D_141_mean,
        MIN(D_141) AS D_141_min, 
        MAX(D_141) AS D_141_max, 
        SUM(D_141) AS D_141_sum,
        COUNT(D_141) AS D_141_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_141_mean,
        a.D_141_min, 
        a.D_141_max, 
        a.D_141_sum,
        a.D_141_max - a.D_141_min AS D_141_range,
        a.D_141_count,
        f.D_141_first,
        l.D_141_last,
        d.D_141_delta_mean,
        d.D_141_delta_max,
        d.D_141_delta_min,
        pd.D_141_delta_pd,
        cs.D_141_span
    FROM
        aggs a
        LEFT JOIN first_D_141 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_141 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_141_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_141_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_141_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_141_mean, 
    v.D_141_min,
    v.D_141_max, 
    v.D_141_range,
    v.D_141_sum,
    ISNULL(v.D_141_count, 0) AS D_141_count,
    v.D_141_first, 
    v.D_141_last,
    v.D_141_delta_mean,
    v.D_141_delta_max,
    v.D_141_delta_min,
    v.D_141_delta_pd,
    v.D_141_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_142_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_142 
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
    WHERE D_142 IS NOT NULL
    GROUP BY customer_ID
),
first_D_142 AS
(
    SELECT
        f.customer_ID, s.D_142 AS D_142_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_142 AS
(
    SELECT
        f.customer_ID, s.D_142 AS D_142_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_142_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_142_span
    FROM
        first_last
),
D_142_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_142,
        s.D_142 - LAG(s.D_142, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_142_delta
    FROM
        subset s
),
D_142_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_142_delta
    FROM
        D_142_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_142_delta_per_day AS
(
    SELECT
        customer_ID,
        D_142_delta / date_delta AS D_142_delta_per_day
    FROM
        D_142_delta
),
D_142_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_142_delta_per_day) AS D_142_delta_pd
    FROM
        D_142_delta_per_day
    GROUP BY
        customer_ID
),      
D_142_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_142_delta) AS D_142_delta_mean,
        MAX(D_142_delta) AS D_142_delta_max,
        MIN(D_142_delta) AS D_142_delta_min
    FROM
        D_142_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_142) AS D_142_mean,
        MIN(D_142) AS D_142_min, 
        MAX(D_142) AS D_142_max, 
        SUM(D_142) AS D_142_sum,
        COUNT(D_142) AS D_142_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_142_mean,
        a.D_142_min, 
        a.D_142_max, 
        a.D_142_sum,
        a.D_142_max - a.D_142_min AS D_142_range,
        a.D_142_count,
        f.D_142_first,
        l.D_142_last,
        d.D_142_delta_mean,
        d.D_142_delta_max,
        d.D_142_delta_min,
        pd.D_142_delta_pd,
        cs.D_142_span
    FROM
        aggs a
        LEFT JOIN first_D_142 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_142 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_142_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_142_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_142_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_142_mean, 
    v.D_142_min,
    v.D_142_max, 
    v.D_142_range,
    v.D_142_sum,
    ISNULL(v.D_142_count, 0) AS D_142_count,
    v.D_142_first, 
    v.D_142_last,
    v.D_142_delta_mean,
    v.D_142_delta_max,
    v.D_142_delta_min,
    v.D_142_delta_pd,
    v.D_142_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_143_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_143 
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
    WHERE D_143 IS NOT NULL
    GROUP BY customer_ID
),
first_D_143 AS
(
    SELECT
        f.customer_ID, s.D_143 AS D_143_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_143 AS
(
    SELECT
        f.customer_ID, s.D_143 AS D_143_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_143_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_143_span
    FROM
        first_last
),
D_143_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_143,
        s.D_143 - LAG(s.D_143, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_143_delta
    FROM
        subset s
),
D_143_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_143_delta
    FROM
        D_143_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_143_delta_per_day AS
(
    SELECT
        customer_ID,
        D_143_delta / date_delta AS D_143_delta_per_day
    FROM
        D_143_delta
),
D_143_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_143_delta_per_day) AS D_143_delta_pd
    FROM
        D_143_delta_per_day
    GROUP BY
        customer_ID
),      
D_143_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_143_delta) AS D_143_delta_mean,
        MAX(D_143_delta) AS D_143_delta_max,
        MIN(D_143_delta) AS D_143_delta_min
    FROM
        D_143_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_143) AS D_143_mean,
        MIN(D_143) AS D_143_min, 
        MAX(D_143) AS D_143_max, 
        SUM(D_143) AS D_143_sum,
        COUNT(D_143) AS D_143_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_143_mean,
        a.D_143_min, 
        a.D_143_max, 
        a.D_143_sum,
        a.D_143_max - a.D_143_min AS D_143_range,
        a.D_143_count,
        f.D_143_first,
        l.D_143_last,
        d.D_143_delta_mean,
        d.D_143_delta_max,
        d.D_143_delta_min,
        pd.D_143_delta_pd,
        cs.D_143_span
    FROM
        aggs a
        LEFT JOIN first_D_143 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_143 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_143_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_143_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_143_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_143_mean, 
    v.D_143_min,
    v.D_143_max, 
    v.D_143_range,
    v.D_143_sum,
    ISNULL(v.D_143_count, 0) AS D_143_count,
    v.D_143_first, 
    v.D_143_last,
    v.D_143_delta_mean,
    v.D_143_delta_max,
    v.D_143_delta_min,
    v.D_143_delta_pd,
    v.D_143_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_144_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_144 
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
    WHERE D_144 IS NOT NULL
    GROUP BY customer_ID
),
first_D_144 AS
(
    SELECT
        f.customer_ID, s.D_144 AS D_144_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_144 AS
(
    SELECT
        f.customer_ID, s.D_144 AS D_144_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_144_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_144_span
    FROM
        first_last
),
D_144_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_144,
        s.D_144 - LAG(s.D_144, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_144_delta
    FROM
        subset s
),
D_144_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_144_delta
    FROM
        D_144_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_144_delta_per_day AS
(
    SELECT
        customer_ID,
        D_144_delta / date_delta AS D_144_delta_per_day
    FROM
        D_144_delta
),
D_144_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_144_delta_per_day) AS D_144_delta_pd
    FROM
        D_144_delta_per_day
    GROUP BY
        customer_ID
),      
D_144_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_144_delta) AS D_144_delta_mean,
        MAX(D_144_delta) AS D_144_delta_max,
        MIN(D_144_delta) AS D_144_delta_min
    FROM
        D_144_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_144) AS D_144_mean,
        MIN(D_144) AS D_144_min, 
        MAX(D_144) AS D_144_max, 
        SUM(D_144) AS D_144_sum,
        COUNT(D_144) AS D_144_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_144_mean,
        a.D_144_min, 
        a.D_144_max, 
        a.D_144_sum,
        a.D_144_max - a.D_144_min AS D_144_range,
        a.D_144_count,
        f.D_144_first,
        l.D_144_last,
        d.D_144_delta_mean,
        d.D_144_delta_max,
        d.D_144_delta_min,
        pd.D_144_delta_pd,
        cs.D_144_span
    FROM
        aggs a
        LEFT JOIN first_D_144 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_144 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_144_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_144_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_144_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_144_mean, 
    v.D_144_min,
    v.D_144_max, 
    v.D_144_range,
    v.D_144_sum,
    ISNULL(v.D_144_count, 0) AS D_144_count,
    v.D_144_first, 
    v.D_144_last,
    v.D_144_delta_mean,
    v.D_144_delta_max,
    v.D_144_delta_min,
    v.D_144_delta_pd,
    v.D_144_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;
GO

CREATE VIEW test_data_D_145_agg AS
WITH 
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
subset AS
(
    SELECT
        td.customer_ID, td.S_2, td.D_145 
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
    WHERE D_145 IS NOT NULL
    GROUP BY customer_ID
),
first_D_145 AS
(
    SELECT
        f.customer_ID, s.D_145 AS D_145_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.first_dt = s.S_2
),
last_D_145 AS
(
    SELECT
        f.customer_ID, s.D_145 AS D_145_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.customer_ID = s.customer_ID 
            AND f.last_dt = s.S_2
),
D_145_span AS
(
    SELECT
        customer_ID,
        DATEDIFF(DAY, first_dt, last_dt) AS D_145_span
    FROM
        first_last
),
D_145_minus_lag AS
(
    SELECT
        s.customer_ID, 
        s.S_2,
        CAST(DATEDIFF(
            DAY, 
            LAG(s.S_2, 1) OVER(PARTITION BY s.customer_ID ORDER BY s.S_2),
            s.S_2
        ) AS FLOAT) AS date_delta,
        s.D_145,
        s.D_145 - LAG(s.D_145, 1) OVER(
                PARTITION BY s.customer_ID 
                ORDER BY s.S_2
            ) AS D_145_delta
    FROM
        subset s
),
D_145_delta AS
(
    SELECT
        m.customer_ID,
        m.S_2,
        m.date_delta,
        m.D_145_delta
    FROM
        D_145_minus_lag m
        JOIN first_last f
            ON m.customer_ID = f.customer_ID
    WHERE
        m.S_2 > f.first_dt
),
D_145_delta_per_day AS
(
    SELECT
        customer_ID,
        D_145_delta / date_delta AS D_145_delta_per_day
    FROM
        D_145_delta
),
D_145_delta_pd_mean AS
(
    SELECT
        customer_ID,
        AVG(D_145_delta_per_day) AS D_145_delta_pd
    FROM
        D_145_delta_per_day
    GROUP BY
        customer_ID
),      
D_145_delta_aggs AS
(
    SELECT
        customer_ID,
        AVG(D_145_delta) AS D_145_delta_mean,
        MAX(D_145_delta) AS D_145_delta_max,
        MIN(D_145_delta) AS D_145_delta_min
    FROM
        D_145_delta
    GROUP BY
        customer_ID
),
aggs AS
(
    SELECT
        customer_ID, 
        AVG(D_145) AS D_145_mean,
        MIN(D_145) AS D_145_min, 
        MAX(D_145) AS D_145_max, 
        SUM(D_145) AS D_145_sum,
        COUNT(D_145) AS D_145_count
        
    FROM
        subset
    GROUP BY
        customer_ID
),
vals AS
(
    SELECT
        a.customer_ID,
        a.D_145_mean,
        a.D_145_min, 
        a.D_145_max, 
        a.D_145_sum,
        a.D_145_max - a.D_145_min AS D_145_range,
        a.D_145_count,
        f.D_145_first,
        l.D_145_last,
        d.D_145_delta_mean,
        d.D_145_delta_max,
        d.D_145_delta_min,
        pd.D_145_delta_pd,
        cs.D_145_span
    FROM
        aggs a
        LEFT JOIN first_D_145 f
            ON a.customer_ID = f.customer_ID
        LEFT JOIN last_D_145 l
            ON a.customer_ID = l.customer_ID
        LEFT JOIN D_145_delta_aggs d
            ON  a.customer_ID = d.customer_ID
        LEFT JOIN D_145_delta_pd_mean pd
            ON a.customer_ID = pd.customer_ID
        LEFT JOIN D_145_span cs
            ON a.customer_ID = cs.customer_ID
)

SELECT
    u.customer_ID,
    v.D_145_mean, 
    v.D_145_min,
    v.D_145_max, 
    v.D_145_range,
    v.D_145_sum,
    ISNULL(v.D_145_count, 0) AS D_145_count,
    v.D_145_first, 
    v.D_145_last,
    v.D_145_delta_mean,
    v.D_145_delta_max,
    v.D_145_delta_min,
    v.D_145_delta_pd,
    v.D_145_span
    
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.customer_ID = v.customer_ID
;