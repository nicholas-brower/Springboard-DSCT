
CREATE VIEW test_data_B_30_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(B_30) AS FLOAT) AS B_30_count
    FROM test_data
    GROUP BY customer_ID
),
B_30_0_ AS
(
    SELECT customer_ID, CAST(COUNT(B_30) AS FLOAT) AS B_30_0
    FROM test_data
    WHERE B_30 = 0.0
    GROUP BY customer_ID
), 
B_30_1_ AS
(
    SELECT customer_ID, CAST(COUNT(B_30) AS FLOAT) AS B_30_1
    FROM test_data
    WHERE B_30 = 1.0
    GROUP BY customer_ID
), 
B_30_2_ AS
(
    SELECT customer_ID, CAST(COUNT(B_30) AS FLOAT) AS B_30_2
    FROM test_data
    WHERE B_30 = 2.0
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.B_30_count,
        CASE
            WHEN B_30_0 IS NULL THEN 0
            ELSE B_30_0_.B_30_0 / tc.B_30_count
        END AS B_30_0,
        CASE
            WHEN B_30_1 IS NULL THEN 0
            ELSE B_30_1_.B_30_1 / tc.B_30_count
        END AS B_30_1,
        CASE
            WHEN B_30_2 IS NULL THEN 0
            ELSE B_30_2_.B_30_2 / tc.B_30_count
        END AS B_30_2
    FROM
        total_counts tc
        LEFT JOIN B_30_0_ 
            ON tc.customer_ID = B_30_0_.customer_ID
        LEFT JOIN B_30_1_ 
            ON tc.customer_ID = B_30_1_.customer_ID
        LEFT JOIN B_30_2_ 
            ON tc.customer_ID = B_30_2_.customer_ID
)


SELECT
    u.customer_ID,
    c.B_30_count,
    ISNULL(c.B_30_0, 0) AS B_30_0, 
    ISNULL(c.B_30_1, 0) AS B_30_1, 
    ISNULL(c.B_30_2, 0) AS B_30_2
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;
