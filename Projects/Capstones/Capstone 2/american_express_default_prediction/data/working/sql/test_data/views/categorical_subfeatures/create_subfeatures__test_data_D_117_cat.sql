
CREATE VIEW test_data_D_117_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_count
    FROM test_data
    GROUP BY customer_ID
),
D_117_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_0
    FROM test_data
    WHERE D_117 = -1.0
    GROUP BY customer_ID
), 
D_117_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_1
    FROM test_data
    WHERE D_117 = 1.0
    GROUP BY customer_ID
), 
D_117_2_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_2
    FROM test_data
    WHERE D_117 = 2.0
    GROUP BY customer_ID
), 
D_117_3_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_3
    FROM test_data
    WHERE D_117 = 3.0
    GROUP BY customer_ID
), 
D_117_4_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_4
    FROM test_data
    WHERE D_117 = 4.0
    GROUP BY customer_ID
), 
D_117_5_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_5
    FROM test_data
    WHERE D_117 = 5.0
    GROUP BY customer_ID
), 
D_117_6_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_6
    FROM test_data
    WHERE D_117 = 6.0
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.D_117_count,
        CASE
            WHEN D_117_0 IS NULL THEN 0
            ELSE D_117_0_.D_117_0 / tc.D_117_count
        END AS D_117_0,
        CASE
            WHEN D_117_1 IS NULL THEN 0
            ELSE D_117_1_.D_117_1 / tc.D_117_count
        END AS D_117_1,
        CASE
            WHEN D_117_2 IS NULL THEN 0
            ELSE D_117_2_.D_117_2 / tc.D_117_count
        END AS D_117_2,
        CASE
            WHEN D_117_3 IS NULL THEN 0
            ELSE D_117_3_.D_117_3 / tc.D_117_count
        END AS D_117_3,
        CASE
            WHEN D_117_4 IS NULL THEN 0
            ELSE D_117_4_.D_117_4 / tc.D_117_count
        END AS D_117_4,
        CASE
            WHEN D_117_5 IS NULL THEN 0
            ELSE D_117_5_.D_117_5 / tc.D_117_count
        END AS D_117_5,
        CASE
            WHEN D_117_6 IS NULL THEN 0
            ELSE D_117_6_.D_117_6 / tc.D_117_count
        END AS D_117_6
    FROM
        total_counts tc
        LEFT JOIN D_117_0_ 
            ON tc.customer_ID = D_117_0_.customer_ID
        LEFT JOIN D_117_1_ 
            ON tc.customer_ID = D_117_1_.customer_ID
        LEFT JOIN D_117_2_ 
            ON tc.customer_ID = D_117_2_.customer_ID
        LEFT JOIN D_117_3_ 
            ON tc.customer_ID = D_117_3_.customer_ID
        LEFT JOIN D_117_4_ 
            ON tc.customer_ID = D_117_4_.customer_ID
        LEFT JOIN D_117_5_ 
            ON tc.customer_ID = D_117_5_.customer_ID
        LEFT JOIN D_117_6_ 
            ON tc.customer_ID = D_117_6_.customer_ID
)


SELECT
    u.customer_ID,
    c.D_117_count,
    ISNULL(c.D_117_0, 0) AS D_117_0, 
    ISNULL(c.D_117_1, 0) AS D_117_1, 
    ISNULL(c.D_117_2, 0) AS D_117_2, 
    ISNULL(c.D_117_3, 0) AS D_117_3, 
    ISNULL(c.D_117_4, 0) AS D_117_4, 
    ISNULL(c.D_117_5, 0) AS D_117_5, 
    ISNULL(c.D_117_6, 0) AS D_117_6
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;
