
CREATE VIEW test_data_D_68_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_count
    FROM test_data
    GROUP BY customer_ID
),
D_68_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_0
    FROM test_data
    WHERE D_68 = 0.0
    GROUP BY customer_ID
), 
D_68_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_1
    FROM test_data
    WHERE D_68 = 1.0
    GROUP BY customer_ID
), 
D_68_2_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_2
    FROM test_data
    WHERE D_68 = 2.0
    GROUP BY customer_ID
), 
D_68_3_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_3
    FROM test_data
    WHERE D_68 = 3.0
    GROUP BY customer_ID
), 
D_68_4_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_4
    FROM test_data
    WHERE D_68 = 4.0
    GROUP BY customer_ID
), 
D_68_5_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_5
    FROM test_data
    WHERE D_68 = 5.0
    GROUP BY customer_ID
), 
D_68_6_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_6
    FROM test_data
    WHERE D_68 = 6.0
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.D_68_count,
        CASE
            WHEN D_68_0 IS NULL THEN 0
            ELSE D_68_0_.D_68_0 / tc.D_68_count
        END AS D_68_0,
        CASE
            WHEN D_68_1 IS NULL THEN 0
            ELSE D_68_1_.D_68_1 / tc.D_68_count
        END AS D_68_1,
        CASE
            WHEN D_68_2 IS NULL THEN 0
            ELSE D_68_2_.D_68_2 / tc.D_68_count
        END AS D_68_2,
        CASE
            WHEN D_68_3 IS NULL THEN 0
            ELSE D_68_3_.D_68_3 / tc.D_68_count
        END AS D_68_3,
        CASE
            WHEN D_68_4 IS NULL THEN 0
            ELSE D_68_4_.D_68_4 / tc.D_68_count
        END AS D_68_4,
        CASE
            WHEN D_68_5 IS NULL THEN 0
            ELSE D_68_5_.D_68_5 / tc.D_68_count
        END AS D_68_5,
        CASE
            WHEN D_68_6 IS NULL THEN 0
            ELSE D_68_6_.D_68_6 / tc.D_68_count
        END AS D_68_6
    FROM
        total_counts tc
        LEFT JOIN D_68_0_ 
            ON tc.customer_ID = D_68_0_.customer_ID
        LEFT JOIN D_68_1_ 
            ON tc.customer_ID = D_68_1_.customer_ID
        LEFT JOIN D_68_2_ 
            ON tc.customer_ID = D_68_2_.customer_ID
        LEFT JOIN D_68_3_ 
            ON tc.customer_ID = D_68_3_.customer_ID
        LEFT JOIN D_68_4_ 
            ON tc.customer_ID = D_68_4_.customer_ID
        LEFT JOIN D_68_5_ 
            ON tc.customer_ID = D_68_5_.customer_ID
        LEFT JOIN D_68_6_ 
            ON tc.customer_ID = D_68_6_.customer_ID
)


SELECT
    u.customer_ID,
    c.D_68_count,
    ISNULL(c.D_68_0, 0) AS D_68_0, 
    ISNULL(c.D_68_1, 0) AS D_68_1, 
    ISNULL(c.D_68_2, 0) AS D_68_2, 
    ISNULL(c.D_68_3, 0) AS D_68_3, 
    ISNULL(c.D_68_4, 0) AS D_68_4, 
    ISNULL(c.D_68_5, 0) AS D_68_5, 
    ISNULL(c.D_68_6, 0) AS D_68_6
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;
