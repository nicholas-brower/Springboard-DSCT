
CREATE VIEW test_data_D_63_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM test_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_count
    FROM test_data
    GROUP BY customer_ID
),
D_63_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_0
    FROM test_data
    WHERE D_63 = 'CL'
    GROUP BY customer_ID
), 
D_63_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_1
    FROM test_data
    WHERE D_63 = 'CO'
    GROUP BY customer_ID
), 
D_63_2_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_2
    FROM test_data
    WHERE D_63 = 'CR'
    GROUP BY customer_ID
), 
D_63_3_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_3
    FROM test_data
    WHERE D_63 = 'XL'
    GROUP BY customer_ID
), 
D_63_4_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_4
    FROM test_data
    WHERE D_63 = 'XM'
    GROUP BY customer_ID
), 
D_63_5_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_5
    FROM test_data
    WHERE D_63 = 'XZ'
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.D_63_count,
        CASE
            WHEN D_63_0 IS NULL THEN 0
            ELSE D_63_0_.D_63_0 / tc.D_63_count
        END AS D_63_0,
        CASE
            WHEN D_63_1 IS NULL THEN 0
            ELSE D_63_1_.D_63_1 / tc.D_63_count
        END AS D_63_1,
        CASE
            WHEN D_63_2 IS NULL THEN 0
            ELSE D_63_2_.D_63_2 / tc.D_63_count
        END AS D_63_2,
        CASE
            WHEN D_63_3 IS NULL THEN 0
            ELSE D_63_3_.D_63_3 / tc.D_63_count
        END AS D_63_3,
        CASE
            WHEN D_63_4 IS NULL THEN 0
            ELSE D_63_4_.D_63_4 / tc.D_63_count
        END AS D_63_4,
        CASE
            WHEN D_63_5 IS NULL THEN 0
            ELSE D_63_5_.D_63_5 / tc.D_63_count
        END AS D_63_5
    FROM
        total_counts tc
        LEFT JOIN D_63_0_ 
            ON tc.customer_ID = D_63_0_.customer_ID
        LEFT JOIN D_63_1_ 
            ON tc.customer_ID = D_63_1_.customer_ID
        LEFT JOIN D_63_2_ 
            ON tc.customer_ID = D_63_2_.customer_ID
        LEFT JOIN D_63_3_ 
            ON tc.customer_ID = D_63_3_.customer_ID
        LEFT JOIN D_63_4_ 
            ON tc.customer_ID = D_63_4_.customer_ID
        LEFT JOIN D_63_5_ 
            ON tc.customer_ID = D_63_5_.customer_ID
)


SELECT
    u.customer_ID,
    c.D_63_count,
    ISNULL(c.D_63_0, 0) AS D_63_0, 
    ISNULL(c.D_63_1, 0) AS D_63_1, 
    ISNULL(c.D_63_2, 0) AS D_63_2, 
    ISNULL(c.D_63_3, 0) AS D_63_3, 
    ISNULL(c.D_63_4, 0) AS D_63_4, 
    ISNULL(c.D_63_5, 0) AS D_63_5
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;
