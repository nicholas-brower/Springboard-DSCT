
CREATE VIEW train_data_D_64_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_64) AS FLOAT) AS D_64_count
    FROM train_data
    GROUP BY customer_ID
),
D_64_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_64) AS FLOAT) AS D_64_0
    FROM train_data
    WHERE D_64 = '-1'
    GROUP BY customer_ID
), 
D_64_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_64) AS FLOAT) AS D_64_1
    FROM train_data
    WHERE D_64 = 'O'
    GROUP BY customer_ID
), 
D_64_2_ AS
(
    SELECT customer_ID, CAST(COUNT(D_64) AS FLOAT) AS D_64_2
    FROM train_data
    WHERE D_64 = 'R'
    GROUP BY customer_ID
), 
D_64_3_ AS
(
    SELECT customer_ID, CAST(COUNT(D_64) AS FLOAT) AS D_64_3
    FROM train_data
    WHERE D_64 = 'U'
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.D_64_count,
        CASE
            WHEN D_64_0 IS NULL THEN 0
            ELSE D_64_0_.D_64_0 / tc.D_64_count
        END AS D_64_0,
        CASE
            WHEN D_64_1 IS NULL THEN 0
            ELSE D_64_1_.D_64_1 / tc.D_64_count
        END AS D_64_1,
        CASE
            WHEN D_64_2 IS NULL THEN 0
            ELSE D_64_2_.D_64_2 / tc.D_64_count
        END AS D_64_2,
        CASE
            WHEN D_64_3 IS NULL THEN 0
            ELSE D_64_3_.D_64_3 / tc.D_64_count
        END AS D_64_3
    FROM
        total_counts tc
        LEFT JOIN D_64_0_ 
            ON tc.customer_ID = D_64_0_.customer_ID
        LEFT JOIN D_64_1_ 
            ON tc.customer_ID = D_64_1_.customer_ID
        LEFT JOIN D_64_2_ 
            ON tc.customer_ID = D_64_2_.customer_ID
        LEFT JOIN D_64_3_ 
            ON tc.customer_ID = D_64_3_.customer_ID
)


SELECT
    u.customer_ID,
    c.D_64_count,
    ISNULL(c.D_64_0, 0) AS D_64_0, 
    ISNULL(c.D_64_1, 0) AS D_64_1, 
    ISNULL(c.D_64_2, 0) AS D_64_2, 
    ISNULL(c.D_64_3, 0) AS D_64_3
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;
