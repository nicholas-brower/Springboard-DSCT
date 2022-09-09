
CREATE VIEW train_data_D_116_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_116) AS FLOAT) AS D_116_count
    FROM train_data
    GROUP BY customer_ID
),
D_116_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_116) AS FLOAT) AS D_116_0
    FROM train_data
    WHERE D_116 = 0.0
    GROUP BY customer_ID
), 
D_116_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_116) AS FLOAT) AS D_116_1
    FROM train_data
    WHERE D_116 = 1.0
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.D_116_count,
        CASE
            WHEN D_116_0 IS NULL THEN 0
            ELSE D_116_0_.D_116_0 / tc.D_116_count
        END AS D_116_0,
        CASE
            WHEN D_116_1 IS NULL THEN 0
            ELSE D_116_1_.D_116_1 / tc.D_116_count
        END AS D_116_1
    FROM
        total_counts tc
        LEFT JOIN D_116_0_ 
            ON tc.customer_ID = D_116_0_.customer_ID
        LEFT JOIN D_116_1_ 
            ON tc.customer_ID = D_116_1_.customer_ID
)


SELECT
    u.customer_ID,
    c.D_116_count,
    ISNULL(c.D_116_0, 0) AS D_116_0, 
    ISNULL(c.D_116_1, 0) AS D_116_1
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;
