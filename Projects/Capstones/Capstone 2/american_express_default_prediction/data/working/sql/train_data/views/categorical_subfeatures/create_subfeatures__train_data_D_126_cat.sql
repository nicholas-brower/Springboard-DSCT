
CREATE VIEW train_data_D_126_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_126) AS FLOAT) AS D_126_count
    FROM train_data
    GROUP BY customer_ID
),
D_126_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_126) AS FLOAT) AS D_126_0
    FROM train_data
    WHERE D_126 = -1.0
    GROUP BY customer_ID
), 
D_126_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_126) AS FLOAT) AS D_126_1
    FROM train_data
    WHERE D_126 = 0.0
    GROUP BY customer_ID
), 
D_126_2_ AS
(
    SELECT customer_ID, CAST(COUNT(D_126) AS FLOAT) AS D_126_2
    FROM train_data
    WHERE D_126 = 1.0
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.D_126_count,
        CASE
            WHEN D_126_0 IS NULL THEN 0
            ELSE D_126_0_.D_126_0 / tc.D_126_count
        END AS D_126_0,
        CASE
            WHEN D_126_1 IS NULL THEN 0
            ELSE D_126_1_.D_126_1 / tc.D_126_count
        END AS D_126_1,
        CASE
            WHEN D_126_2 IS NULL THEN 0
            ELSE D_126_2_.D_126_2 / tc.D_126_count
        END AS D_126_2
    FROM
        total_counts tc
        LEFT JOIN D_126_0_ 
            ON tc.customer_ID = D_126_0_.customer_ID
        LEFT JOIN D_126_1_ 
            ON tc.customer_ID = D_126_1_.customer_ID
        LEFT JOIN D_126_2_ 
            ON tc.customer_ID = D_126_2_.customer_ID
)


SELECT
    u.customer_ID,
    c.D_126_count,
    ISNULL(c.D_126_0, 0) AS D_126_0, 
    ISNULL(c.D_126_1, 0) AS D_126_1, 
    ISNULL(c.D_126_2, 0) AS D_126_2
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;
