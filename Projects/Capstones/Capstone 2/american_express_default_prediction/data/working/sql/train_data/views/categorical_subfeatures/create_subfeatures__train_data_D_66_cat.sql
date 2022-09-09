
CREATE VIEW train_data_D_66_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_66) AS FLOAT) AS D_66_count
    FROM train_data
    GROUP BY customer_ID
),
D_66_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_66) AS FLOAT) AS D_66_0
    FROM train_data
    WHERE D_66 = 0.0
    GROUP BY customer_ID
), 
D_66_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_66) AS FLOAT) AS D_66_1
    FROM train_data
    WHERE D_66 = 1.0
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.D_66_count,
        CASE
            WHEN D_66_0 IS NULL THEN 0
            ELSE D_66_0_.D_66_0 / tc.D_66_count
        END AS D_66_0,
        CASE
            WHEN D_66_1 IS NULL THEN 0
            ELSE D_66_1_.D_66_1 / tc.D_66_count
        END AS D_66_1
    FROM
        total_counts tc
        LEFT JOIN D_66_0_ 
            ON tc.customer_ID = D_66_0_.customer_ID
        LEFT JOIN D_66_1_ 
            ON tc.customer_ID = D_66_1_.customer_ID
)


SELECT
    u.customer_ID,
    c.D_66_count,
    ISNULL(c.D_66_0, 0) AS D_66_0, 
    ISNULL(c.D_66_1, 0) AS D_66_1
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;
