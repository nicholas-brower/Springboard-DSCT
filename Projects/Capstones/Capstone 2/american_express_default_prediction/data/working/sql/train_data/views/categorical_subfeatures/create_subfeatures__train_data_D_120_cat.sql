
CREATE VIEW train_data_D_120_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_120) AS FLOAT) AS D_120_count
    FROM train_data
    GROUP BY customer_ID
),
D_120_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_120) AS FLOAT) AS D_120_0
    FROM train_data
    WHERE D_120 = 0.0
    GROUP BY customer_ID
), 
D_120_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_120) AS FLOAT) AS D_120_1
    FROM train_data
    WHERE D_120 = 1.0
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.D_120_count,
        CASE
            WHEN D_120_0 IS NULL THEN 0
            ELSE D_120_0_.D_120_0 / tc.D_120_count
        END AS D_120_0,
        CASE
            WHEN D_120_1 IS NULL THEN 0
            ELSE D_120_1_.D_120_1 / tc.D_120_count
        END AS D_120_1
    FROM
        total_counts tc
        LEFT JOIN D_120_0_ 
            ON tc.customer_ID = D_120_0_.customer_ID
        LEFT JOIN D_120_1_ 
            ON tc.customer_ID = D_120_1_.customer_ID
)


SELECT
    u.customer_ID,
    c.D_120_count,
    ISNULL(c.D_120_0, 0) AS D_120_0, 
    ISNULL(c.D_120_1, 0) AS D_120_1
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;
