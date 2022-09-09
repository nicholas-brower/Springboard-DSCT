
CREATE VIEW train_data_B_38_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(B_38) AS FLOAT) AS B_38_count
    FROM train_data
    GROUP BY customer_ID
),
B_38_0_ AS
(
    SELECT customer_ID, CAST(COUNT(B_38) AS FLOAT) AS B_38_0
    FROM train_data
    WHERE B_38 = 1.0
    GROUP BY customer_ID
), 
B_38_1_ AS
(
    SELECT customer_ID, CAST(COUNT(B_38) AS FLOAT) AS B_38_1
    FROM train_data
    WHERE B_38 = 2.0
    GROUP BY customer_ID
), 
B_38_2_ AS
(
    SELECT customer_ID, CAST(COUNT(B_38) AS FLOAT) AS B_38_2
    FROM train_data
    WHERE B_38 = 3.0
    GROUP BY customer_ID
), 
B_38_3_ AS
(
    SELECT customer_ID, CAST(COUNT(B_38) AS FLOAT) AS B_38_3
    FROM train_data
    WHERE B_38 = 4.0
    GROUP BY customer_ID
), 
B_38_4_ AS
(
    SELECT customer_ID, CAST(COUNT(B_38) AS FLOAT) AS B_38_4
    FROM train_data
    WHERE B_38 = 5.0
    GROUP BY customer_ID
), 
B_38_5_ AS
(
    SELECT customer_ID, CAST(COUNT(B_38) AS FLOAT) AS B_38_5
    FROM train_data
    WHERE B_38 = 6.0
    GROUP BY customer_ID
), 
B_38_6_ AS
(
    SELECT customer_ID, CAST(COUNT(B_38) AS FLOAT) AS B_38_6
    FROM train_data
    WHERE B_38 = 7.0
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.B_38_count,
        CASE
            WHEN B_38_0 IS NULL THEN 0
            ELSE B_38_0_.B_38_0 / tc.B_38_count
        END AS B_38_0,
        CASE
            WHEN B_38_1 IS NULL THEN 0
            ELSE B_38_1_.B_38_1 / tc.B_38_count
        END AS B_38_1,
        CASE
            WHEN B_38_2 IS NULL THEN 0
            ELSE B_38_2_.B_38_2 / tc.B_38_count
        END AS B_38_2,
        CASE
            WHEN B_38_3 IS NULL THEN 0
            ELSE B_38_3_.B_38_3 / tc.B_38_count
        END AS B_38_3,
        CASE
            WHEN B_38_4 IS NULL THEN 0
            ELSE B_38_4_.B_38_4 / tc.B_38_count
        END AS B_38_4,
        CASE
            WHEN B_38_5 IS NULL THEN 0
            ELSE B_38_5_.B_38_5 / tc.B_38_count
        END AS B_38_5,
        CASE
            WHEN B_38_6 IS NULL THEN 0
            ELSE B_38_6_.B_38_6 / tc.B_38_count
        END AS B_38_6
    FROM
        total_counts tc
        LEFT JOIN B_38_0_ 
            ON tc.customer_ID = B_38_0_.customer_ID
        LEFT JOIN B_38_1_ 
            ON tc.customer_ID = B_38_1_.customer_ID
        LEFT JOIN B_38_2_ 
            ON tc.customer_ID = B_38_2_.customer_ID
        LEFT JOIN B_38_3_ 
            ON tc.customer_ID = B_38_3_.customer_ID
        LEFT JOIN B_38_4_ 
            ON tc.customer_ID = B_38_4_.customer_ID
        LEFT JOIN B_38_5_ 
            ON tc.customer_ID = B_38_5_.customer_ID
        LEFT JOIN B_38_6_ 
            ON tc.customer_ID = B_38_6_.customer_ID
)


SELECT
    u.customer_ID,
    c.B_38_count,
    ISNULL(c.B_38_0, 0) AS B_38_0, 
    ISNULL(c.B_38_1, 0) AS B_38_1, 
    ISNULL(c.B_38_2, 0) AS B_38_2, 
    ISNULL(c.B_38_3, 0) AS B_38_3, 
    ISNULL(c.B_38_4, 0) AS B_38_4, 
    ISNULL(c.B_38_5, 0) AS B_38_5, 
    ISNULL(c.B_38_6, 0) AS B_38_6
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;
