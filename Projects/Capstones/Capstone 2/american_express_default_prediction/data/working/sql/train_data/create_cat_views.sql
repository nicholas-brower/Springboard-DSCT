
GO

CREATE VIEW train_data_B_30_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(B_30) AS FLOAT) AS B_30_count
    FROM train_data
    GROUP BY customer_ID
),
B_30_0_ AS
(
    SELECT customer_ID, CAST(COUNT(B_30) AS FLOAT) AS B_30_0
    FROM train_data
    WHERE B_30 = 0.0
    GROUP BY customer_ID
), 
B_30_1_ AS
(
    SELECT customer_ID, CAST(COUNT(B_30) AS FLOAT) AS B_30_1
    FROM train_data
    WHERE B_30 = 1.0
    GROUP BY customer_ID
), 
B_30_2_ AS
(
    SELECT customer_ID, CAST(COUNT(B_30) AS FLOAT) AS B_30_2
    FROM train_data
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

GO

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

GO

CREATE VIEW train_data_D_114_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_114) AS FLOAT) AS D_114_count
    FROM train_data
    GROUP BY customer_ID
),
D_114_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_114) AS FLOAT) AS D_114_0
    FROM train_data
    WHERE D_114 = 0.0
    GROUP BY customer_ID
), 
D_114_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_114) AS FLOAT) AS D_114_1
    FROM train_data
    WHERE D_114 = 1.0
    GROUP BY customer_ID
),
cat_pct AS
(
    SELECT
        tc.customer_ID,
        tc.D_114_count,
        CASE
            WHEN D_114_0 IS NULL THEN 0
            ELSE D_114_0_.D_114_0 / tc.D_114_count
        END AS D_114_0,
        CASE
            WHEN D_114_1 IS NULL THEN 0
            ELSE D_114_1_.D_114_1 / tc.D_114_count
        END AS D_114_1
    FROM
        total_counts tc
        LEFT JOIN D_114_0_ 
            ON tc.customer_ID = D_114_0_.customer_ID
        LEFT JOIN D_114_1_ 
            ON tc.customer_ID = D_114_1_.customer_ID
)


SELECT
    u.customer_ID,
    c.D_114_count,
    ISNULL(c.D_114_0, 0) AS D_114_0, 
    ISNULL(c.D_114_1, 0) AS D_114_1
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.customer_ID = c.customer_ID
;

GO

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

GO

CREATE VIEW train_data_D_117_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_count
    FROM train_data
    GROUP BY customer_ID
),
D_117_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_0
    FROM train_data
    WHERE D_117 = -1.0
    GROUP BY customer_ID
), 
D_117_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_1
    FROM train_data
    WHERE D_117 = 1.0
    GROUP BY customer_ID
), 
D_117_2_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_2
    FROM train_data
    WHERE D_117 = 2.0
    GROUP BY customer_ID
), 
D_117_3_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_3
    FROM train_data
    WHERE D_117 = 3.0
    GROUP BY customer_ID
), 
D_117_4_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_4
    FROM train_data
    WHERE D_117 = 4.0
    GROUP BY customer_ID
), 
D_117_5_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_5
    FROM train_data
    WHERE D_117 = 5.0
    GROUP BY customer_ID
), 
D_117_6_ AS
(
    SELECT customer_ID, CAST(COUNT(D_117) AS FLOAT) AS D_117_6
    FROM train_data
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

GO

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

GO

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

GO

CREATE VIEW train_data_D_63_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_count
    FROM train_data
    GROUP BY customer_ID
),
D_63_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_0
    FROM train_data
    WHERE D_63 = 'CL'
    GROUP BY customer_ID
), 
D_63_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_1
    FROM train_data
    WHERE D_63 = 'CO'
    GROUP BY customer_ID
), 
D_63_2_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_2
    FROM train_data
    WHERE D_63 = 'CR'
    GROUP BY customer_ID
), 
D_63_3_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_3
    FROM train_data
    WHERE D_63 = 'XL'
    GROUP BY customer_ID
), 
D_63_4_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_4
    FROM train_data
    WHERE D_63 = 'XM'
    GROUP BY customer_ID
), 
D_63_5_ AS
(
    SELECT customer_ID, CAST(COUNT(D_63) AS FLOAT) AS D_63_5
    FROM train_data
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

GO

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

GO

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

GO

CREATE VIEW train_data_D_68_cat AS

WITH
u_ids AS
(
    SELECT DISTINCT customer_ID FROM train_data
),
total_counts AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_count
    FROM train_data
    GROUP BY customer_ID
),
D_68_0_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_0
    FROM train_data
    WHERE D_68 = 0.0
    GROUP BY customer_ID
), 
D_68_1_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_1
    FROM train_data
    WHERE D_68 = 1.0
    GROUP BY customer_ID
), 
D_68_2_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_2
    FROM train_data
    WHERE D_68 = 2.0
    GROUP BY customer_ID
), 
D_68_3_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_3
    FROM train_data
    WHERE D_68 = 3.0
    GROUP BY customer_ID
), 
D_68_4_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_4
    FROM train_data
    WHERE D_68 = 4.0
    GROUP BY customer_ID
), 
D_68_5_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_5
    FROM train_data
    WHERE D_68 = 5.0
    GROUP BY customer_ID
), 
D_68_6_ AS
(
    SELECT customer_ID, CAST(COUNT(D_68) AS FLOAT) AS D_68_6
    FROM train_data
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
