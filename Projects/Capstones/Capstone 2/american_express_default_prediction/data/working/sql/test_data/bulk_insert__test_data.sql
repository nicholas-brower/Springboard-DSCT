
USE [amex_default]
GO
BULK INSERT [dbo].[test_data]
    FROM 'C:\Users\nick_\Desktop\springboard\Data Science Career Track\Projects\Capstone Projects\Capstone 2\amex_default\data\raw\test_data.csv'
    WITH (
        FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    KEEPNULLS
)
;