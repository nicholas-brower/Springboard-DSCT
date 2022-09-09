
USE [amex_default]
GO
BULK INSERT [dbo].[train_labels]
    FROM 'C:\Users\nick_\Desktop\springboard\Data Science Career Track\Projects\Capstone Projects\Capstone 2\amex_default\data\raw\train_labels.csv'
    WITH (
        FIELDTERMINATOR = ',', 
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    KEEPNULLS
)
;