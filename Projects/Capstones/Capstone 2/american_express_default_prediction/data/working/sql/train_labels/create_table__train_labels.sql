
USE [amex_default]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[train_labels] (
    [customer_ID] VARCHAR(80) NOT NULL,
    [target] INT NULL
);