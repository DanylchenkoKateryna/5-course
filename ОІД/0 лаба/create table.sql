USE [Student]
GO

/****** Object:  Table [dbo].[StudentTest]    Script Date: 19.10.2025 21:18:16 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[StudentTest](
	[St_ID] [int] IDENTITY(1,1) NOT NULL,
	[Recordbook] [int] NULL,
	[First_Name] [nvarchar](30) NULL,
	[Last_Name] [nvarchar](30) NULL,
	[Date_of_Birth] [date] NULL,
	[Enter_Date] [date] NULL
) ON [PRIMARY]
GO


