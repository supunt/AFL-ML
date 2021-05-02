CREATE TABLE [dbo].[AFL_Predictions](
	[Run_Id_] [nvarchar](50) NOT NULL,
	[Run_Id_DateTime] [datetime] NOT NULL,
	[Game] [int] NOT NULL,
	[Date] [date] NOT NULL,
	[Week] [nvarchar](25) NOT NULL,
	[Home_Team] [nvarchar](25) NOT NULL,
	[Away_Team] [nvarchar](25) NOT NULL,
	[Venue] [nvarchar](50) NOT NULL,
	[Prediction] [nvarchar](25) NOT NULL,
	[Prediction] [Result](25) NULL,
    PRIMARY KEY CLUSTERED
    (
        [Run_Id_DateTime] ASC,
        [Game] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]