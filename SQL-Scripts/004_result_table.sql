CREATE TABLE [dbo].[AFL_Results](
	[Game] [int] NOT NULL,
	[Date] [date] NOT NULL,
	[Home_Team] [nvarchar](25) NOT NULL,
	[Away_Team] [nvarchar](25) NOT NULL,
	[Result] [nvarchar](25) NOT NULL,
    PRIMARY KEY CLUSTERED
    (
        [Game] ASC
    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]