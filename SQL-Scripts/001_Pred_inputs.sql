CREATE TABLE [dbo].[AFL_Prediction_Inputs](
	[Run_Id] [nvarchar](50) NOT NULL,
	[Run_Id_DateTime] [datetime] NOT NULL,
	[Game] [int] NOT NULL,
	[Date] [date] NOT NULL,
	[Week] [nvarchar](25) NOT NULL,
	[Home_Team] [nvarchar](25) NOT NULL,
	[Away_Team] [nvarchar](25) NOT NULL,
	[Venue] [nvarchar](50) NOT NULL,
	[F_Home_Ground_Adv] [decimal](1, 0) NOT NULL,
	[F_Away_Ground_Adv] [decimal](1, 0) NOT NULL,
	[F_Last_5_h2h] [decimal](1, 0) NOT NULL,
	[F_Last_5_h2h_In_Ground] [decimal](10, 3) NOT NULL,
	[F_Season_Weighted_Last_5_h2h] [decimal](10, 3) NOT NULL,
	[F_Margin_Weighted_Last_5_h2h] [decimal](10, 3) NOT NULL,
	[F_Last_5_Home_Form] [decimal](10, 3) NOT NULL,
	[F_Last_5_Home_Dominance] [decimal](10, 3) NOT NULL,
	[F_Last_5_Away_Form] [decimal](10, 3) NOT NULL,
	[F_Last_5_Away_Dominance] [decimal](10, 3) NOT NULL,
	[F_Home_Odds] [decimal](4, 2) NOT NULL,
	[F_Away_Odds] [decimal](4, 2) NULL,
PRIMARY KEY CLUSTERED
(
	[Run_Id_DateTime] ASC,
	[Game] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]