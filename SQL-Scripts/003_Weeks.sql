CREATE TABLE [dbo].[AFL_Weeks](
	[Season] [int] NOT NULL,
	[Week] [int] NOT NULL,
	[WeekName] [nvarchar](25) NOT NULL,
PRIMARY KEY CLUSTERED
(
	[Season] ASC,
	[Week] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

-------------------------------------------------------------------
-- Insert Meta Data -----------------------------------------------
-------------------------------------------------------------------
TRUNCATE TABLE [dbo].[AFL_Weeks];
INSERT INTO [dbo].[AFL_Weeks] ([Season], [Week], [WeekName]) VALUES
(2021, 1, 'Week_1'),
(2021, 2, 'Week_2'),
(2021, 3, 'Week_3'),
(2021, 4, 'Week_4'),
(2021, 5, 'Week_5'),
(2021, 6, 'Week_6')