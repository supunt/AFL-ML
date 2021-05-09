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
(2021, 1, 'Week-1'),
(2021, 2, 'Week-2'),
(2021, 3, 'Week-3'),
(2021, 4, 'Week-4'),
(2021, 5, 'Week-5'),
(2021, 6, 'Week-6'),
(2021, 7, 'Week-7'),
(2021, 8, 'Week-8')