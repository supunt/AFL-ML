alter table [dbo].[AFL_Prediction_Inputs] ADD [F_This_Season_Home_Form] decimal(10,3) NOT NULL DEFAULT 0.0
alter table [dbo].[AFL_Prediction_Inputs] ADD [F_This_Season_Away_Form] decimal(10,3) NOT NULL DEFAULT 0.0
alter table [dbo].[AFL_Prediction_Inputs] ADD [F_This_Season_H2H] decimal(10,3) NOT NULL DEFAULT 0.0
alter table [dbo].[AFL_Prediction_Inputs] ADD [This_Season_Home_Results] varchar(max) NULL DEFAULT ''
alter table [dbo].[AFL_Prediction_Inputs] ADD [This_Season_Away_Results] varchar(max) NULL DEFAULT ''


alter table [dbo].[AFL_Prediction_Inputs] ADD [This_Season_Away_Results_Detailed] varchar(max) NULL DEFAULT ''
alter table [dbo].[AFL_Prediction_Inputs] ADD [This_Season_Home_Results_Detailed] varchar(max) NULL DEFAULT ''
