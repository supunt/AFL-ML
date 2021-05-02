CREATE OR ALTER VIEW [dbo].[GetAFLPredictions] AS

	WITH CTE( MaxRunIdDt, [Week]) AS (SELECT DISTINCT
		MAX(preds.[Run_Id_DateTime]) as MaxRunIdDt,
		preds.[Week]
	FROM [dbo].[AFL_Predictions] preds
	GROUP BY preds.[Week])

	SELECT
		pred_Inputs.Run_Id,
		pred_Inputs.Game,
		pred_Inputs.[Date],
		pred_Inputs.[Week],
		pred_Inputs.Home_Team,
		pred_Inputs.Away_Team,
		pred_Inputs.Venue,
		pred_Inputs.F_Home_Ground_Adv,
		pred_Inputs.F_Away_Ground_Adv,
		pred_Inputs.F_Last_5_h2h,
		pred_Inputs.F_Last_5_h2h_In_Ground,
		pred_Inputs.F_Season_Weighted_Last_5_h2h,
		pred_Inputs.F_Margin_Weighted_Last_5_h2h,
		pred_Inputs.F_Last_5_Home_Form,
		pred_Inputs.F_Last_5_Home_Dominance,
		pred_Inputs.F_Last_5_Away_Form,
		pred_Inputs.F_Last_5_Away_Dominance,
		pred_Inputs.F_Home_Odds,
		pred_Inputs.F_Away_Odds,
		preds.Prediction,
		res.Result,
		Accurate = CASE WHEN res.Result = preds.Prediction THEN 1 ELSE 0 END
	FROM [dbo].[AFL_Predictions] preds
	INNER JOIN [dbo].[AFL_Prediction_Inputs] pred_Inputs ON pred_Inputs.game = preds.game and pred_Inputs.Run_Id_DateTime = preds.Run_Id_DateTime
	INNER JOIN CTE ON CTE.MaxRunIdDt = pred_Inputs.Run_Id_DateTime
	LEFT JOIN [dbo].[AFL_Results] res on res.game = pred_Inputs.game and res.[date] = pred_Inputs.[date]