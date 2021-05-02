from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('mssql+pyodbc://@localhost/AFL?driver=ODBC+Driver+17+for+SQL+Server', echo=False)
connection = engine.connect()

result_data = pd.read_sql('SELECT * FROM [dbo].[GetAFLPredictions] WHERE RESULT IS NOT NULL', connection)
summary = result_data.groupby(['Week']).agg({'Game': 'count', 'Accurate': 'sum'})
summary = summary.rename(columns={'Game': 'Number_of_Games', 'Accurate': 'Correct_Predictions'})
summary = summary.reset_index()
print("Weekly Prediction Accuracy summary")
print(summary.to_string())
