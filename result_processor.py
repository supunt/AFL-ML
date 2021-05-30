from data_sources import data_store
from sqlalchemy import create_engine


def process_results():
    match_results, next_week_frame = data_store.get_cleaned_data()

    minimized_result_frame = match_results[['game', 'date', 'home_team', 'away_team', 'result']]

    minimized_result_frame = minimized_result_frame.rename(columns={'result': 'winning_team'})

    minimized_result_frame.loc[minimized_result_frame['winning_team'] == 0, 'result'] = 'Draw'
    minimized_result_frame.loc[minimized_result_frame['winning_team'] == 1, 'result'] = minimized_result_frame['home_team']
    minimized_result_frame.loc[minimized_result_frame['winning_team'] == -1, 'result'] = minimized_result_frame['away_team']

    minimized_result_frame = minimized_result_frame.drop(columns=['winning_team'])

    minimized_result_frame = minimized_result_frame.rename(columns={'game': 'Game',
                                                                    'date': 'Date',
                                                                    'home_team': 'Home_team',
                                                                    'away_team': 'Away_team',
                                                                    'result': 'Result'})

    engine = create_engine('mssql+pyodbc://@localhost/AFL?driver=ODBC+Driver+17+for+SQL+Server', echo=False)

    with engine.begin() as connection:
        minimized_result_frame.to_sql(name="AFL_Results", con=connection, schema="dbo", if_exists='replace', index=False)


if __name__ == '__main__':
    process_results()
