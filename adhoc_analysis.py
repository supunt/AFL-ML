from data_sources import data_store
match_results, next_week_frame = data_store.get_cleaned_data(None)

brisbane = match_results[
    (match_results['season'] == 2021) & ((match_results['home_team'] == 'Brisbane') | (match_results['away_team'] == 'Brisbane'))
]

brisbane.to_csv('C:\\Users\\supun\\Desktop\\brisbane_2021.csv')