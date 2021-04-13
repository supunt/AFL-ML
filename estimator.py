from data_sources import set_load_cached
from sklearn.preprocessing import StandardScaler
from utils.algo_tuner import find_best_algorithms
from utils.features import get_result_last_x_encounters, get_result_last_x_encounters_in_ground,\
    get_result_season_weighted_last_x_encounters

set_load_cached(False)

from data_sources import data_store

__year__ = 2019


def estimate(transform_scaler=True, min_season_to_train=2000):
    print('Load data')
    match_results, next_week_frame = data_store.get_cleaned_data()

    match_results['f_home_ground_adv'] = match_results['f_home_ground_adv'].apply(lambda x: 1.0 if x else 0.0)
    match_results['f_away_ground_adv'] = match_results['f_away_ground_adv'].apply(lambda x: 1.0 if x else 0.0)

    # Append last 5 encounter results
    last_5_encounter_feature, encounter_5_matrix = get_result_last_x_encounters(match_results, 5)
    last_5_encounter_ground_feature, encounter_5_ground_matrix = get_result_last_x_encounters_in_ground(match_results,
                                                                                                        5)
    season_based_last_5_encounter_feature, season_based_encounter_5_matrix = \
        get_result_season_weighted_last_x_encounters(match_results, 5)

    match_results = match_results.merge(last_5_encounter_feature, on="game")
    match_results = match_results.merge(last_5_encounter_ground_feature, on="game")
    match_results = match_results.merge(season_based_last_5_encounter_feature, on="game")

    train_df = match_results[match_results.season == __year__]
    feature_cols = ['f_away_team_id', 'f_home_team_id', 'f_ground_id',
                    'f_home_ground_adv', 'f_away_ground_adv', 'f_last_5_encounters',
                    'f_last_5_encounters_in_ground']

    feature_cols_original = feature_cols.copy()
    feature_cols.extend(['game'])

    # Create our train set
    X = match_results[match_results.season > min_season_to_train][feature_cols]
    Y = match_results.loc[match_results.season > min_season_to_train, 'result']

    if transform_scaler:
        scaler = StandardScaler()
        X[feature_cols_original] = scaler.fit_transform(X[feature_cols_original])

    print('Find the best algo')
    algorithm_perf = find_best_algorithms(X, Y, kf_splits=5)
    print(algorithm_perf.to_string())

    chosen_algorithms = algorithm_perf.loc[algorithm_perf['Mean Log Loss'] < 0.67]

    print("\nWith 'Mean Log Loss' < 2/3\n", "---------------------------------\n", chosen_algorithms.to_string())


# Execute Estimation ---------------------------------------------------------------------------------------------------
estimate(transform_scaler=True, min_season_to_train=2005)
