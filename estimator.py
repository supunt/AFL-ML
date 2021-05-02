from data_sources import set_load_cached
from sklearn.preprocessing import StandardScaler
from utils.algo_tuner import find_best_algorithms
from utils.features import get_last_x_h2h_feature, get_last_x_h2h_in_ground_feature,\
    get_season_weighted_last_x_h2h_feature, get_last_x_matches_form_feature, \
    get_margin_weighted_last_x_h2h_feature, get_last_x_matches_dominance_feature

set_load_cached(False)

from data_sources import data_store

__year__ = 2019


def estimate(transform_scaler=True, min_season_to_train=2000):
    print('Load data')
    match_results, next_week_frame = data_store.get_cleaned_data()

    match_results['f_home_ground_adv'] = match_results['f_home_ground_adv'].apply(lambda x: 1.0 if x else 0.0)
    match_results['f_away_ground_adv'] = match_results['f_away_ground_adv'].apply(lambda x: 1.0 if x else 0.0)

    # Features START ---------------------------------------------------------------------------------------------------
    last_5_encounter_feature, encounter_5_matrix = get_last_x_h2h_feature(match_results, 5)
    last_5_encounter_ground_feature, encounter_5_ground_matrix = get_last_x_h2h_in_ground_feature(match_results,
                                                                                                         5)
    season_based_last_5_encounter_feature, season_based_encounter_5_matrix = \
        get_season_weighted_last_x_h2h_feature(match_results, 5)

    last_5_match_form_feature, last_5_match_from_frame = get_last_x_matches_form_feature(match_results, 5)

    last_5_matches_h2h_dominance_feature, last_5_h2h_match_dominance_frame = \
        get_margin_weighted_last_x_h2h_feature(match_results, 5)

    last_5_matches_dominance_feature, last_5_match_dominance_frame = \
        get_last_x_matches_dominance_feature(match_results, 5)

    # Features END -----------------------------------------------------------------------------------------------------

    match_results = match_results.merge(last_5_encounter_feature, on="game")
    match_results = match_results.merge(last_5_encounter_ground_feature, on="game")
    match_results = match_results.merge(season_based_last_5_encounter_feature, on="game")
    match_results = match_results.merge(last_5_match_form_feature, on="game", how="left")
    match_results = match_results.merge(last_5_matches_h2h_dominance_feature, on="game", how="left")
    match_results = match_results.merge(last_5_matches_dominance_feature, on="game", how="left")

    # Features Concat END ----------------------------------------------------------------------------------------------

    train_df = match_results[match_results.season == __year__]
    feature_cols = ['f_away_team_id', 'f_home_team_id', 'f_ground_id', 'f_home_odds', 'f_away_odds',
                    'f_home_ground_adv', 'f_away_ground_adv', 'f_last_5_h2h',
                    'f_last_5_h2h_in_ground', 'f_last_5_away_form', 'f_last_5_home_form',
                    'f_margin_weighted_last_5_h2h', 'f_last_5_home_dominance', 'f_last_5_away_dominance']

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
