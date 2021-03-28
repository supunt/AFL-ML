from data_sources import set_load_cached
from sklearn.preprocessing import StandardScaler
from utils.algo_tuner import __known_best_params__
from sklearn import linear_model
import pandas as pd
from utils.features import get_result_last_x_encounters, get_cross_team_key


set_load_cached(False)

from data_sources import data_store


def run_prediction(transform_scaler=True):
    print('Load data')
    match_results, next_week_frame = data_store.get_cleaned_data()

    match_results_fwd = match_results.copy().append(next_week_frame)
    match_results_fwd = match_results_fwd.sort_values(by=['game'], ascending=False)

    last_5_encounter_feature, encounter_matrix = get_result_last_x_encounters(match_results, 5)

    match_results_fwd = match_results_fwd.merge(last_5_encounter_feature, on="game", how="left")
    match_results_fwd['f_last_5_encounters'] = match_results_fwd['f_last_5_encounters'].fillna(0.0)

    train_df = match_results_fwd[~match_results_fwd.game.isin(next_week_frame.game)]

    feature_cols_og = ['f_away_team_id', 'f_home_team_id', 'f_ground_id',
                       'f_home_ground_adv', 'f_away_ground_adv', 'f_last_5_encounters']

    feature_cols = feature_cols_og.copy()
    feature_cols.extend(['game'])

    train_x = train_df.drop(columns=['result'])
    train_y = train_df.result

    if transform_scaler:
        scaler = StandardScaler()
        train_x[feature_cols_og] = scaler.fit_transform(train_x[feature_cols_og])

    train_x = train_x[feature_cols]

    next_round_x = match_results_fwd[match_results_fwd.game.isin(next_week_frame.game)]
    next_round_x['comp_key'] = ''
    next_round_x['comp_key'] = next_round_x.apply(lambda df: get_cross_team_key(df['home_team'], df['away_team']),
                                                  axis=1)

    encounter_matrix_fr_object = {
        'comp_key': [],
        'f_last_5_encounters': []
    }

    for k, v in encounter_matrix.items():
        encounter_matrix_fr_object['comp_key'].append(k)
        encounter_matrix_fr_object['f_last_5_encounters'].append(v)

    encounter_matrix_frame = pd.DataFrame(encounter_matrix_fr_object)
    next_round_x = next_round_x.drop(columns=['f_last_5_encounters'])
    next_round_x = next_round_x.merge(encounter_matrix_frame, on='comp_key')

    bc = linear_model.LogisticRegressionCV(**__known_best_params__)
    bc.fit(train_x[feature_cols], train_y)
    prediction_probs = bc.predict(next_round_x[feature_cols])

    odds = []

    for item in prediction_probs:
        if item > 0.0:
            odds.append('Home')
        elif item < 0.0:
            odds.append('Away')
        elif item == 0.0:
            odds.append('Draw')

    preds_df = (next_round_x[['date', 'home_team', 'away_team', 'venue', 'game']].copy().assign(result=odds))

    preds_df.loc[preds_df['result'] == 'Away', 'result'] = preds_df['away_team']
    preds_df.loc[preds_df['result'] == 'Home', 'result'] = preds_df['home_team']

    print(preds_df.to_string())


# Execute Prediction ---------------------------------------------------------------------------------------------------
run_prediction(transform_scaler=False)
