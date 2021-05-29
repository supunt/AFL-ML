from data_sources import set_load_cached
from sklearn.preprocessing import StandardScaler
from utils.algo_tuner import __known_best_params__
from sklearn import linear_model
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
from utils.features import get_last_x_h2h_feature, get_cross_team_key, get_last_x_h2h_in_ground_feature, \
    get_cross_team_ground_key, get_season_weighted_last_x_h2h_feature, get_last_x_matches_form_feature, \
    get_margin_weighted_last_x_h2h_feature, get_last_x_matches_dominance_feature
import argparse


set_load_cached(False)

from data_sources import data_store


def persist_data(next_week_inputs: pd.DataFrame, prediction, week_id=None):
    run_id_dt = dt.datetime.now()
    run_id = run_id_dt.strftime('%Y-%m-%d_%H%M%S_%f')[0:-3]
    engine = create_engine('mssql+pyodbc://@localhost/AFL?driver=ODBC+Driver+17+for+SQL+Server', echo=False)

    with engine.begin() as connection:
        next_week_inputs['Run_Id_DateTime'] = run_id_dt
        prediction['Run_Id_DateTime'] = run_id_dt

        next_week_inputs['Run_Id'] = run_id
        prediction['Run_Id'] = run_id

        next_week_inputs['Week'] = week_id if week_id is not None else ''
        prediction['Week'] = week_id if week_id is not None else ''
        prediction = prediction.rename(columns={'result': 'prediction'})

        next_week_inputs.to_sql(name="AFL_Prediction_Inputs", con=connection,
                                schema="dbo", if_exists='append', index=False)

        prediction.to_sql(name="AFL_Predictions", con=connection, schema="dbo", if_exists='append', index=False)


def run_prediction(transform_scaler=True, min_season_to_train=2000, week_id=None, persist=False):
    print('Load data')
    match_results, next_week_frame = data_store.get_cleaned_data(week_id)

    match_results_fwd = match_results.copy().append(next_week_frame)
    match_results_fwd = match_results_fwd.sort_values(by=['game'], ascending=False)

    last_5_encounter_feature, encounter_matrix_frame = get_last_x_h2h_feature(match_results, 5)
    last_5_encounter_in_ground_feature, encounter_in_ground_matrix_frame = get_last_x_h2h_in_ground_feature(
        match_results, 5)

    season_based_last_5_encounter_feature, season_based_encounter_5_matrix_frame = \
        get_season_weighted_last_x_h2h_feature(match_results, 5)

    # last 5 Match Forms
    last_5_match_form_feature, last_5_match_from_frame = get_last_x_matches_form_feature(match_results, 5)

    last_5_matches_h2h_dominance_feature, last_5_match_h2h_dominance_frame = \
        get_margin_weighted_last_x_h2h_feature(match_results,  5)

    last_5_matches_dominance_feature, last_5_match_dominance_frame = \
        get_last_x_matches_dominance_feature(match_results, 5)

    # ------------------------------------------------------------------------------------------------------------------
    # - Adding features to train data set
    # ------------------------------------------------------------------------------------------------------------------
    match_results_fwd = match_results_fwd.merge(last_5_encounter_feature, on="game", how="left")
    match_results_fwd['f_last_5_h2h'] = match_results_fwd['f_last_5_h2h'].fillna(0.0)

    match_results_fwd = match_results_fwd.merge(last_5_encounter_in_ground_feature, on="game", how="left")
    match_results_fwd['f_last_5_h2h_in_ground'] = match_results_fwd['f_last_5_h2h_in_ground'].fillna(0.0)

    match_results_fwd = match_results_fwd.merge(season_based_last_5_encounter_feature, on="game", how="left")
    match_results_fwd['f_season_weighted_last_5_h2h'] = match_results_fwd[
        'f_season_weighted_last_5_h2h'].fillna(0.0)

    match_results_fwd = match_results_fwd.merge(last_5_match_form_feature, on="game", how="left")

    match_results_fwd = match_results_fwd.merge(last_5_matches_h2h_dominance_feature, on="game", how="left")
    match_results_fwd['f_margin_weighted_last_5_h2h'] = match_results_fwd[
        'f_margin_weighted_last_5_h2h'].fillna(0.0)

    match_results_fwd = match_results_fwd.merge(last_5_matches_dominance_feature, on="game", how="left")

    # ------------------------------------------------------------------------------------------------------------------

    train_df = match_results_fwd[~match_results_fwd.game.isin(next_week_frame.game)]

    train_df = train_df[train_df['game'] > min_season_to_train]

    feature_cols_og = ['f_away_team_id', 'f_home_team_id', 'f_ground_id',
                       'f_home_ground_adv', 'f_away_ground_adv', 'f_last_5_h2h', 'f_last_5_h2h_in_ground',
                       'f_season_weighted_last_5_h2h', 'f_last_5_away_form', 'f_last_5_home_form',
                       'f_margin_weighted_last_5_h2h', 'f_last_5_home_dominance', 'f_last_5_away_dominance',
                       'f_home_odds', 'f_away_odds']

    feature_cols = feature_cols_og.copy()
    feature_cols.extend(['game'])

    train_x = train_df.drop(columns=['result'])
    train_y = train_df.result

    if transform_scaler:
        scaler = StandardScaler()
        train_x[feature_cols_og] = scaler.fit_transform(train_x[feature_cols_og])

    train_x = train_x[feature_cols]

    # ------------------------------------------------------------------------------------------------------------------
    # Append Features to next games
    # ------------------------------------------------------------------------------------------------------------------
    # last known rolling 5 per cross team
    next_round_x = match_results_fwd[match_results_fwd.game.isin(next_week_frame.game)].copy()
    next_round_x['comp_key'] = ''
    next_round_x['comp_key'] = next_round_x.apply(lambda df: get_cross_team_key(df['home_team'], df['away_team']),
                                                  axis=1)

    next_round_x = next_round_x.drop(
        columns=['f_last_5_h2h', 'f_last_5_h2h_in_ground', 'f_season_weighted_last_5_h2h',
                 'f_last_5_home_form', 'f_last_5_away_form', 'f_margin_weighted_last_5_h2h',
                 'f_last_5_home_dominance', 'f_last_5_away_dominance'])

    next_round_x['unordered_comp_key'] = next_round_x.apply(
        lambda df: f"{df['home_team'].lower().replace(' ', '_')}::{df['away_team'].lower().replace(' ', '_')}", axis=1)

    next_round_x = next_round_x.merge(encounter_matrix_frame, on='comp_key', how="left")

    next_round_x.loc[
        next_round_x['unordered_comp_key'] != next_round_x['comp_key'], 'f_last_5_h2h'] = -next_round_x[
        'f_last_5_h2h']

    next_round_x = next_round_x.drop(columns=['unordered_comp_key'])

    # last known rolling 5 per cross team on ground --------------------------------------------------------------------
    next_round_x['comp_key'] = ''
    next_round_x['comp_key'] = next_round_x.apply(lambda df: get_cross_team_ground_key(df['home_team'],
                                                                                       df['away_team'],
                                                                                       df['f_ground_id']),
                                                  axis=1)

    next_round_x['unordered_comp_key'] = next_round_x.apply(
        lambda df: f"{df['home_team'].lower().replace(' ', '_')}::{df['away_team'].lower().replace(' ', '_')}::{df['f_ground_id']}", axis=1)

    next_round_x = next_round_x.merge(encounter_in_ground_matrix_frame, on='comp_key', how="left")

    next_round_x.loc[
        next_round_x['unordered_comp_key'] != next_round_x['comp_key'], 'f_last_5_h2h_in_ground'] = - \
        next_round_x[
            'f_last_5_h2h_in_ground']

    next_round_x = next_round_x.drop(columns=['unordered_comp_key', 'comp_key'])

    next_round_x['f_last_5_h2h_in_ground'] = next_round_x['f_last_5_h2h_in_ground'].fillna(0.0)

    # last known WEIGHTED rolling 5 per cross team ---------------------------------------------------------------------
    next_round_x['comp_key'] = ''
    next_round_x['comp_key'] = next_round_x.apply(lambda df: get_cross_team_key(df['home_team'], df['away_team']),
                                                  axis=1)

    next_round_x['unordered_comp_key'] = next_round_x.apply(
        lambda df: f"{df['home_team'].lower().replace(' ', '_')}::{df['away_team'].lower().replace(' ', '_')}", axis=1)

    next_round_x = next_round_x.merge(season_based_encounter_5_matrix_frame, on='comp_key', how="left")

    next_round_x.loc[
        next_round_x['unordered_comp_key'] != next_round_x['comp_key'], 'f_season_weighted_last_5_h2h'] = - \
    next_round_x['f_season_weighted_last_5_h2h']

    next_round_x = next_round_x.drop(columns=['unordered_comp_key'])

    # last known DOMINANCE rolling 5 per cross team --------------------------------------------------------------------
    next_round_x['comp_key'] = ''
    next_round_x['comp_key'] = next_round_x.apply(lambda df: get_cross_team_key(df['home_team'], df['away_team']),
                                                  axis=1)

    next_round_x['unordered_comp_key'] = next_round_x.apply(
        lambda df: f"{df['home_team'].lower().replace(' ', '_')}::{df['away_team'].lower().replace(' ', '_')}", axis=1)

    next_round_x = next_round_x.merge(last_5_match_h2h_dominance_frame, on='comp_key', how="left")

    next_round_x.loc[
        next_round_x['unordered_comp_key'] != next_round_x['comp_key'], 'f_margin_weighted_last_5_h2h'] = - \
        next_round_x['f_margin_weighted_last_5_h2h']

    next_round_x = next_round_x.drop(columns=['unordered_comp_key'])

    # last known form --------------------------------------------------------------------------------------------------
    next_round_x = next_round_x.merge(last_5_match_from_frame, left_on='home_team', right_on='team', how='left')
    next_round_x = next_round_x.rename(columns={"last_5_form": "f_last_5_home_form"})

    next_round_x = next_round_x.merge(last_5_match_from_frame, left_on='away_team', right_on='team', how='left')
    next_round_x = next_round_x.rename(columns={"last_5_form": "f_last_5_away_form"})

    # last known form --------------------------------------------------------------------------------------------------
    next_round_x = next_round_x.merge(last_5_match_dominance_frame, left_on='home_team', right_on='team', how='left')
    next_round_x = next_round_x.rename(columns={"last_5_dominance": "f_last_5_home_dominance"})

    next_round_x = next_round_x.merge(last_5_match_dominance_frame, left_on='away_team', right_on='team', how='left')
    next_round_x = next_round_x.rename(columns={"last_5_dominance": "f_last_5_away_dominance"})

    # ------------------------------------------------------------------------------------------------------------------
    # This frame is for easier visualization
    # ------------------------------------------------------------------------------------------------------------------
    visualizer_cols = ['game', 'date', 'home_team', 'away_team', 'venue',
                       'f_home_ground_adv', 'f_away_ground_adv', 'f_last_5_h2h', 'f_last_5_h2h_in_ground',
                       'f_season_weighted_last_5_h2h', 'f_margin_weighted_last_5_h2h',
                       'f_last_5_home_form', 'f_last_5_home_dominance', 'f_last_5_away_form',
                       'f_last_5_away_dominance', 'f_home_odds', 'f_away_odds']

    next_round_x_useful_stats = next_round_x[visualizer_cols]
    # ------------------------------------------------------------------------------------------------------------------
    # magic happens here
    # ------------------------------------------------------------------------------------------------------------------

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

    if persist:
        persist_data(next_round_x_useful_stats, preds_df, week_id)


# Execute Prediction ---------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate AFL Predictions for next round')
    parser.add_argument('--persist', action='store_true', help='Persist to Database')

    args = parser.parse_args()
    print(f"Persist to Database : {'ENABLED' if args.persist else 'DISABLED'}")

    # run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist, week_id='week-1')
    # run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist, week_id='week-2')
    # run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist, week_id='week-3')
    # run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist, week_id='week-4')
    # run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist, week_id='week-5')
    # run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist, week_id='week-6')
    # run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist, week_id='week-7')
    # run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist, week_id='week-8')
    # run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist, week_id='week-9')
    # run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist, week_id='week-10')
    run_prediction(transform_scaler=True, min_season_to_train=2005, persist=args.persist)
