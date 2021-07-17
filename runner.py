from data_sources import set_load_cached
from sklearn.preprocessing import StandardScaler
from utils.algo_tuner import __known_best_params__
from sklearn import linear_model
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

from utils.features.base_features import get_cross_team_ground_key, get_cross_team_key
from utils.features.last_x_h2h_feature import get_last_x_h2h_feature
from utils.features.last_x_h2h_in_ground_feature import get_last_x_h2h_in_ground_feature
from utils.features.season_weighted_last_x_h2h_feature import get_season_weighted_last_x_h2h_feature
from utils.features.last_x_matches_form_feature import get_last_x_matches_form_feature
from utils.features.margin_weighted_last_x_h2h_feature import get_margin_weighted_last_x_h2h_feature
from utils.features.last_x_matches_dominance_feature import get_last_x_matches_dominance_feature
from utils.features.this_season_h2h_feature import get_this_season_h2h_feature

from data_sources import data_store

from utils.features.this_season_form_feature import get_this_season_matches_form_feature

import argparse
from result_processor import process_results


def persist_data(next_week_inputs_frame: pd.DataFrame, prediction, week_id=None):
    next_week_inputs = next_week_inputs_frame.copy()
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

    this_season_form_feature, this_season_form_frame = get_this_season_matches_form_feature(match_results, 2021)
    this_season_encounter_feature, this_season_encounter_matrix = get_this_season_h2h_feature(match_results, 2021)

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

    match_results_fwd = match_results_fwd.merge(this_season_form_feature, on="game", how="left")
    match_results_fwd['f_this_season_home_form'] = match_results_fwd['f_this_season_home_form'].fillna(0.0)
    match_results_fwd['f_this_season_away_form'] = match_results_fwd['f_this_season_away_form'].fillna(0.0)

    match_results_fwd = match_results_fwd.merge(this_season_encounter_feature, on="game", how="left")
    match_results_fwd['f_this_season_h2h'] = match_results_fwd['f_this_season_h2h'].fillna(0.0)

    # ------------------------------------------------------------------------------------------------------------------

    train_df = match_results_fwd[~match_results_fwd.game.isin(next_week_frame.game)]

    train_df = train_df[train_df['game'] > min_season_to_train]

    feature_cols_og = ['f_away_team_id', 'f_home_team_id', 'f_ground_id',
                       'f_home_ground_adv', 'f_away_ground_adv', 'f_last_5_h2h', 'f_last_5_h2h_in_ground',
                       'f_season_weighted_last_5_h2h', 'f_last_5_away_form', 'f_last_5_home_form',
                       'f_margin_weighted_last_5_h2h', 'f_last_5_home_dominance', 'f_last_5_away_dominance',
                       'f_home_odds', 'f_away_odds',
                       'f_this_season_home_form', 'f_this_season_away_form',
                       'f_this_season_h2h']

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
                 'f_last_5_home_dominance', 'f_last_5_away_dominance',
                 'f_this_season_home_form', 'f_this_season_away_form', 'f_this_season_h2h'])

    # FEATURE :: Last 5 h2h --------------------------------------------------------------------------------------------
    next_round_x['unordered_comp_key'] = next_round_x.apply(
        lambda df: f"{df['home_team'].lower().replace(' ', '_')}::{df['away_team'].lower().replace(' ', '_')}", axis=1)

    next_round_x = next_round_x.merge(encounter_matrix_frame, on='comp_key', how="left")

    next_round_x.loc[
        next_round_x['unordered_comp_key'] != next_round_x['comp_key'], 'f_last_5_h2h'] = -next_round_x[
        'f_last_5_h2h']

    next_round_x = next_round_x.drop(columns=['unordered_comp_key'])

    # FEATURE :: last known rolling 5 per cross team on ground ---------------------------------------------------------
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

    # FEATURE :: last known WEIGHTED rolling 5 per cross team ----------------------------------------------------------
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

    # FEATURE :: last known DOMINANCE rolling 5 per cross team ---------------------------------------------------------
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

    # FEATURE :: last known form ---------------------------------------------------------------------------------------
    next_round_x = next_round_x.merge(last_5_match_from_frame, left_on='home_team', right_on='team', how='left')
    next_round_x = next_round_x.rename(columns={"last_5_form": "f_last_5_home_form"})

    next_round_x = next_round_x.merge(last_5_match_from_frame, left_on='away_team', right_on='team', how='left')
    next_round_x = next_round_x.rename(columns={"last_5_form": "f_last_5_away_form"})

    # FEATURE :: last known dominance ----------------------------------------------------------------------------------
    next_round_x = next_round_x.merge(last_5_match_dominance_frame, left_on='home_team', right_on='team', how='left')
    next_round_x = next_round_x.rename(columns={"last_5_dominance": "f_last_5_home_dominance"})

    next_round_x = next_round_x.merge(last_5_match_dominance_frame, left_on='away_team', right_on='team', how='left')
    next_round_x = next_round_x.rename(columns={"last_5_dominance": "f_last_5_away_dominance"})

    # FEATURE :: this season form --------------------------------------------------------------------------------------
    next_round_x = next_round_x.merge(this_season_form_frame, left_on='home_team', right_on='team', how='left')
    next_round_x = next_round_x.rename(columns={"this_season_form": "f_this_season_home_form"})
    next_round_x = next_round_x.rename(columns={"this_season_results": "this_season_home_results"})
    next_round_x = next_round_x.rename(columns={"this_season_results_detailed": "this_season_home_results_detailed"})

    next_round_x['f_this_season_home_form'] = next_round_x['f_this_season_home_form'].fillna(0.0)

    next_round_x = next_round_x.merge(this_season_form_frame, left_on='away_team', right_on='team', how='left')
    next_round_x = next_round_x.rename(columns={"this_season_form": "f_this_season_away_form"})
    next_round_x = next_round_x.rename(columns={"this_season_results": "this_season_away_results"})
    next_round_x = next_round_x.rename(columns={"this_season_results_detailed": "this_season_away_results_detailed"})

    next_round_x['f_this_season_away_form'] = next_round_x['f_this_season_away_form'].fillna(0.0)

    # FEATURE ::  This season h2h --------------------------------------------------------------------------------------
    next_round_x['unordered_comp_key'] = next_round_x.apply(
        lambda df: f"{df['home_team'].lower().replace(' ', '_')}::{df['away_team'].lower().replace(' ', '_')}", axis=1)

    next_round_x = next_round_x.merge(this_season_encounter_matrix, on='comp_key', how="left")

    next_round_x.loc[
        next_round_x['unordered_comp_key'] != next_round_x['comp_key'], 'f_this_season_h2h'] = -next_round_x[
        'f_this_season_h2h']
    next_round_x['f_this_season_h2h'] = next_round_x['f_this_season_h2h'].fillna(0.0)

    next_round_x = next_round_x.drop(columns=['unordered_comp_key'])

    # ------------------------------------------------------------------------------------------------------------------
    # This frame is for easier visualization
    # ------------------------------------------------------------------------------------------------------------------
    visualizer_cols = ['game', 'date', 'home_team', 'away_team', 'venue',
                       'f_home_ground_adv', 'f_away_ground_adv', 'f_last_5_h2h', 'f_last_5_h2h_in_ground',
                       'f_season_weighted_last_5_h2h', 'f_margin_weighted_last_5_h2h',
                       'f_last_5_home_form', 'f_last_5_home_dominance', 'f_last_5_away_form',
                       'f_last_5_away_dominance', 'f_home_odds', 'f_away_odds',
                       'f_this_season_home_form', 'f_this_season_away_form',
                       'this_season_home_results', 'this_season_away_results',
                       'this_season_home_results_detailed', 'this_season_away_results_detailed',
                       'f_this_season_h2h']

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
from concurrent.futures import ThreadPoolExecutor
import concurrent


if __name__ == "__main__":
    max_week = 17
    parser = argparse.ArgumentParser(description='Generate AFL Predictions for next round')
    parser.add_argument('--persist', action='store_true', help='Persist to Database')
    parser.add_argument('--run_history', action='store_true', help='Persist to Database')

    args = parser.parse_args()
    print(f"Persist to Database : {'ENABLED' if args.persist else 'DISABLED'}")

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {}
        if args.run_history:
            print("Executing History....")
            futures = {executor.submit(run_prediction, True, 2015, f'week-{i}', args.persist): i for i in
                       range(1, max_week + 1)}

        print("Executing Next week....")
        futures[executor.submit(run_prediction, True, 2015, None, args.persist)] = ''
        for future in concurrent.futures.as_completed(futures):
            week_id = futures[future]
            try:
                future.result()
            except Exception as exc:
                print('Week : %r generated an exception: %s' % (week_id, exc))

    process_results()

