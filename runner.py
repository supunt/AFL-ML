from data_sources import set_load_cached
from sklearn import ensemble
import datetime as dt
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import StratifiedKFold, cross_val_score
from utils import elo_getter
from utils.algo_tuner import find_best_algorithms, optimise_hyperparameters, __known_best_params__
from utils.features import get_form_features, get_base_feature_frame
from sklearn import preprocessing

set_load_cached(False)

from data_sources import data_store

__year__ = 2019

print('Load data')
match_results, next_week_frame = data_store.get_cleaned_data()
match_results['date'] = match_results['date'].apply(lambda x: dt.datetime.strptime(x[0:10], '%Y-%m-%d').date())


match_results_fwd = match_results.copy().append(next_week_frame)
match_results_fwd = match_results_fwd.sort_values(by=['game'], ascending=False)


form_btwn_teams_fwd = get_form_features(match_results_fwd)
features_fwd = get_base_feature_frame(match_results_fwd)
features_fwd = features_fwd.merge(form_btwn_teams_fwd, on=['game', 'home_team', 'away_team',
                                                           'home_team_id', 'away_team_id', 'ground_id'])

# ELO
elos_fwd, probs_fwd, elo_dict_fwd = elo_getter.elo_applier(match_results_fwd, 30)

# # Add our created features - elo, efficiency etc.
features_fwd_with_elo = (features_fwd.assign(
    f_elo_home=lambda df: df.game.map(elos_fwd).apply(lambda x: x[0]),
    f_elo_away=lambda df: df.game.map(elos_fwd).apply(lambda x: x[1])))

features_fwd_with_elo = features_fwd_with_elo.merge(match_results_fwd[['game', 'home_score', 'away_score']],
                                                    on="game",
                                                    how="inner")

features_fwd_with_elo = features_fwd_with_elo.rename(columns={
    'margin': 'f_margin',
    'home_ground_adv': 'f_home_ground_adv',
    'away_ground_adv': 'f_away_ground_adv',
    'away_team_id': 'f_away_team_id',
    'home_team_id': 'f_home_team_id',
    'ground_id': 'f_ground_id'
})


train_df = features_fwd_with_elo[~features_fwd_with_elo.game.isin(next_week_frame.game)]
feature_cols = []
feature_cols.extend(['f_away_team_id', 'f_home_team_id', 'f_ground_id', 'f_home_ground_adv', 'f_away_ground_adv'])


train_x = train_df.drop(columns=['result'])
train_y = train_df.result

train_x['f_home_ground_adv'] = train_x['f_home_ground_adv'].apply(lambda x: 1.0 if True else 0.0)
train_x['f_away_ground_adv'] = train_x['f_away_ground_adv'].apply(lambda x: 1.0 if True else 0.0)

next_round_x = features_fwd_with_elo[features_fwd_with_elo.game.isin(next_week_frame.game)]

next_round_x['f_home_ground_adv'] = next_round_x['f_home_ground_adv'].apply(lambda x: 1.0 if True else 0.0)
next_round_x['f_away_ground_adv'] = next_round_x['f_away_ground_adv'].apply(lambda x: 1.0 if True else 0.0)


bc = ensemble.BaggingClassifier(**__known_best_params__)
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