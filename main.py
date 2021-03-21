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

__year__ = 2020

print('Load data')
match_results, next_week_frame = data_store.get_cleaned_data()
match_results['date'] = match_results['date'].apply(lambda x: dt.datetime.strptime(x[0:10], '%Y-%m-%d').date())


# Create Features ------------------------------------------------------------------------------------------------------
print('Creating Features')

form_btwn_teams = get_form_features(match_results)
features = get_base_feature_frame(match_results)
features = features.merge(form_btwn_teams.drop(columns=['margin']), on=['game', 'home_team', 'away_team'])

# ELO
elos, probs, elo_dict = elo_getter.elo_applier(match_results, 30)

# # Add our created features - elo, efficiency etc.
features_with_elo = (features.assign(f_elo_home=lambda df: df.game.map(elos).apply(lambda x: x[0]),
                                     f_elo_away=lambda df: df.game.map(elos).apply(lambda x: x[1])))

features_with_elo = features_with_elo.merge(match_results[['game', 'home_score', 'away_score']].copy(),
                                            on=['game'], how='inner')

features_with_elo = features_with_elo.rename(columns={'margin': 'f_margin',
                                                      'home_score': 'f_home_score',
                                                      'away_score': 'f_away_score'})

print('Create test and train data sets')
# features_with_elo = features_with_elo.merge(match_results[])
# create a  test and train data sets
feature_columns = [col for col in features_with_elo if col.startswith('f_')]

# Create our test set
test_x = features_with_elo.loc[features_with_elo.season == __year__, ['game'] + feature_columns]
test_y = features_with_elo.loc[features_with_elo.season == __year__, 'result']

# Create our train set
X = features_with_elo.loc[features_with_elo.season != __year__, ['game'] + feature_columns]
Y = features_with_elo.loc[features_with_elo.season != __year__, 'result']

# Scale features
scaler = StandardScaler()
X[feature_columns] = scaler.fit_transform(X[feature_columns])
test_x[feature_columns] = scaler.transform(test_x[feature_columns])

print('Find the best algo')
# best_algos = find_best_algorithms(X, Y)
# chosen_algorithms = best_algos.loc[best_algos['Mean Log Loss'] < 0.67]
# print(chosen_algorithms.to_string())

kfold = StratifiedKFold(n_splits=5)
cv_scores = cross_val_score(ensemble.BaggingClassifier(), X, Y, scoring='accuracy', cv=kfold)
print(cv_scores.mean())


# Define our parameters to run a grid search over
lr_grid = {
    'max_samples': [0.05, 0.1, 0.2, 0.5]
}

alg_list = [ensemble.BaggingClassifier()]
param_list = [lr_grid]

best_estimators = optimise_hyperparameters(X, Y, alg_list, param_list)
lr_best_params = best_estimators[0].get_params()

print(lr_best_params)

cv_scores = cross_val_score(ensemble.BaggingClassifier(**__known_best_params__), X, Y, scoring='accuracy', cv=kfold)
print(cv_scores.mean())


lr = ensemble.BaggingClassifier(**lr_best_params)
lr.fit(X, Y)
final_predictions = lr.predict(test_x)

accuracy = (final_predictions == test_y).mean() * 100

print(f"Our accuracy in predicting the {__year__} season is:" + " {:.2f}%".format(accuracy))

game_ids = test_x[(final_predictions != test_y)].game
# sucked = match_results[match_results.game.isin(game_ids)]

predictions_probs = lr.predict_proba(test_x)


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------

match_results_fwd = match_results.copy().append(next_week_frame)
match_results_fwd = match_results_fwd.sort_values(by=['game'], ascending=False)


form_btwn_teams_fwd = get_form_features(match_results_fwd)
features_fwd = get_base_feature_frame(match_results_fwd)
features_fwd = features_fwd.merge(form_btwn_teams_fwd, on=['game', 'home_team', 'away_team'])

# ELO
elos_fwd, probs_fwd, elo_dict_fwd = elo_getter.elo_applier(match_results_fwd, 30)

# # Add our created features - elo, efficiency etc.
features_fwd_with_elo = (features_fwd.assign(
    f_elo_home=lambda df: df.game.map(elos_fwd).apply(lambda x: x[0]),
    f_elo_away=lambda df: df.game.map(elos_fwd).apply(lambda x: x[1])))

# features_fwd_with_elo = features_fwd_with_elo.merge(match_results_fwd, on=['game'], how='inner')

features_fwd_with_elo = features_fwd_with_elo.rename(columns={
    'margin': 'f_margin',
    'home_score': 'f_home_score',
    'away_score': 'f_away_score'
})


train_df = features_fwd_with_elo[~features_fwd_with_elo.game.isin(next_week_frame.game)]
feature_cols = [col for col in train_df if col.startswith('f_') and col]


train_x = train_df.drop(columns=['result'])
train_y = train_df.result
next_round_x = features_fwd_with_elo[features_fwd_with_elo.game.isin(next_week_frame.game)]

scaler = StandardScaler()
scaler.fit(train_x)

le = preprocessing.LabelEncoder()
for column_name in train_x.columns:
    if train_x[column_name].dtype == object:
        train_x[column_name] = le.fit_transform(train_x[column_name])
    else:
        pass

next_round_x[feature_cols] = scaler.transform(next_round_x[feature_cols])

bc = ensemble.BaggingClassifier(**__known_best_params__)
# from sklearn.linear_model import LogisticRegression
# bc = LogisticRegression()
bc.fit(train_x, train_y)
prediction_probs = bc.predict_proba(next_round_x)

modelled_home_odds = [1/i[1] for i in prediction_probs]
modelled_away_odds = [1/i[0] for i in prediction_probs]

preds_df = (next_round_x[['date', 'home_team', 'away_team', 'venue', 'game']].copy()
               .assign(modelled_home_odds=modelled_home_odds,
                       modelled_away_odds=modelled_away_odds)

           )

print(preds_df.to_string())

