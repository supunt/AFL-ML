from data_sources import set_load_cached
import pandas as pd


set_load_cached(True)

from data_sources import data_store
from utils import elo_getter


match_results = data_store.get_cleaned_data()


form_btwn_teams = match_results[['game', 'home_team', 'away_team', 'margin']].copy()

form_btwn_teams['f_form_margin_btwn_teams'] = (match_results.groupby(['home_team', 'away_team'])['margin']
                                               .transform(lambda row: row.rolling(5).mean().shift())
                                               .fillna(0))

form_btwn_teams['f_form_past_5_btwn_teams'] = \
(match_results.assign(win=lambda df: df.apply(lambda row: 1 if row.margin > 0 else 0, axis='columns'))
              .groupby(['home_team', 'away_team'])['result']
              .transform(lambda row: row.rolling(5).mean().shift() * 5)
              .fillna(0))


# Create Features ------------------------------------------------------------------------------------------------------
features = match_results[['date', 'game', 'home_team', 'away_team',
                          'venue', 'f_home_ground_adv', 'f_away_ground_adv']].copy()

features = features.merge(form_btwn_teams.drop(columns=['margin']), on=['game', 'home_team', 'away_team'])


# ELO
elos, probs, elo_dict = elo_getter.elo_applier(match_results, 30)
x=1