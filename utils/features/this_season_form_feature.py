import pandas as pd

from utils.features.base_features import get_array_sum


def get_this_season_matches_form_feature(match_results, season):
    print(f"Generating feature : Form for This Season")
    sub_frame = match_results[
        match_results['season'] == season]
    [['game', 'home_team', 'away_team', 'f_home_team_id', 'f_away_team_id', 'result']].copy()

    sub_frame = sub_frame.sort_values(by="game")

    all_teams = list(sub_frame['home_team'].unique())
    all_teams.extend(list(sub_frame['away_team'].unique()))
    all_teams.sort()
    all_unique_teams = set(all_teams)

    this_season_form = {team: [] for team in all_unique_teams}

    sub_frame['f_this_season_home_form'] = 0.0
    sub_frame['f_this_season_away_form'] = 0.0

    for index, row in sub_frame.iterrows():
        row_home = row['home_team']
        row_away = row['away_team']

        sub_frame.loc[index, 'f_this_season_home_form'] = get_array_sum(this_season_form[row_home])
        sub_frame.loc[index, 'f_this_season_away_form'] = get_array_sum(this_season_form[row_away])

        if row['result'] == 0:
            this_season_form[row_home].append(0)
            this_season_form[row_away].append(0)
        elif row['result'] == 1:
            this_season_form[row_home].append(1)
            this_season_form[row_away].append(-1)
        elif row['result'] == -1:
            this_season_form[row_home].append(-1)
            this_season_form[row_away].append(1)

    ret_frame = sub_frame[['game', 'f_this_season_home_form', 'f_this_season_away_form']].copy()
    ret_frame['f_this_season_home_form'] = ret_frame['f_this_season_home_form'].fillna(0.0)
    ret_frame['f_this_season_away_form'] = ret_frame['f_this_season_away_form'].fillna(0.0)

    this_season_form_sums = {
        'team': [],
        'this_season_form': [],
        'this_season_results': []
    }

    for key in this_season_form:
        this_season_form_sums['team'].append(key)
        this_season_form_sums['this_season_form'].append(get_array_sum(this_season_form[key]))
        this_season_form_sums['this_season_results'].append(str(this_season_form[key]))

    this_season_form_frame = pd.DataFrame(this_season_form_sums)

    return ret_frame, this_season_form_frame
