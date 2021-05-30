import pandas as pd
from utils.features.base_features import get_array_sum


def get_last_x_matches_dominance_feature(match_results, x):
    print(f"Generating feature : Dominance for last {x} rolling")
    sub_frame = match_results[['game', 'home_team', 'away_team', 'f_home_team_id',
                               'f_away_team_id', 'result', 'f_margin']].copy()
    sub_frame = sub_frame.sort_values(by="game")

    all_teams = list(sub_frame['home_team'].unique())
    all_teams.extend(list(sub_frame['away_team'].unique()))
    all_teams.sort()
    all_unique_teams = set(all_teams)

    last_x_match_form = {team: [] for team in all_unique_teams}

    sub_frame[f'f_last_{x}_home_dominance'] = 0.0
    sub_frame[f'f_last_{x}_away_dominance'] = 0.0

    for index, row in sub_frame.iterrows():
        row_home = row['home_team']
        row_away = row['away_team']
        abs_row_margin = abs(row['f_margin'])

        if row['result'] == 0:
            last_x_match_form[row_home].append(0)
            last_x_match_form[row_away].append(0)
        elif row['result'] == 1:
            last_x_match_form[row_home].append(abs_row_margin)
            last_x_match_form[row_away].append(-abs_row_margin)
        elif row['result'] == -1:
            last_x_match_form[row_home].append(-abs_row_margin)
            last_x_match_form[row_away].append(abs_row_margin)

        if len(last_x_match_form[row_home]) > x:
            last_x_match_form[row_home] = last_x_match_form[row_home][1:]
            sub_frame.loc[index, f'f_last_{x}_home_dominance'] = get_array_sum(last_x_match_form[row_home])

        if len(last_x_match_form[row_away]) > x:
            last_x_match_form[row_away] = last_x_match_form[row_away][1:]
            sub_frame.loc[index, f'f_last_{x}_away_dominance'] = get_array_sum(last_x_match_form[row_away])

    ret_frame = sub_frame[['game', f'f_last_{x}_home_dominance', f'f_last_{x}_away_dominance']].copy()
    ret_frame[f'f_last_{x}_home_dominance'] = ret_frame[f'f_last_{x}_home_dominance'].fillna(0.0)
    ret_frame[f'f_last_{x}_away_dominance'] = ret_frame[f'f_last_{x}_home_dominance'].fillna(0.0)

    last_x_match_form_sums = {
        'team': [],
        f'last_{x}_dominance': []
    }

    for key in last_x_match_form:
        last_x_match_form_sums['team'].append(key)
        last_x_match_form_sums[f'last_{x}_dominance'].append(get_array_sum(last_x_match_form[key]))

    last_x_match_form_frame = pd.DataFrame(last_x_match_form_sums)

    return ret_frame, last_x_match_form_frame
