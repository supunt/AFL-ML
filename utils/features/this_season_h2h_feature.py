import pandas as pd
from utils.features.base_features import get_cross_team_key


def get_this_season_h2h_feature(match_results: pd.DataFrame, season=2021):
    print(f"Generating feature : Head-to-Head Result for this season")
    sub_frame = match_results[['game', 'season', 'home_team', 'away_team', 'f_home_team_id', 'f_away_team_id', 'result']].copy()
    sub_frame = sub_frame[sub_frame['season'] == season].copy()

    sub_frame = sub_frame.sort_values(by="game")
    sub_frame[f'f_this_season_h2h'] = 0.0
    if len(sub_frame) > 0:
        sub_frame['comp_key'] = sub_frame.apply(lambda df: get_cross_team_key(df['home_team'], df['away_team']), axis=1)
    else:
        sub_frame['comp_key'] = ''

    this_season_h2h = {x: 0 for x in list(sub_frame['comp_key'].unique())}

    sub_frame_list = []

    for key in this_season_h2h:
        sub_frame_copy = sub_frame[sub_frame['comp_key'] == key].copy()
        key_part_1 = key.split("::")[0]

        # flip score if home and away are flipped
        sub_frame_copy.loc[sub_frame_copy['home_team'].str.lower() != key_part_1, 'result'] = -sub_frame_copy['result']
        sub_frame_copy['f_this_season_h2h'] = sub_frame_copy['result'].expanding().sum()
        sub_frame_list.append(sub_frame_copy)

        this_season_h2h[key] = sub_frame_copy.iloc[len(sub_frame_copy) - 1]['f_this_season_h2h']

    concat_frame = pd.DataFrame(columns=list(sub_frame.columns))
    for item in sub_frame_list:
        concat_frame = concat_frame.append(item, ignore_index=True)

    concat_frame = concat_frame.sort_values(by="game")
    concat_frame['f_this_season_h2h'] = concat_frame['f_this_season_h2h'].fillna(0.0)

    concat_frame = concat_frame[['game', 'f_this_season_h2h']]

    encounter_matrix_fr_object = {
        'comp_key': [],
        f'f_this_season_h2h': []
    }

    for k, v in this_season_h2h.items():
        encounter_matrix_fr_object['comp_key'].append(k)
        encounter_matrix_fr_object[f'f_this_season_h2h'].append(v)

    this_season_h2h_frame = pd.DataFrame(encounter_matrix_fr_object)

    return concat_frame, this_season_h2h_frame
