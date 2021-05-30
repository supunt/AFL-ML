import datetime as dt
import pandas as pd
from utils.features.base_features import get_cross_team_key


def get_margin_weighted_last_x_h2h_feature(match_results, x):
    print(f"Generating feature : Head-to-Head Margin weighted result for last {x} rolling (Dominance)")
    last_x_h2h = {}
    sub_frame = match_results[['game', 'home_team', 'away_team',
                               'f_home_team_id', 'f_away_team_id', 'result', 'season', 'f_margin']].copy()

    sub_frame = sub_frame.sort_values(by="game")
    sub_frame[f'f_margin_weighted_last_{x}_h2h'] = 0.0
    sub_frame['comp_key'] = sub_frame.apply(lambda df: get_cross_team_key(df['home_team'], df['away_team']), axis=1)

    last_x_h2h = {x: 0 for x in list(sub_frame['comp_key'].unique())}

    sub_frame_list = []

    this_season = dt.datetime.now().year

    for key in last_x_h2h:
        sub_frame_copy = sub_frame[sub_frame['comp_key'] == key].copy()
        key_part_1 = key.split("::")[0]

        # flip score if home and away are flipped
        sub_frame_copy.loc[sub_frame_copy['home_team'].str.lower() != key_part_1, 'result'] = -sub_frame_copy['result']
        sub_frame_copy['result'] = sub_frame_copy.apply(
            lambda df: df['result'] * abs(df['f_margin']), axis=1)

        sub_frame_copy[f'f_margin_weighted_last_{x}_h2h'] = sub_frame_copy['result'].rolling(window=x).sum()
        sub_frame_list.append(sub_frame_copy)

        last_x_h2h[key] = sub_frame_copy.iloc[len(sub_frame_copy) - 1][f'f_margin_weighted_last_{x}_h2h']

    concat_frame = pd.DataFrame(columns=list(sub_frame.columns))
    for item in sub_frame_list:
        concat_frame = concat_frame.append(item, ignore_index=True)

    concat_frame = concat_frame.sort_values(by="game")
    concat_frame[f'f_margin_weighted_last_{x}_h2h'] = concat_frame[
        f'f_margin_weighted_last_{x}_h2h'].fillna(0.0)

    concat_frame = concat_frame[['game', f'f_margin_weighted_last_{x}_h2h']]

    encounter_matrix_fr_object = {
        'comp_key': [],
        f'f_margin_weighted_last_{x}_h2h': []
    }

    for k, v in last_x_h2h.items():
        encounter_matrix_fr_object['comp_key'].append(k)
        encounter_matrix_fr_object[f'f_margin_weighted_last_{x}_h2h'].append(v)

    last_x_h2h_fr = pd.DataFrame(encounter_matrix_fr_object)

    return concat_frame, last_x_h2h_fr
