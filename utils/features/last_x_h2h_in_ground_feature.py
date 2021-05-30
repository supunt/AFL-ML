import pandas as pd
from utils.features.base_features import get_cross_team_ground_key


def get_last_x_h2h_in_ground_feature(match_results, x):
    print(f"Generating feature : Head-to-Head on a specific Ground for last {x} rolling")
    last_x_h2h_in_ground = {}
    sub_frame = match_results[['game', 'home_team', 'away_team', 'f_ground_id', 'result']].copy()

    sub_frame = sub_frame.sort_values(by="game")
    sub_frame[f'f_last_{x}_h2h_in_ground'] = 0.0
    sub_frame['comp_key'] = sub_frame.apply(lambda df: get_cross_team_ground_key(df['home_team'], df['away_team'],
                                                                                 df['f_ground_id']), axis=1)

    last_x_h2h_in_ground = {x: 0 for x in list(sub_frame['comp_key'].unique())}

    sub_frame_list = []

    for key in last_x_h2h_in_ground:
        sub_frame_copy = sub_frame[sub_frame['comp_key'] == key].copy()

        key_part_1 = key.split("::")[0]

        # flip score if home and away are flipped
        sub_frame_copy.loc[sub_frame_copy['home_team'].str.lower() != key_part_1, 'result'] = -sub_frame_copy['result']

        sub_frame_copy[f'f_last_{x}_h2h_in_ground'] = sub_frame_copy.iloc[:, 4].rolling(window=x).sum()
        sub_frame_list.append(sub_frame_copy)

        last_x_h2h_in_ground[key] = sub_frame_copy.iloc[len(sub_frame_copy) - 1][
            f'f_last_{x}_h2h_in_ground']

    concat_frame = pd.DataFrame(columns=list(sub_frame.columns))
    for item in sub_frame_list:
        concat_frame = concat_frame.append(item, ignore_index=True)

    concat_frame = concat_frame.sort_values(by="game")
    concat_frame[f'f_last_{x}_h2h_in_ground'] = concat_frame[f'f_last_{x}_h2h_in_ground'].fillna(0.0)

    concat_frame = concat_frame[['game', f'f_last_{x}_h2h_in_ground']]

    encounter_matrix_fr_object = {
        'comp_key': [],
        f'f_last_{x}_h2h_in_ground': []
    }

    for k, v in last_x_h2h_in_ground.items():
        encounter_matrix_fr_object['comp_key'].append(k)
        encounter_matrix_fr_object[f'f_last_{x}_h2h_in_ground'].append(v)

    last_x_h2h_gr_fr = pd.DataFrame(encounter_matrix_fr_object)

    return concat_frame, last_x_h2h_gr_fr