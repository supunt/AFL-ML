import pandas as pd
import datetime as dt


def get_base_feature_frame(match_results):
    features = match_results[['date', 'game', 'home_team', 'away_team', 'home_team_id', 'away_team_id', 'ground_id',
                              'venue', 'home_ground_adv', 'away_ground_adv', 'result']].copy()

    features['season'] = features.apply(lambda s: int(s['date'].strftime('%Y')), axis=1)

    return features


def get_cross_team_key(home, away):
    sorted_array = sorted([home, away])
    return f"{sorted_array[0].lower().replace(' ', '_')}::{sorted_array[1].lower().replace(' ', '_')}"


def get_cross_team_ground_key(home, away, ground_id):
    sorted_array = sorted([home, away])
    return f"{sorted_array[0].lower().replace(' ', '_')}::{sorted_array[1].lower().replace(' ', '_')}::{str(int(ground_id))}"


def get_last_x_h2h_feature(match_results, x):
    print(f"Generating feature : Head-to-Head Result for last {x} rolling")
    last_x_h2h = {}
    sub_frame = match_results[['game', 'home_team', 'away_team', 'f_home_team_id', 'f_away_team_id', 'result']].copy()

    sub_frame = sub_frame.sort_values(by="game")
    sub_frame[f'f_last_{x}_h2h'] = 0.0
    sub_frame['comp_key'] = sub_frame.apply(lambda df: get_cross_team_key(df['home_team'], df['away_team']), axis=1)

    last_x_h2h = {x: 0 for x in list(sub_frame['comp_key'].unique())}

    sub_frame_list = []

    for key in last_x_h2h:
        sub_frame_copy = sub_frame[sub_frame['comp_key'] == key].copy()
        key_part_1 = key.split("::")[0]

        # flip score if home and away are flipped
        sub_frame_copy.loc[sub_frame_copy['home_team'].str.lower() != key_part_1, 'result'] = -sub_frame_copy['result']
        sub_frame_copy[f'f_last_{x}_h2h'] = sub_frame_copy['result'].rolling(window=x).sum()
        sub_frame_list.append(sub_frame_copy)

        last_x_h2h[key] = sub_frame_copy.iloc[len(sub_frame_copy) - 1][f'f_last_{x}_h2h']

    concat_frame = pd.DataFrame(columns=list(sub_frame.columns))
    for item in sub_frame_list:
        concat_frame = concat_frame.append(item, ignore_index=True)

    concat_frame = concat_frame.sort_values(by="game")
    concat_frame[f'f_last_{x}_h2h'] = concat_frame[f'f_last_{x}_h2h'].fillna(0.0)

    concat_frame = concat_frame[['game', f'f_last_{x}_h2h']]

    encounter_matrix_fr_object = {
        'comp_key': [],
        f'f_last_{x}_h2h': []
    }

    for k, v in last_x_h2h.items():
        encounter_matrix_fr_object['comp_key'].append(k)
        encounter_matrix_fr_object[f'f_last_{x}_h2h'].append(v)

    last_x_h2h_fr = pd.DataFrame(encounter_matrix_fr_object)

    return concat_frame, last_x_h2h_fr


def __get_season_based_weight__(season, this_year):
    if this_year - season < 5:
        return this_year - season
    else:
        return 0


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


def get_season_weighted_last_x_h2h_feature(match_results, x):
    print(f"Generating feature : Head-to-Head Season weighted result for last {x} rolling")
    last_x_h2h = {}
    sub_frame = match_results[['game', 'home_team', 'away_team',
                               'f_home_team_id', 'f_away_team_id', 'result', 'season']].copy()

    sub_frame = sub_frame.sort_values(by="game")
    sub_frame[f'f_season_weighted_last_{x}_h2h'] = 0.0
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
            lambda df: df['result'] * __get_season_based_weight__(df['season'], this_season), axis=1)

        sub_frame_copy[f'f_season_weighted_last_{x}_h2h'] = sub_frame_copy['result'].rolling(window=x).sum()
        sub_frame_list.append(sub_frame_copy)

        last_x_h2h[key] = sub_frame_copy.iloc[len(sub_frame_copy) - 1][f'f_season_weighted_last_{x}_h2h']

    concat_frame = pd.DataFrame(columns=list(sub_frame.columns))
    for item in sub_frame_list:
        concat_frame = concat_frame.append(item, ignore_index=True)

    concat_frame = concat_frame.sort_values(by="game")
    concat_frame[f'f_season_weighted_last_{x}_h2h'] = concat_frame[
        f'f_season_weighted_last_{x}_h2h'].fillna(0.0)

    concat_frame = concat_frame[['game', f'f_season_weighted_last_{x}_h2h']]

    encounter_matrix_fr_object = {
        'comp_key': [],
        f'f_season_weighted_last_{x}_h2h': []
    }

    for k, v in last_x_h2h.items():
        encounter_matrix_fr_object['comp_key'].append(k)
        encounter_matrix_fr_object[f'f_season_weighted_last_{x}_h2h'].append(v)

    last_x_h2h_fr = pd.DataFrame(encounter_matrix_fr_object)

    return concat_frame, last_x_h2h_fr


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


def __get_array_sum__(arr):
    arr_sum = 0.0
    for item in arr:
        arr_sum += item

    return arr_sum


def get_last_x_matches_form_feature(match_results, x):
    print(f"Generating feature : Form for last {x} rolling")
    sub_frame = match_results[['game', 'home_team', 'away_team', 'f_home_team_id', 'f_away_team_id', 'result']].copy()
    sub_frame = sub_frame.sort_values(by="game")

    all_teams = list(sub_frame['home_team'].unique())
    all_teams.extend(list(sub_frame['away_team'].unique()))
    all_teams.sort()
    all_unique_teams = set(all_teams)

    last_x_match_form = {team: [] for team in all_unique_teams}

    sub_frame[f'f_last_{x}_home_form'] = 0.0
    sub_frame[f'f_last_{x}_away_form'] = 0.0

    for index, row in sub_frame.iterrows():
        row_home = row['home_team']
        row_away = row['away_team']

        if row['result'] == 0:
            last_x_match_form[row_home].append(0)
            last_x_match_form[row_away].append(0)
        elif row['result'] == 1:
            last_x_match_form[row_home].append(1)
            last_x_match_form[row_away].append(-1)
        elif row['result'] == -1:
            last_x_match_form[row_home].append(-1)
            last_x_match_form[row_away].append(1)

        if len(last_x_match_form[row_home]) > x:
            last_x_match_form[row_home] = last_x_match_form[row_home][1:]
            sub_frame.loc[index, f'f_last_{x}_home_form'] = __get_array_sum__(last_x_match_form[row_home])

        if len(last_x_match_form[row_away]) > x:
            last_x_match_form[row_away] = last_x_match_form[row_away][1:]
            sub_frame.loc[index, f'f_last_{x}_away_form'] = __get_array_sum__(last_x_match_form[row_away])

    ret_frame = sub_frame[['game', f'f_last_{x}_home_form', f'f_last_{x}_away_form']].copy()
    ret_frame[f'f_last_{x}_home_form'] = ret_frame[f'f_last_{x}_home_form'].fillna(0.0)
    ret_frame[f'f_last_{x}_away_form'] = ret_frame[f'f_last_{x}_home_form'].fillna(0.0)

    last_x_match_form_sums = {
        'team': [],
        f'last_{x}_form': []
    }

    for key in last_x_match_form:
        last_x_match_form_sums['team'].append(key)
        last_x_match_form_sums[f'last_{x}_form'].append(__get_array_sum__(last_x_match_form[key]))

    last_x_match_form_frame = pd.DataFrame(last_x_match_form_sums)

    return ret_frame, last_x_match_form_frame


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
            sub_frame.loc[index, f'f_last_{x}_home_dominance'] = __get_array_sum__(last_x_match_form[row_home])

        if len(last_x_match_form[row_away]) > x:
            last_x_match_form[row_away] = last_x_match_form[row_away][1:]
            sub_frame.loc[index, f'f_last_{x}_away_dominance'] = __get_array_sum__(last_x_match_form[row_away])

    ret_frame = sub_frame[['game', f'f_last_{x}_home_dominance', f'f_last_{x}_away_dominance']].copy()
    ret_frame[f'f_last_{x}_home_dominance'] = ret_frame[f'f_last_{x}_home_dominance'].fillna(0.0)
    ret_frame[f'f_last_{x}_away_dominance'] = ret_frame[f'f_last_{x}_home_dominance'].fillna(0.0)

    last_x_match_form_sums = {
        'team': [],
        f'last_{x}_dominance': []
    }

    for key in last_x_match_form:
        last_x_match_form_sums['team'].append(key)
        last_x_match_form_sums[f'last_{x}_dominance'].append(__get_array_sum__(last_x_match_form[key]))

    last_x_match_form_frame = pd.DataFrame(last_x_match_form_sums)

    return ret_frame, last_x_match_form_frame
