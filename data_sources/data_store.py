import pandas as pd
import datetime as dt
from python_json_config import ConfigBuilder
from typing import List

# create config parser
__builder__ = ConfigBuilder()

# parse config and extract
__config__ = __builder__.parse_config('config.json')
__base_path__ = __config__.stats_data.path
__history_path_format__ = __config__.stats_data.history_path_format

team_home_ground_info = None


# ----------------------------------------------------------------------------------------------------------------------
def is_home_for_team(team_name, ground):
    return True if len(team_home_ground_info[
                           (team_home_ground_info['Name_In_Data'].str.lower() == ground.lower()) &
                           (team_home_ground_info['Team'].str.lower() == team_name.lower())]) > 0 else False


# ----------------------------------------------------------------------------------------------------------------------
def __get_next_week_frame__(max_game_id_in_history, week_id=None):
    path = f'{__base_path__}{__config__.stats_data.next_week_file}'

    if week_id is not None:
        path = f"{__history_path_format__.format(week_id=week_id)}{__config__.stats_data.next_week_file}"

    next_week_data = pd.read_excel(path, sheet_name="Data", header=0)

    next_week_data['home_ground_adv'] = next_week_data.apply(lambda r: is_home_for_team(
        r['Home_Team'], r['Venue']), axis=1)
    next_week_data['away_ground_adv'] = next_week_data.apply(lambda r: is_home_for_team(
        r['Away_Team'], r['Venue']), axis=1)

    next_week_data['Date'] = next_week_data['Date'].apply(lambda x: x.date())

    for index, row in next_week_data.iterrows():
        next_week_data.loc[index, 'game'] = int(max_game_id_in_history) + int(len(next_week_data) - index)

    next_week_data = next_week_data.rename(str.lower, axis='columns')

    return next_week_data


# ----------------------------------------------------------------------------------------------------------------------
def __load_past_match_data__(week_id) -> pd.DataFrame:
    print("1. Loading Match data (minimized)")
    file_path = f"{__base_path__}{__config__.stats_data.history_file}"

    if week_id is not None:
        file_path = f"{__history_path_format__.format(week_id=week_id)}{__config__.stats_data.history_file}"

    __past_match_data_min__ = pd.read_excel(file_path, sheet_name="Data",
                                            header=0
                                            , dtype=str)

    for index, row in __past_match_data_min__.iterrows():
        __past_match_data_min__.loc[index, 'game'] = int(len(__past_match_data_min__) - index)

    return __past_match_data_min__


# ----------------------------------------------------------------------------------------------------------------------
def __get_ground_name_mappings__(venus_in_data: List) -> pd.DataFrame:
    print("2. Loading Match Ground Name Mappings")
    __afl_ground_names__ = pd.read_excel(f"{__base_path__}{__config__.meta_data.ground_names_file}",
                                         sheet_name="Sheet1", header=0,
                                         dtype=str)
    __afl_ground_names__ = __afl_ground_names__.fillna('')
    print("\t2.1. Set Name In Data to Ground Names")
    for index, row in __afl_ground_names__.iterrows():
        if row['Ground_Name'].lower() in venus_in_data:
            __afl_ground_names__.loc[index, 'Name_In_Data'] = row['Ground_Name']
        else:
            for i in range(1, 4):
                if row[f'Other_Name_{i}'] != '' and row[f'Other_Name_{i}'].lower() in venus_in_data:
                    __afl_ground_names__.loc[index, 'Name_In_Data'] = row[f'Other_Name_{i}']
                    break

    return __afl_ground_names__


# ----------------------------------------------------------------------------------------------------------------------
def __get_home_ground_info__(afl_ground_names: pd.DataFrame) -> pd.DataFrame:
    print("3. Loading Home Ground information")
    t_home_ground_info = pd.read_excel(f"{__base_path__}{__config__.meta_data.home_grounds_file}",
                                       sheet_name="Sheet1", header=0,
                                       dtype=str)
    t_home_ground_info = t_home_ground_info.fillna('')

    print("\t3.1. Set Name In Data to Home Ground Names")
    for index, row in t_home_ground_info.iterrows():
        ground_name = row['Ground_Name']

        if len(afl_ground_names[afl_ground_names['Ground_Name'] == ground_name]) > 0:
            selection = list(afl_ground_names[afl_ground_names['Ground_Name'] == ground_name]['Name_In_Data'])
            id_selection = list(afl_ground_names[afl_ground_names['Ground_Name'] == ground_name]['Ground_Id'])
            t_home_ground_info.loc[index, 'Name_In_Data'] = selection[0]
            t_home_ground_info.loc[index, 'Ground_Id'] = id_selection[0]

        for i in range(1, 4):
            if len(afl_ground_names[afl_ground_names[f'Other_Name_{i}'] == ground_name]) > 0:
                selection = list(afl_ground_names[
                                     afl_ground_names[f'Other_Name_{i}'] == ground_name]['Name_In_Data'])
                id_selection = list(afl_ground_names[
                                        afl_ground_names[f'Other_Name_{i}'] == ground_name]['Ground_Id'])

                t_home_ground_info.loc[index, 'Name_In_Data'] = selection[0]
                t_home_ground_info.loc[index, 'Ground_Id'] = id_selection[0]
                break

    return t_home_ground_info


# ----------------------------------------------------------------------------------------------------------------------
def get_cleaned_data(week_id=None):
    # Load Historical Data
    __past_match_data_min__ = __load_past_match_data__(week_id)

    # Load Ground Name Mappings
    afl_ground_names = __get_ground_name_mappings__(
        venus_in_data=list(__past_match_data_min__['Venue'].str.lower().unique()))

    # Load home ground information
    __team_home_ground_info__ = __get_home_ground_info__(afl_ground_names)
    global team_home_ground_info
    team_home_ground_info = __team_home_ground_info__.copy()

    print("4. Tag Home_Ground_Adv and Away_Ground_Adv in match data")
    __past_match_data_min__['home_ground_adv'] = __past_match_data_min__.apply(lambda r: is_home_for_team(
        r['Home_Team'], r['Venue']), axis=1)
    __past_match_data_min__['away_ground_adv'] = __past_match_data_min__.apply(lambda r: is_home_for_team(
        r['Away_Team'], r['Venue']), axis=1)

    print("5. Column Lower Case")
    __past_match_data_min__ = __past_match_data_min__.rename(str.lower, axis='columns')

    print("6. Set Match Result")
    __past_match_data_min__['margin'] = __past_match_data_min__['home_score'].astype(int) - \
                                        __past_match_data_min__['away_score'].astype(int)

    __past_match_data_min__.loc[__past_match_data_min__['margin'] == 0, 'result'] = 0
    __past_match_data_min__.loc[__past_match_data_min__['margin'] > 0, 'result'] = 1
    __past_match_data_min__.loc[__past_match_data_min__['margin'] < 0, 'result'] = -1

    __past_match_data_min__ = __past_match_data_min__.merge(
        afl_ground_names[['Name_In_Data', 'Ground_Id']].copy().drop_duplicates(subset=['Name_In_Data']),
        left_on='venue',
        right_on='Name_In_Data',
        how='inner')

    __past_match_data_min__ = __past_match_data_min__.rename(str.lower, axis='columns')

    __past_match_data_min__ = __past_match_data_min__.drop(columns=['name_in_data'])

    teams = pd.DataFrame(__past_match_data_min__['home_team']).drop_duplicates().reset_index(drop=True)
    teams = teams.rename(columns={
        'home_team': 'team'
    })

    for index, row in teams.iterrows():
        teams.loc[index, 'team_id'] = index + 1

    __past_match_data_min__ = __past_match_data_min__.merge(teams,
                                                            how="inner",
                                                            left_on='home_team',
                                                            right_on='team')

    __past_match_data_min__ = __past_match_data_min__.rename(columns={
        'team_id': 'home_team_id'
    })

    __past_match_data_min__ = __past_match_data_min__.drop(columns=['team'])

    __past_match_data_min__ = __past_match_data_min__.merge(teams,
                                                            how="inner",
                                                            left_on='away_team',
                                                            right_on='team')

    __past_match_data_min__ = __past_match_data_min__.rename(columns={
        'team_id': 'away_team_id'
    })

    __past_match_data_min__ = __past_match_data_min__.drop(columns=['team'])

    print("7. Sort by date and Set Game Number")
    __past_match_data_min__['date'] = __past_match_data_min__['date'].apply(
        lambda x: dt.datetime.strptime(x[0:10], '%Y-%m-%d').date())

    print("8. Reorder Cols")

    cols = list(__past_match_data_min__.columns)
    cols.remove('game')

    new_col_order = ['game']
    new_col_order.extend(cols)
    __past_match_data_min__ = __past_match_data_min__[new_col_order]

    __past_match_data_min__['season'] = __past_match_data_min__.apply(lambda s: int(s['date'].strftime('%Y')), axis=1)

    print("\n9. Cleaned Data Stats")
    print("--------------------------------------------------------------------------")
    print(f"Total Matches in Data \t: {len(__past_match_data_min__)}")
    print(f"Total Matches in where Home team had Ground Adv: {len(__past_match_data_min__[__past_match_data_min__['home_ground_adv'] == True])}")
    print(f"Total Matches in where Away team had Ground Adv: {len(__past_match_data_min__[__past_match_data_min__['away_ground_adv'] == True])}")

    past_match_data_min = __past_match_data_min__.copy()

    next_week_frame = __get_next_week_frame__(past_match_data_min['game'].max(), week_id)

    next_week_frame = next_week_frame.merge(
        afl_ground_names[['Name_In_Data', 'Ground_Id']].copy().drop_duplicates(subset=['Name_In_Data']),
        left_on='venue',
        right_on='Name_In_Data',
        how='inner')

    next_week_frame = next_week_frame.rename(str.lower, axis='columns')

    next_week_frame = next_week_frame.drop(columns=['name_in_data'])

    teams = pd.DataFrame(past_match_data_min['home_team']).drop_duplicates().reset_index(drop=True)
    teams = teams.rename(columns={
        'home_team': 'team'
    })

    for index, row in teams.iterrows():
        teams.loc[index, 'team_id'] = index + 1

    next_week_frame = next_week_frame.merge(teams,
                                            how="inner",
                                            left_on='home_team',
                                            right_on='team')

    next_week_frame = next_week_frame.rename(columns={
        'team_id': 'home_team_id'
    })

    next_week_frame = next_week_frame.drop(columns=['team'])

    next_week_frame = next_week_frame.merge(teams,
                                            how="inner",
                                            left_on='away_team',
                                            right_on='team')

    next_week_frame = next_week_frame.rename(columns={
        'team_id': 'away_team_id'
    })

    next_week_frame = next_week_frame.drop(columns=['team'])

    next_week_frame['season'] = next_week_frame.apply(lambda s: int(s['date'].strftime('%Y')), axis=1)

    past_match_data_min = past_match_data_min.sort_values(by=['game'], ascending=False)
    next_week_frame = next_week_frame.sort_values(by=['game'], ascending=False)

    past_match_data_min['home_ground_adv_tf'] = past_match_data_min['home_ground_adv']
    past_match_data_min['away_ground_adv_tf'] = past_match_data_min['away_ground_adv']

    next_week_frame['home_ground_adv_tf'] = next_week_frame['home_ground_adv']
    next_week_frame['away_ground_adv_tf'] = next_week_frame['away_ground_adv']

    renames_mapping = {
        'margin': 'f_margin',
        'away_team_id': 'f_away_team_id',
        'home_team_id': 'f_home_team_id',
        'home_ground_adv': 'f_home_ground_adv',
        'away_ground_adv': 'f_away_ground_adv',
        'ground_id': 'f_ground_id',
        'home_odds': 'f_home_odds',
        'away_odds': 'f_away_odds'
    }
    past_match_data_min = past_match_data_min.rename(columns=renames_mapping)
    next_week_frame = next_week_frame.rename(columns=renames_mapping)

    past_match_data_min['f_home_ground_adv'] = past_match_data_min['f_home_ground_adv'].apply(
        lambda x: 1.0 if x else 0.0)
    past_match_data_min['f_away_ground_adv'] = past_match_data_min['f_away_ground_adv'].apply(
        lambda x: 1.0 if x else 0.0)

    next_week_frame['f_away_ground_adv'] = next_week_frame['f_away_ground_adv'].apply(
        lambda x: 1.0 if x else 0.0)

    next_week_frame['f_away_ground_adv'] = next_week_frame['f_away_ground_adv'].apply(
        lambda x: 1.0 if x else 0.0)

    return past_match_data_min, next_week_frame


