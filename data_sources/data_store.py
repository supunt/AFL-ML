import pandas as pd
from data_sources import __loading_cached__
import datetime as dt


__cached_file_path__ = 'H:\\temp\\_cached_race_data.csv'
__next_week_file_path__ = 'H:\\temp\\next-week.xlsx'

team_home_ground_info = None


# ----------------------------------------------------------------------------------------------------------------------
def is_home_for_team(team_name, ground):
    return True if len(team_home_ground_info[
                           (team_home_ground_info['Name_In_Data'].str.lower() == ground.lower()) &
                           (team_home_ground_info['Team'].str.lower() == team_name.lower())]) > 0 else False


# ----------------------------------------------------------------------------------------------------------------------
def get_next_week_frame(max_game_id_in_history):
    next_week_data = pd.read_excel(__next_week_file_path__, sheet_name="Data", header=0)

    next_week_data['home_ground_adv'] = next_week_data.apply(lambda r: is_home_for_team(
        r['Home_Team'], r['Venue']), axis=1)
    next_week_data['away_ground_adv'] = next_week_data.apply(lambda r: is_home_for_team(
        r['Away_Team'], r['Venue']), axis=1)

    for index, row in next_week_data.iterrows():
        next_week_data.loc[index, 'game'] = int(max_game_id_in_history) + int(len(next_week_data) - index)

    next_week_data['Date'] = next_week_data['Date'].apply(lambda x: x.date())

    next_week_data = next_week_data.rename(str.lower, axis='columns')

    return next_week_data


def get_cleaned_data():
    # if __loading_cached__:
    #     print('Loading from last cached file')
    #     past_match_data_min = pd.read_csv(__cached_file_path__, header=0)
    #
    #     return past_match_data_min, get_next_week_frame(past_match_data_min['game'].max())

    # ------------------------------------------------------------------------------------------------------------------
    print("1. Loading Match data (minimized)")
    __past_match_data_min__ = pd.read_excel(".\\data_samples\\afl-reduced_results.xlsx", sheet_name="Data", header=0
                                            , dtype=str)

    # ------------------------------------------------------------------------------------------------------------------
    print("2. Loading Match Ground Name Mappings")
    __afl_ground_names__ = pd.read_excel(".\\data_samples\\afl_ground_names.xlsx", sheet_name="Sheet1", header=0,
                                         dtype=str)
    __afl_ground_names__ = __afl_ground_names__.fillna('')
    __venues_in_data__ = list(__past_match_data_min__['Venue'].str.lower().unique())

    print("\t2.1. Set Name In Data to Ground Names")
    for index, row in __afl_ground_names__.iterrows():
        if row['Ground_Name'].lower() in __venues_in_data__:
            __afl_ground_names__.loc[index, 'Name_In_Data'] = row['Ground_Name']
        else:
            for i in range(1, 4):
                if row[f'Other_Name_{i}'] != '' and row[f'Other_Name_{i}'].lower() in __venues_in_data__:
                    __afl_ground_names__.loc[index, 'Name_In_Data'] = row[f'Other_Name_{i}']
                    break

    afl_ground_names = __afl_ground_names__.copy()

    # ------------------------------------------------------------------------------------------------------------------
    print("3. Loading Home Ground information")
    __team_home_ground_info__ = pd.read_excel(".\\data_samples\\afl-home-grounds.xlsx", sheet_name="Sheet1", header=0,
                                              dtype=str)
    __team_home_ground_info__ = __team_home_ground_info__.fillna('')

    print("\t3.1. Set Name In Data to Home Ground Names")
    for index, row in __team_home_ground_info__.iterrows():
        ground_name = row['Ground_Name']

        if len(__afl_ground_names__[__afl_ground_names__['Ground_Name'] == ground_name]) > 0:
            selection = list(__afl_ground_names__[__afl_ground_names__['Ground_Name'] == ground_name]['Name_In_Data'])
            id_selection = list(__afl_ground_names__[__afl_ground_names__['Ground_Name'] == ground_name]['Ground_Id'])
            __team_home_ground_info__.loc[index, 'Name_In_Data'] = selection[0]
            __team_home_ground_info__.loc[index, 'Ground_Id'] = id_selection[0]

        for i in range(1, 4):
            if len(__afl_ground_names__[__afl_ground_names__[f'Other_Name_{i}'] == ground_name]) > 0:
                selection = list(__afl_ground_names__[
                                     __afl_ground_names__[f'Other_Name_{i}'] == ground_name]['Name_In_Data'])
                id_selection = list(__afl_ground_names__[
                                     __afl_ground_names__[f'Other_Name_{i}'] == ground_name]['Ground_Id'])

                __team_home_ground_info__.loc[index, 'Name_In_Data'] = selection[0]
                __team_home_ground_info__.loc[index, 'Ground_Id'] = id_selection[0]
                break

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
    __past_match_data_min__.loc[
        __past_match_data_min__['home_score'] == __past_match_data_min__['away_score'], 'result'] = 0

    __past_match_data_min__.loc[
        __past_match_data_min__['home_score'] > __past_match_data_min__['away_score'], 'result'] = 1

    __past_match_data_min__.loc[
        __past_match_data_min__['home_score'] < __past_match_data_min__['away_score'], 'result'] = -1

    __past_match_data_min__['margin'] = __past_match_data_min__['home_score'].astype(int) - \
                                        __past_match_data_min__['away_score'].astype(int)

    __past_match_data_min__ = __past_match_data_min__.merge(afl_ground_names[['Name_In_Data', 'Ground_Id']],
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

    __past_match_data_min__['home_team_id'] = 0
    __past_match_data_min__['away_team_id'] = 0

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

    print("7. Set Game Number")
    for index, row in __past_match_data_min__.iterrows():
        __past_match_data_min__.loc[index, 'game'] = int(len(__past_match_data_min__) - index)

    print("8. Reorder Cols")

    cols = list(__past_match_data_min__.columns)
    cols.remove('game')

    new_col_order = ['game']
    new_col_order.extend(cols)
    __past_match_data_min__ = __past_match_data_min__[new_col_order]

    print("\n9. Cleaned Data Stats")
    print("--------------------------------------------------------------------------")
    print(f"Total Matches in Data \t: {len(__past_match_data_min__)}")
    print(f"Total Matches in where Home team had Ground Adv: {len(__past_match_data_min__[__past_match_data_min__['home_ground_adv'] == True])}")
    print(f"Total Matches in where Away team had Ground Adv: {len(__past_match_data_min__[__past_match_data_min__['away_ground_adv'] == True])}")

    past_match_data_min = __past_match_data_min__.copy()

    print(f"10. Cached File written to {__cached_file_path__}")
    past_match_data_min.to_csv(__cached_file_path__)

    next_week_frame = get_next_week_frame(past_match_data_min['game'].max())

    next_week_frame = next_week_frame.merge(afl_ground_names[['Name_In_Data', 'Ground_Id']],
                                                            left_on='venue',
                                                            right_on='Name_In_Data',
                                                            how='inner')

    next_week_frame = next_week_frame.rename(str.lower, axis='columns')

    next_week_frame = next_week_frame.drop(columns=['name_in_data'])

    teams = pd.DataFrame(next_week_frame['home_team']).drop_duplicates().reset_index(drop=True)
    teams = teams.rename(columns={
        'home_team': 'team'
    })

    for index, row in teams.iterrows():
        teams.loc[index, 'team_id'] = index + 1

    next_week_frame['home_team_id'] = 0
    next_week_frame['away_team_id'] = 0

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

    return past_match_data_min, next_week_frame


