import pandas as pd

# ----------------------------------------------------------------------------------------------------------------------
print("1. Loading Match data (minimized)")
__past_match_data_min__ = pd.read_excel(".\\data_samples\\afl-reduced_results.xlsx", sheet_name="Data", header=0
                                        , dtype=str)


# ----------------------------------------------------------------------------------------------------------------------
print("2. Loading Match Ground Name Mappings")
__afl_ground_names__ = pd.read_excel(".\\data_samples\\afl_ground_names.xlsx", sheet_name="Sheet1", header=0, dtype=str)
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

# ----------------------------------------------------------------------------------------------------------------------
print("3. Loading Home Ground information")
__team_home_ground_info__ = pd.read_excel(".\\data_samples\\afl-home-grounds.xlsx", sheet_name="Sheet1", header=0,
                                          dtype=str)
__team_home_ground_info__ = __team_home_ground_info__.fillna('')

print("\t3.1. Set Name In Data to Home Ground Names")
for index, row in __team_home_ground_info__.iterrows():
    ground_name = row['Ground_Name']

    if len(__afl_ground_names__[__afl_ground_names__['Ground_Name'] == ground_name]) > 0:
        selection = list(__afl_ground_names__[__afl_ground_names__['Ground_Name'] == ground_name]['Name_In_Data'])
        __team_home_ground_info__.loc[index, 'Name_In_Data'] = selection[0]

    for i in range(1, 4):
        if len(__afl_ground_names__[__afl_ground_names__[f'Other_Name_{i}'] == ground_name]) > 0:
            selection = list(__afl_ground_names__[__afl_ground_names__[f'Other_Name_{i}'] == ground_name]['Name_In_Data'])
            __team_home_ground_info__.loc[index, 'Name_In_Data'] = selection[0]
            break

team_home_ground_info = __team_home_ground_info__.copy()


# ----------------------------------------------------------------------------------------------------------------------
def is_home_for_team(team_name, ground):
    return True if len(team_home_ground_info[
        (team_home_ground_info['Name_In_Data'].str.lower() == ground.lower()) &
        (team_home_ground_info['Team'].str.lower() == team_name.lower())]) > 0 else False


print("4. Tag Home_Ground_Adv and Away_Ground_Adv in match data")
__past_match_data_min__['Home_Ground_Adv'] = __past_match_data_min__.apply(lambda r: is_home_for_team(r['Home_Team'],
                                                                                                      r['Venue']),
                                                                           axis=1)
__past_match_data_min__['Away_Ground_Adv'] = __past_match_data_min__.apply(lambda r: is_home_for_team(r['Away_Team'],
                                                                                                      r['Venue']),
                                                                           axis=1)


print("\n5. Cleaned Data Stats")
print("--------------------------------------------------------------------------")
print(f"Total Matches in Data \t: {len(__past_match_data_min__)}")
print(f"Total Matches in where Home team had Ground Adv: {len(__past_match_data_min__[__past_match_data_min__['Home_Ground_Adv'] == True])}")
print(f"Total Matches in where Away team had Ground Adv: {len(__past_match_data_min__[__past_match_data_min__['Away_Ground_Adv'] == True])}")

__past_match_data_min__ = __past_match_data_min__.rename(str.lower, axis='columns')

past_match_data_min = __past_match_data_min__.copy()
