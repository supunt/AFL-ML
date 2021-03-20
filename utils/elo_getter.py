import pandas as pd


def elo_applier(df, k_factor):
    # Initialise a dictionary with default elos for each team
    elo_dict = {team: 1500 for team in df['home_team'].unique()}
    elos, elo_probs = {}, {}

    # # Get a home and away dataframe so that we can get the teams on the same row
    # home_df = df.loc[df.home_game == 1, ['home_team', 'game', 'f_margin', 'f_home_ground_adv']].rename()
    # away_df = df.loc[df.home_game == 0, ['home_team', 'game']].rename(columns={'team': 'away_team'})
    #
    # df = (pd.merge(home_df, away_df, on='game')
    #         .sort_values(by='game')
    #         .drop_duplicates(subset='game', keep='first')
    #         .reset_index(drop=True))

    df = df.rename(columns={'margin': 'f_margin'})
    # Loop over the rows in the DataFrame
    for index, row in df.iterrows():
        # Get the Game ID
        game_id = row['game']

        # Get the margin
        margin = row['f_margin']

        # If the game already has the elos for the home and away team in the elos dictionary, go to the next game
        if game_id in elos.keys():
            continue

        # Get the team and opposition
        home_team = row['home_team']
        away_team = row['away_team']

        # Get the team and opposition elo score
        home_team_elo = elo_dict[home_team]
        away_team_elo = elo_dict[away_team]

        # Calculated the probability of winning for the team and opposition
        prob_win_home = 1 / (1 + 10**((away_team_elo - home_team_elo) / 400))
        prob_win_away = 1 - prob_win_home

        # Add the elos and probabilities our elos dictionary and elo_probs dictionary based on the Game ID
        elos[game_id] = [home_team_elo, away_team_elo]
        elo_probs[game_id] = [prob_win_home, prob_win_away]

        # Calculate the new elos of each team
        if margin > 0: # Home team wins; update both teams' elo
            new_home_team_elo = home_team_elo + k_factor*(1 - prob_win_home)
            new_away_team_elo = away_team_elo + k_factor*(0 - prob_win_away)
        elif margin < 0: # Away team wins; update both teams' elo
            new_home_team_elo = home_team_elo + k_factor*(0 - prob_win_home)
            new_away_team_elo = away_team_elo + k_factor*(1 - prob_win_away)
        elif margin == 0: # Drawn game' update both teams' elo
            new_home_team_elo = home_team_elo + k_factor*(0.5 - prob_win_home)
            new_away_team_elo = away_team_elo + k_factor*(0.5 - prob_win_away)

        # Update elos in elo dictionary
        elo_dict[home_team] = new_home_team_elo
        elo_dict[away_team] = new_away_team_elo

    return elos, elo_probs, elo_dict