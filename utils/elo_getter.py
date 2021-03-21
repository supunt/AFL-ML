import pandas as pd


def elo_applier(df, k_factor):
    # Initialise a dictionary with default elos for each team
    elo_dict = {team: 1500 for team in df['home_team'].unique()}
    elos, elo_probs = {}, {}

    df = df.rename(columns={'result': 'f_result'})
    # Loop over the rows in the DataFrame
    for index, row in df.iterrows():
        # Get the Game ID
        game_id = row['game']

        # Get the margin
        result = row['f_result']

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
        if result == 1: # Home team wins; update both teams' elo
            new_home_team_elo = home_team_elo + k_factor*(1 - prob_win_home)
            new_away_team_elo = away_team_elo + k_factor*(0 - prob_win_away)
        elif result == -1: # Away team wins; update both teams' elo
            new_home_team_elo = home_team_elo + k_factor*(0 - prob_win_home)
            new_away_team_elo = away_team_elo + k_factor*(1 - prob_win_away)
        elif result == 0: # Drawn game' update both teams' elo
            new_home_team_elo = home_team_elo + k_factor*(0.5 - prob_win_home)
            new_away_team_elo = away_team_elo + k_factor*(0.5 - prob_win_away)

        # Update elos in elo dictionary
        elo_dict[home_team] = new_home_team_elo
        elo_dict[away_team] = new_away_team_elo

    return elos, elo_probs, elo_dict