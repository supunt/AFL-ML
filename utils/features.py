def get_base_feature_frame(match_results):
    features = match_results[['date', 'game', 'home_team', 'away_team',
                              'venue', 'home_ground_adv', 'away_ground_adv', 'result']].copy()

    features['season'] = features.apply(lambda s: int(s['date'].strftime('%Y')), axis=1)

    return features


def get_form_features(match_results):
    form_btwn_teams = match_results[['game', 'home_team', 'away_team', 'margin']].copy()

    form_btwn_teams['f_form_margin_btwn_teams'] = (match_results.groupby(['home_team', 'away_team'])['margin']
                                                   .transform(lambda row: row.rolling(5).mean().shift())
                                                   .fillna(0))

    form_btwn_teams['f_form_past_5_btwn_teams'] = \
        (match_results.assign(win=lambda df: df.apply(lambda row: 1 if row.margin > 0 else 0, axis='columns'))
                  .groupby(['home_team', 'away_team'])['result']
                  .transform(lambda row: row.rolling(5).mean().shift() * 5)
                  .fillna(0))

    return form_btwn_teams
