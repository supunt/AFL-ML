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


def get_array_sum(arr):
    arr_sum = 0.0
    for item in arr:
        arr_sum += item

    return arr_sum
