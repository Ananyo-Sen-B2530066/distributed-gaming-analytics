import pandas as pd

def load_and_transform(path):
    df = pd.read_csv(path)

    players_data = []

    for _, row in df.iterrows():

        # Team 1
        for i in range(1, 6):
            players_data.append({
                "kills": row.get(f"team1_player{i}_kills", 0),
                "deaths": row.get(f"team1_player{i}_deaths", 0),
                "assists": row.get(f"team1_player{i}_assists", 0),
                "adr": row.get(f"team1_player{i}_adr", 0),
                "kast": row.get(f"team1_player{i}_kast", 0),
                "kddiff": row.get(f"team1_player{i}_kddiff", 0)
            })

        # Team 2
        for i in range(1, 6):
            players_data.append({
                "kills": row.get(f"team2_player{i}_kills", 0),
                "deaths": row.get(f"team2_player{i}_deaths", 0),
                "assists": row.get(f"team2_player{i}_assists", 0),
                "adr": row.get(f"team2_player{i}_adr", 0),
                "kast": row.get(f"team2_player{i}_kast", 0),
                "kddiff": row.get(f"team2_player{i}_kddiff", 0)
            })

    player_df = pd.DataFrame(players_data)
    player_df = player_df.dropna()

    return player_df
