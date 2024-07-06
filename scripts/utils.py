import pandas as pd
from typing import Dict, Any, List


def read_csv(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath)

def write_csv(df: pd.DataFrame, filepath: str) -> None:
    df.to_csv(filepath, index=False)

def read_parquet(filepath: str) -> pd.DataFrame:
    return pd.read_parquet(filepath)

def write_parquet(df: pd.DataFrame, filepath: str) -> None:
    df.to_parquet(filepath, index=False)

def flatten_series_search(json_data: Dict[str, Any]) -> pd.DataFrame:
    series_search_data = json_data['data']
    flat_series_search_df = pd.json_normalize(series_search_data, sep='.')
    flat_series_search_df = flat_series_search_df.add_prefix('series.')

    return flat_series_search_df


def flatten_series_info(json_data: Dict[str, Any]) -> pd.DataFrame:
    series_info_data = json_data['data']['info']
    flat_series_info_df = pd.json_normalize(series_info_data, sep='.')
    flat_series_info_df = flat_series_info_df.add_prefix('series.')

    series_match_list = json_data['data']['matchList']
    flat_match_df = pd.json_normalize(series_match_list, sep='.')
    flat_match_df = flat_match_df.add_prefix('match.')

    # Adding a temporary key for cross join
    flat_series_info_df['_key'] = 1 
    flat_match_df['_key'] = 1

    merged_df = pd.merge(flat_series_info_df, flat_match_df, on='_key').drop('_key', axis=1)

    return merged_df

def flatten_match_info(json_data: Dict[str, Any]) -> pd.DataFrame:
    match_info_data = json_data['data']
    flat_match_info_df = pd.json_normalize(match_info_data, sep='.')
    flat_match_info_df = flat_match_info_df.add_prefix('match.')

    return flat_match_info_df

def flatten_player_info(json_data: Dict[str, Any]) -> pd.DataFrame:
    player_info = json_data['data']
    stats = player_info['stats']
    stats_df = pd.json_normalize(stats, sep='.')
    
    flattened_data = {
        'player.id': player_info['id'],
        'player.name': player_info['name'],
        'player.dateOfBirth': player_info['dateOfBirth'],
        'player.role': player_info['role'],
        'player.battingStyle': player_info['battingStyle'],
        'player.bowlingStyle': player_info['bowlingStyle'],
        'player.placeOfBirth': player_info['placeOfBirth'],
        'player.country': player_info['country'],
        'player.playerImg': player_info['playerImg'],
    }

    for _, row in stats_df.iterrows():
        col_name = f"player.stat.{row['fn']}.{row['matchtype']}.{row['stat']}"
        flattened_data[col_name] = row['value']
    
    flattened_df = pd.DataFrame([flattened_data])

    return flattened_df