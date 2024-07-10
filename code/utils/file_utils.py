import pandas as pd
from typing import Dict, Any, List
import os


def read_csv(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath)

def write_csv(df: pd.DataFrame, filepath: str) -> None:
    # Ensure the directory path exists
    directory = os.path.dirname(filepath)
    if directory:
        os.makedirs(directory, exist_ok=True)
    
    df.to_csv(filepath, index=False)

def read_parquet(filepath: str) -> pd.DataFrame:
    return pd.read_parquet(filepath)

def write_parquet(df: pd.DataFrame, filepath: str) -> None:
    df.to_parquet(filepath, index=False)

def flat_json(json_data: Dict[str, Any], sep='.') -> pd.DataFrame:
    return pd.json_normalize(json_data, sep=sep)

