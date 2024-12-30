import hashlib
import inspect
import json
import os
import pandas as pd
import subprocess

from datetime import datetime, timedelta
from loguru import logger

max_recursion_depth = 100
cache_dir = "data/.cache/"
os.makedirs(cache_dir, exist_ok=True)


def cacheit(func):
    def wrapper(*args, **kwargs):
        # Create a unique hash based on function name and arguments
        func_name = func.__name__
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        func_args_str = f"{func_name}:{func_args}"
        func_hash = hashlib.sha256(func_args_str.encode("utf-8")).hexdigest()
        
        # Path for the specific hash cache
        os.makedirs(os.path.join(cache_dir, func_hash), exist_ok=True)
        cache_path = os.path.join(cache_dir, func_hash, "data.parquet")
        metadata_path = os.path.join(cache_dir, func_hash, "metadata.txt")

        # Check if the cache exists and is fresh
        if os.path.exists(metadata_path):
            with open(metadata_path, "r") as f:
                metadata = f.read().strip()
                timestamp = datetime.fromisoformat(metadata)
            
            # Check if the cached data is within 24 hours
            if datetime.now() - timestamp <= timedelta(hours=24):
                # Return cached data
                logger.info(f"Reading cached data for {func_name}")
                return pd.read_parquet(cache_path)
            
        logger.info(f"Executing and writing to cache: {func_name}")

        # If no valid cached version, execute the function
        data = func(*args, **kwargs)

        # Save the data and update the metadata
        data.to_parquet(cache_path)

        with open(metadata_path, "w") as f:
            f.write(datetime.now().isoformat())

        return data
    
    return wrapper


@cacheit
def load_roster_data(league_id) -> pd.DataFrame:
    data_json_str = os.popen(f'curl "https://api.sleeper.app/v1/league/{league_id}/rosters"').read()
    roster_data = pd.DataFrame(json.loads(data_json_str))
    roster_data = roster_data[["league_id", "owner_id", "players"]]
    roster_data = roster_data.explode("players").rename(columns={"owner_id": "user_id", "players": "player_id"})
    return roster_data

@cacheit
def load_user_data(league_id) -> pd.DataFrame:
    data_json_str = os.popen(f'curl "https://api.sleeper.app/v1/league/{league_id}/users"').read()
    user_data = pd.DataFrame(json.loads(data_json_str))
    user_data = user_data[["league_id", "user_id", "display_name"]]
    return user_data

@cacheit
def load_player_projections(year: int) -> pd.DataFrame:
    all_projections = []
    for week in range(1, 19):
        all_projections.extend(_load_player_projections_one_week(year=year, week=week))

    player_projections = pd.DataFrame(all_projections)
    player_projections = player_projections[["stats", "week", "season", "player_id"]]
    return player_projections

def _load_player_projections_one_week(year: int, week: int, recursive_depth: int = 0) -> list[dict]:

    if recursive_depth > max_recursion_depth:
        raise ValueError("Recursion depth exceeded.")

    try:
        output = subprocess.run(
            f"curl https://sleeper.com/projections/nfl/{year}/{week}?season_type=regular",
            shell=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        return json.loads(output)
    except Exception:
        return _load_player_projections_one_week(year=year, week=week, recursive_depth=recursive_depth + 1)

@cacheit
def load_player_data() -> pd.DataFrame:
    data_json_str = os.popen('curl "https://api.sleeper.app/v1/players/nfl"').read()
    data_json_dict = json.loads(data_json_str)
    data_json_dict = [
        {
            "player_id": player_id,
            "player_name": info.get("full_name", None),
            "position": info.get("fantasy_positions", None),
        }
        for player_id, info in data_json_dict.items()
    ]
    data = pd.DataFrame(data_json_dict)
    data = data.explode("position")
    return data

def merge_projections_data(
    roster_data: pd.DataFrame,
    user_data: pd.DataFrame,
    player_data: pd.DataFrame,
    player_projections: pd.DataFrame,
) -> pd.DataFrame:
    
    # Start with roster data
    data = roster_data

    # Join in user data
    data = data.merge(user_data, how="left", on=["league_id", "user_id"], validate="m:1")
    
    # Add in player information
    # Join in player information
    data = data.merge(player_data, how="left", on=["player_id"], validate="m:m")

    # Join in player projections
    data = data.merge(player_projections, how="left", on=["player_id"], validate="m:m")
    data = data.rename(columns={"season": "year"})
    data = pd.concat([data, pd.json_normalize(data["stats"])], axis=1)
    data = data.drop(columns=["stats"])
    
    return data


def run_data_pipeline(league_id: str, year: int):

    # Create component datasets
    roster_data = load_roster_data(league_id=league_id)
    user_data = load_user_data(league_id=league_id)
    player_data = load_player_data()
    player_projections = load_player_projections(year=year)

    data = merge_projections_data(
        roster_data=roster_data,
        user_data=user_data,
        player_data=player_data,
        player_projections=player_projections,
    )
    
    return data