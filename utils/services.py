import itertools
import pandas as pd

from data.data_loading import run_data_pipeline
from loguru import logger
from utils.mapping import Mapping
from utils.scoring import Projections


def get_recommended_trades(
    league_id: str,
    year: int,
    user_id: str,
    max_group_size: int,
    positions: list[str] | None = None,
    other_users: str | list[str] | None = None,
) -> pd.DataFrame:
    """Main service function for getting recommended trades. Given a league and other parameters,
    produces a report (returned as a data frame) of all trades that are favorable for the user and
    at least neutral for the trading party.

    Parameters
    ----------
    league_id : str
        The Sleeper ID for the league in which the trade is taking place.
    year : int
        The year / season to consider.
    user_id : str
        The Sleeper user ID for the user to calculate trades for.
    max_group_size : int
        The maximum number of players to be considered for a trade on each side (e.g. 2 means a
        2-for-2 swap is possible).
    positions : list[str] | None = None
        List of positions to be considered for trades. If None, all positions are considered.
    other_users : str | list[str] | None = None
        Other user(s) to be considered for trading with. If None, all other users in the league are
        considered.
    
    Returns
    -------
    pd.DataFrame
        Data frame with columns
            - "With": The user ID of the player to trade with.
            - "Gives": List of player ID's to give in the trade.
            - "Receives": List of player ID's to receive in the trade.
            - "User Projection": User's projected average weekly score after the trade.
            - "Other Projection": Trade partner's projected average weekly score after the trade.
    """
    
    projections_data = run_data_pipeline(league_id=league_id, year=year)
    
    # Filter to relevant positions
    if positions is not None:
        projections_data = projections_data[projections_data["position"].isin(positions)]
    
    # Filter to relevant user(s)
    if other_users is not None:
        if isinstance(other_users, str):
            other_users = [other_users]
        projections_data = projections_data[
            (projections_data["user_id"] == user_id)
            | (projections_data["user_id"].isin(other_users))
        ]
    
    # Execute trades
    recommended_trades = _get_recommended_trades(
        player_projections=projections_data,
        max_group_size=max_group_size,
        user_id=user_id,
    )
    recommended_trades = pd.DataFrame(recommended_trades)
    
    # Format trade data frame:
    # Convert tuples of players to strings with names and positions
    mapping = Mapping(league_id=league_id)
    recommended_trades["Gives"] = [
        ", ".join([
            f"{mapping.player_id_to_player_name.get(player_id)} "
            f"({mapping.player_id_to_player_position.get(player_id)})"
            for player_id in player_ids
        ])
        for player_ids in recommended_trades["Gives"]
    ]
    recommended_trades["Receives"] = [
        ", ".join([
            f"{mapping.player_id_to_player_name.get(player_id)} "
            f"({mapping.player_id_to_player_position.get(player_id)})"
            for player_id in player_ids
        ])
        for player_ids in recommended_trades["Receives"]
    ]
    recommended_trades["With"] = recommended_trades["With"].map(mapping.user_id_to_display_name)

    return recommended_trades.sort_values("User Projection", ascending=False)

def _get_recommended_trades(
    player_projections: pd.DataFrame,
    max_group_size: int,
    user_id: str,
) -> list[dict]:
    """Helper function for getting recommended trades. This helper function isolates the logic of
    getting the trades; other pre- and post-processing is included in the service function.

    Parameters
    ----------
    player_projection : pd.DataFrame
        Data frame storing projections for all players in the league for all weeks. Has columns
        "player_id", "position", "week", and "pts_ppr".
    max_group_size : int
        The maximum number of players to be considered for a trade on each side (e.g. 2 means a
        2-for-2 swap is possible).
    user_id : str
        The Sleeper user ID for the user to calculate trades for.
    
    Returns
    -------
    list[dict]
        List of dictionaries with keys "With", "Gives", "Receives", "User Projection", and
        "Other Projection".
    """
        
    # Create projections object with all projections, for use across all team types
    all_projections = Projections(
        player_ids=player_projections["player_id"],
        positions=player_projections["position"],
        weeks=player_projections["week"],
        pts_ppr=player_projections["pts_ppr"],
    )

    # Create set of user players, and the user's base score
    user_players = set(player_projections[player_projections["user_id"] == user_id]["player_id"])
    user_base_score = all_projections.get_max_possible_score(player_ids=user_players)

    # Create other players: maps user ID to set of player_id
    other_players = player_projections[player_projections["user_id"] != user_id].groupby("user_id").agg({"player_id": set}).reset_index()
    other_players = {row["user_id"]: row["player_id"] for _, row in other_players.iterrows()}

    accepted_trades = []

    # For user, get all groups of players of up to specified size
    for user_player_group in [combo for group_size in range(1, max_group_size + 1) for combo in itertools.combinations(user_players, group_size)]:

        # Loop through other players
        for other_user, other_player_ids in other_players.items():

            # Get base score for other player's roster
            other_base_score = all_projections.get_max_possible_score(player_ids=other_player_ids)

            # For other user, get all groups of players of up to specified size
            for other_player_group in [combo for group_size in range(1, max_group_size + 1) for combo in itertools.combinations(other_player_ids, group_size)]:

                logger.info(f"Testing trade: user {user_player_group} other {other_player_group}")

                # Create the modified rosters for this trade
                proposed_user_players = (user_players - set(user_player_group)) | set(other_player_group)
                proposed_other_players = (other_player_ids - set(other_player_group)) | set(user_player_group)

                # Calculate the max scores with the trade
                proposed_user_score = all_projections.get_max_possible_score(player_ids=proposed_user_players)
                proposed_other_score = all_projections.get_max_possible_score(player_ids=proposed_other_players)

                if proposed_user_score > user_base_score and proposed_other_score >= other_base_score:
                    accepted_trades.append({
                        "With": other_user,
                        "Gives": user_player_group,
                        "Receives": other_player_group,
                        "User Projection": proposed_user_score,
                        "Other Projection": proposed_other_score,
                    })

    return accepted_trades
