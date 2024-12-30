import copy
import pandas as pd

from config import TEAM_COMPOSITION

class Projections:
    """Class for storing a set of projections and performing calculations on them."""

    def __init__(
        self,
        player_ids: list[str],
        positions: list[str],
        weeks: list[int],
        pts_ppr: list[float],
    ):
        """Initializes a list of projections. Given lists of data, creates a list of dictionaries
        with keys corresponding to the original lists provided.
        """
        self.projections = [
            {
                "player_id": player_id,
                "position": position,
                "week": week,
                "pts_ppr": pts,
            }
            for player_id, position, week, pts in zip(player_ids, positions, weeks, pts_ppr)
        ]
        
    def get_max_possible_score(
        self,
        player_ids: set[str],
    ) -> float:
        """Gets the maximum possible average weekly score for a set of player ID's, given the
        projections.

        Parameters
        ----------
        player_ids : set[str]
            Players available when calculating the max possible score.
        
        Returns
        -------
        float
            The max possible average weekly score.
        """
        
        projections = copy.deepcopy(self.projections)
        projections = [projection for projection in projections if projection["player_id"] in player_ids]
        position_map = {
            "flex": ["RB", "WR", "TE"],
            "superflex": ["RB", "WR", "TE", "QB"],
        }
        score = 0.0

        for week in set([projection["week"] for projection in projections]):
            
            for position, quantity in TEAM_COMPOSITION["team_composition"].items():

                # Format the position(s) (to account for flex/superflex)
                # If position doesn't need to map to a list, cast it to a list
                formatted_positions = position_map.get(position, [position])

                for _ in range(quantity):
                    possible_projections = [
                        projection for projection in projections
                        if projection["week"] == week
                        and projection["position"] in formatted_positions
                        and not pd.isna(projection["pts_ppr"])
                    ]
                    if len(possible_projections) > 0:
                        selected_projection = max(possible_projections, key=lambda x: x["pts_ppr"])
                        score += selected_projection["pts_ppr"]
                        projections = [
                            projection for projection in projections
                            if projection["player_id"] != selected_projection["player_id"]
                            or projection["week"] != week
                        ]
        
        return score / len(set([projection["week"] for projection in projections]))