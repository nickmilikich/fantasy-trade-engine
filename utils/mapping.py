import hashlib
from data.data_loading import load_player_data, load_user_data
from functools import partial


class Mapping:
    _mappings = {}

    def __init__(self, league_id: str):
        self.league_id = league_id

    def _retrieve_mapping(
        self,
        partial_fxn: partial,
        key_column: str,
        value_column: str,
    ) -> dict:
        # Create a unique hash based on function name and arguments
        func_name = partial_fxn.func.__name__
        args = partial_fxn.args
        kwargs = tuple(sorted(partial_fxn.keywords.items())) if partial_fxn.keywords else ()
        func_hash = hashlib.sha256(
            str((func_name, args, kwargs, key_column, value_column)).encode("utf-8")
        ).hexdigest()

        if func_hash not in self._mappings.keys():
            data = partial_fxn()
            self._mappings[func_hash] = data.set_index(key_column)[value_column].to_dict()

        return self._mappings[func_hash]

    @property
    def user_id_to_display_name(self) -> dict:
        return self._retrieve_mapping(
            partial_fxn=partial(load_user_data, league_id=self.league_id),
            key_column="user_id",
            value_column="display_name",
        )

    @property
    def display_name_to_user_id(self) -> dict:
        return self._retrieve_mapping(
            partial_fxn=partial(load_user_data, league_id=self.league_id),
            key_column="display_name",
            value_column="user_id",
        )

    @property
    def player_id_to_player_name(self) -> dict:
        return self._retrieve_mapping(
            partial_fxn=partial(load_player_data),
            key_column="player_id",
            value_column="player_name",
        )

    @property
    def player_id_to_player_position(self) -> dict:
        return self._retrieve_mapping(
            partial_fxn=partial(load_player_data),
            key_column="player_id",
            value_column="position",
        )

    @property
    def league_id_to_display_names(self) -> dict:
        """This behaves differently because it is not a 1:1 mapping."""
        data = load_user_data(league_id=self.league_id)
        return {self.league_id: data["display_name"].tolist()}
