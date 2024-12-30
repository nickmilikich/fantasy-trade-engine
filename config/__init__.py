import os
import yaml

_config_dir = os.path.dirname(__file__)
TEAM_COMPOSITION = yaml.safe_load(open(f"{_config_dir}/team_composition.yml", "rb"))