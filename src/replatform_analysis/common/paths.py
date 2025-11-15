from pathlib import Path


ROOT_PATH = Path(__file__).resolve().parent.parent
SECRET_PATH = ROOT_PATH.joinpath("secrets")
DATA_FILES_PATH = ROOT_PATH.joinpath("data")
FIXTURES_PATH = ROOT_PATH.joinpath("fixtures")
