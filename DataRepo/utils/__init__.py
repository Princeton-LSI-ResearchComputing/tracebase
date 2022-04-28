from collections import namedtuple

from DataRepo.models import Researcher
from DataRepo.models.utilities import get_researchers
from DataRepo.utils.accucor_data_loader import AccuCorDataLoader
from DataRepo.utils.compounds_loader import CompoundsLoader
from DataRepo.utils.exceptions import (
    AmbiguousCompoundDefinitionError,
    DryRun,
    HeaderConfigError,
    HeaderError,
    LoadingError,
    MissingSamplesError,
    RequiredValueError,
    ResearcherError,
    ValidationDatabaseSetupError,
)
from DataRepo.utils.queryset_to_pandas_dataframe import (
    QuerysetToPandasDataFrame,
)
from DataRepo.utils.sample_table_loader import SampleTableLoader
from DataRepo.utils.tissues_loader import TissuesLoader

__all__ = [
    "AccuCorDataLoader",
    "CompoundsLoader",
    "HeaderError",
    "HeaderConfigError",
    "RequiredValueError",
    "ResearcherError",
    "MissingSamplesError",
    "AmbiguousCompoundDefinitionError",
    "ValidationDatabaseSetupError",
    "DryRun",
    "LoadingError",
    "QuerysetToPandasDataFrame",
    "SampleTableLoader",
    "TissuesLoader",
]


def leaderboard_data():
    """
    Get list of tuples for leaderboard data
    [(Researcher, count)]
    """

    leaderboards = {
        "studies_leaderboard": [],
        "animals_leaderboard": [],
        "peakgroups_leaderboard": [],
    }
    LeaderboardRow = namedtuple("LeaderboardRow", ["researcher", "score"])
    for name in get_researchers():
        researcher = Researcher(name=name)
        leaderboards["studies_leaderboard"].append(
            LeaderboardRow(researcher, researcher.studies.count())
        )
        leaderboards["animals_leaderboard"].append(
            LeaderboardRow(researcher, researcher.animals.count())
        )
        leaderboards["peakgroups_leaderboard"].append(
            LeaderboardRow(researcher, researcher.peakgroups.count())
        )
    # Sort leaderboards by count
    for leaderboard in leaderboards.values():
        leaderboard.sort(key=lambda x: x.score, reverse=True)

    return leaderboards
