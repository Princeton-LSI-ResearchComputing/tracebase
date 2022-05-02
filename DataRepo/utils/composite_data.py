from collections import namedtuple
from DataRepo.models import Researcher
from DataRepo.models.utilities import get_researchers


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
