from DataRepo.views.loading.submission import upload
from DataRepo.views.loading.validation import (
    DataValidationView,
    validation_disabled,
)
from DataRepo.views.models import (
    AnimalDetailView,
    AnimalListView,
    CompoundDetailView,
    CompoundListView,
    MSRunDetailView,
    MSRunListView,
    PeakDataListView,
    PeakGroupDetailView,
    PeakGroupListView,
    PeakGroupSetDetailView,
    PeakGroupSetListView,
    ProtocolDetailView,
    ProtocolListView,
    SampleDetailView,
    SampleListView,
    StudyDetailView,
    StudyListView,
    TissueDetailView,
    TissueListView,
    study_summary,
)
from DataRepo.views.nav import home
from DataRepo.views.search.advanced.download import AdvancedSearchTSVView
from DataRepo.views.search.advanced.view import AdvancedSearchView
from DataRepo.views.search.basic import search_basic
from DataRepo.views.search.results import example_barebones_advanced_search

__all__ = [
    "home",
    "upload",
    "DataValidationView",
    "validation_disabled",
    "search_basic",
    "example_barebones_advanced_search",
    "AdvancedSearchView",
    "AdvancedSearchTSVView",
    "CompoundListView",
    "CompoundDetailView",
    "StudyListView",
    "StudyDetailView",
    "study_summary",
    "ProtocolListView",
    "ProtocolDetailView",
    "AnimalListView",
    "AnimalDetailView",
    "TissueListView",
    "TissueDetailView",
    "SampleListView",
    "SampleDetailView",
    "MSRunListView",
    "MSRunDetailView",
    "PeakGroupSetListView",
    "PeakGroupSetDetailView",
    "PeakGroupListView",
    "PeakGroupDetailView",
    "PeakDataListView",
]
