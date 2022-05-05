from DataRepo.views.nav import home
from DataRepo.views.loading.submission import upload
from DataRepo.views.loading.validation import DataValidationView
from DataRepo.views.loading.validation import validation_disabled
from DataRepo.views.search.basic import search_basic
from DataRepo.views.search.advanced.view import AdvancedSearchView
from DataRepo.views.search.results import example_barebones_advanced_search
from DataRepo.views.search.advanced.download import AdvancedSearchTSVView
from DataRepo.views.models import (
    CompoundListView,
    CompoundDetailView,
    StudyListView,
    StudyDetailView,
    study_summary,
    ProtocolListView,
    ProtocolDetailView,
    AnimalListView,
    AnimalDetailView,
    TissueListView,
    TissueDetailView,
    SampleListView,
    SampleDetailView,
    MSRunListView,
    MSRunDetailView,
    PeakGroupSetListView,
    PeakGroupSetDetailView,
    PeakGroupListView,
    PeakGroupDetailView,
    PeakDataListView,
)

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
