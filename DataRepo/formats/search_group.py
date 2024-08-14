from DataRepo.formats.compounds_dataformat import CompoundsFormat
from DataRepo.formats.dataformat_group import FormatGroup
from DataRepo.formats.fluxcirc_dataformat import FluxCircFormat
from DataRepo.formats.peakdata_dataformat import PeakDataFormat
from DataRepo.formats.peakgroups_dataformat import PeakGroupsFormat


class SearchGroup(FormatGroup):
    """
    This class groups all search output formats in a single class and adds metadata that applies to all
    search output formats as a whole.  It includes all derived classes of Format.
    """

    def __init__(self):
        self.addFormats(
            [PeakGroupsFormat(), PeakDataFormat(), FluxCircFormat(), CompoundsFormat()]
        )
