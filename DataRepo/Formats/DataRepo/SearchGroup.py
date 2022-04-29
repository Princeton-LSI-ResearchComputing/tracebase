from DataRepo.formats.DataRepo.FluxCircFormat import FluxCircFormat
from DataRepo.formats.DataRepo.PeakDataFormat import PeakDataFormat
from DataRepo.formats.DataRepo.PeakGroupsFormat import PeakGroupsFormat
from DataRepo.formats.FormatGroup import FormatGroup


class SearchGroup(FormatGroup):
    """
    This class groups all search output formats in a single class and adds metadata that applies to all
    search output formats as a whole.  It includes all derived classes of Format.
    """

    def __init__(self):
        self.addFormats([PeakGroupsFormat(), PeakDataFormat(), FluxCircFormat()])
