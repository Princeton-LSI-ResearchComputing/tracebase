from DataRepo.formats.DataFormatGroup import FormatGroup
from DataRepo.formats.FluxCircFormat import FluxCircFormat
from DataRepo.formats.PeakDataFormat import PeakDataFormat
from DataRepo.formats.PeakGroupsFormat import PeakGroupsFormat


class SearchGroup(FormatGroup):
    """
    This class groups all search output formats in a single class and adds metadata that applies to all
    search output formats as a whole.  It includes all derived classes of Format.
    """

    def __init__(self):
        self.addFormats([PeakGroupsFormat(), PeakDataFormat(), FluxCircFormat()])
