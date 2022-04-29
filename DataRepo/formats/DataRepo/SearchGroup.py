from DataRepo.TMPFMT.DataRepo.FluxCircFormat import FluxCircFormat
from DataRepo.TMPFMT.DataRepo.PeakDataFormat import PeakDataFormat
from DataRepo.TMPFMT.DataRepo.PeakGroupsFormat import PeakGroupsFormat
from DataRepo.TMPFMT.FormatGroup import FormatGroup


class SearchGroup(FormatGroup):
    """
    This class groups all search output formats in a single class and adds metadata that applies to all
    search output formats as a whole.  It includes all derived classes of Format.
    """

    def __init__(self):
        self.addFormats([PeakGroupsFormat(), PeakDataFormat(), FluxCircFormat()])
