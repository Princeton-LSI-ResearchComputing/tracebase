from DataRepo.Formats.Group import Group
from DataRepo.Formats.DataRepo.PeakDataFormat import PeakDataFormat
from DataRepo.Formats.DataRepo.PeakGroupsFormat import PeakGroupsFormat
from DataRepo.Formats.DataRepo.FluxCircFormat import FluxCircFormat


class SearchGroup(Group):
    """
    This class groups all search output formats in a single class and adds metadata that applies to all
    search output formats as a whole.  It includes all derived classes of Format.
    """

    def __init__(self):
        self.addFormats([PeakGroupsFormat(), PeakDataFormat(), FluxCircFormat()])
