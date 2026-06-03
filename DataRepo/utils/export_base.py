class ExportBase:
    """The base class of StudiesExporter and ExportsOrganizer.

    Class Attributes:
        staged_ext (str): The string to be appended-to/removed-from export and zip archive filenames when in
            staging_mode.
    """

    staged_ext = ".staged"
