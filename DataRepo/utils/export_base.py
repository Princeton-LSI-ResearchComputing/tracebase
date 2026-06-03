import socket

from django.utils.text import get_valid_filename

from DataRepo.models.study import Study


class ExportBase:
    """The base class of StudiesExporter and ExportsOrganizer.

    Class Attributes:
        staged_ext (str): The string to be appended-to/removed-from export and zip archive filenames when in
            staging_mode.
        default_host (str): Host/domain name of the TraceBase instance (with dashes replaced with underscores).
    Instance Attributes:
        study_names (Dict[int, str]): Study ID keys mapped to actual (not slugified) study names for the current host.
        slugified_study_names (Dict[int, str]): Study ID keys mapped to slugified study names for the current host.
        export_dir (str): The path to the existing output directory.
        staging_mode (bool) [False]: Write outputs as .staged files and publish them (and staged files produced by
            StudiesExporter.export()) by renaming them to their final names when processing completes.
    """

    staged_ext = ".staged"
    default_host = socket.getfqdn().lower().replace("-", "_")

    def __init__(self):
        # NOTE: These (slugified) study names are only for the *current* host.
        self.slugified_study_names = self.get_slugified_study_names_dict()
        self.study_names = dict((rec.id, rec.name) for rec in Study.objects.all())

        self.export_dir: str
        self.staging_mode = False
        self.overwrite = False

    @classmethod
    def get_slugified_study_names_dict(cls):
        """Returns a dict of slugified study names suitable for inclusion in a dash-delimited filename.

        Args:
            None
        Exceptions:
            None
        Returns:
            (Dict[int, str]): Study ID keys mapped to slugified study names.
        """
        return dict(
            (
                rec.id,
                get_valid_filename(rec.name).replace("-", "_"),
            )
            for rec in Study.objects.all()
        )
