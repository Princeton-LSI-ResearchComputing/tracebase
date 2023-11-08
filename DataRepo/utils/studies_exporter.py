import os
from datetime import datetime

from django.db.models import Q
from django.template import loader

from DataRepo.formats.search_group import SearchGroup
from DataRepo.models import Study
from DataRepo.views.search.download import tsv_template_iterator


class StudiesExporter:
    sg = SearchGroup()
    all_data_types = [fmtobj.name for fmtobj in sg.modeldata.values()]
    header_template = loader.get_template(
        "DataRepo/search/downloads/download_header.tsv"
    )
    row_template = loader.get_template("DataRepo/search/downloads/download_row.tsv")

    def __init__(
        self,
        outdir,
        study_targets=None,
        data_types=None,
    ):
        self.bad_searches = {}

        if isinstance(data_types, str):
            data_types = [data_types]
        if isinstance(study_targets, str):
            study_targets = [study_targets]

        self.outdir = outdir
        self.study_targets = study_targets or []
        self.data_types = data_types or self.all_data_types

    def export(self):
        # Export time for the outfile headers
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

        # Identify the study records to export (by name)
        study_names = []
        if len(self.study_targets) > 0:
            for study_target in self.study_targets:
                # Always check for name match
                or_query = Q(name__iexact=str(study_target))
                # If the value looks like an ID
                if study_target.isdigit():
                    or_query |= Q(id__exact=int(study_target))

                try:
                    # Perform a `get` for each record so that non-matching values will raise an exception
                    study_rec = Study.objects.get(or_query)
                    study_names.append(study_rec.name)
                except Exception as e:
                    self.bad_searches[study_target] = e

            # Report all the query issues
            if len(self.bad_searches.keys()) > 0:
                raise BadQueryTerm(self.bad_searches)
        else:
            study_names = list(Study.objects.all().values_list("name", flat=True))

        # Make output directory
        os.mkdir(self.outdir)

        # For each study (name)
        for study_name in study_names:
            # Make study directory
            os.mkdir(os.path.join(self.outdir, study_name))

            # For each data type
            for data_type in self.data_types:
                # A data type name corresponds to a format key
                data_type_key = self.sg.formatNameOrKeyToKey(data_type)

                # Construct a query object understood by the format
                # NOTE: This *assumes* every format in self.sg includes Study.name as searchable
                qry = self.sg.createNewBasicQuery(
                    "Study",
                    "name",
                    "exact",
                    study_name,
                    "identity",
                    data_type_key,
                )

                # Do the query of the format (ignoring count and optional stats)
                results, _, _ = self.sg.performQuery(qry, data_type_key)

                # Output a file
                with open(
                    os.path.join(
                        self.outdir, study_name, f"{study_name}-{data_type}.tsv"
                    ),
                    "w",
                ) as outfile:
                    for line in tsv_template_iterator(
                        self.row_template, self.header_template, results, qry, dt_string
                    ):
                        outfile.write(line)


class BadQueryTerm(Exception):
    def __init__(self, bad_searches_dict):
        deets = [f"{k}: {type(v).__name__}: {v}" for k, v in bad_searches_dict.items()]
        nt = "\n\t"
        message = f"Error searching for studies with name or ID:\n\t{nt.join(deets)}"
        super().__init__(message)
        self.bad_searches_dict = bad_searches_dict
