from collections import namedtuple

import dateutil.parser

from DataRepo.models import (
    Animal,
    Compound,
    Sample,
    Study,
    Tissue,
    value_from_choices_label,
)


class SampleTableLoader:
    """
    Load a sample table
    """

    SampleTableHeaders = namedtuple(
        "SampleTableHeaders",
        [
            "SAMPLE_NAME",
            "SAMPLE_DATE",
            "SAMPLE_RESEARCHER",
            "TISSUE_NAME",
            "STUDY_NAME",
            "STUDY_DESCRIPTION",
            "ANIMAL_NAME",
            "ANIMAL_WEIGHT",
            "ANIMAL_AGE",
            "ANIMAL_SEX",
            "ANIMAL_GENOTYPE",
            "ANIMAL_FEEDING_STATUS",
            "ANIMAL_DIET",
            "ANIMAL_STATE",
            "TRACER_COMPOUND_NAME",
            "TRACER_LABELED_ELEMENT",
            "TRACER_LABELED_COUNT",
            "TRACER_INFUSION_RATE",
            "TRACER_INFUSION_CONCENTRATION",
        ],
    )

    DefaultSampleTableHeaders = SampleTableHeaders(
        SAMPLE_NAME="SAMPLE_NAME",
        SAMPLE_DATE="SAMPLE_DATE",
        SAMPLE_RESEARCHER="SAMPLE_RESEARCHER",
        TISSUE_NAME="TISSUE_NAME",
        STUDY_NAME="STUDY_NAME",
        STUDY_DESCRIPTION="STUDY_DESCRIPTION",
        ANIMAL_NAME="ANIMAL_NAME",
        ANIMAL_WEIGHT="ANIMAL_WEIGHT",
        ANIMAL_AGE="ANIMAL_AGE",
        ANIMAL_SEX="ANIMAL_SEX",
        ANIMAL_GENOTYPE="ANIMAL_GENOTYPE",
        ANIMAL_FEEDING_STATUS="ANIMAL_FEEDING_STATUS",
        ANIMAL_DIET="ANIMAL_DIET",
        ANIMAL_STATE="ANIMAL_STATE",
        TRACER_COMPOUND_NAME="TRACER_COMPOUND_NAME",
        TRACER_LABELED_ELEMENT="TRACER_LABELED_ELEMENT",
        TRACER_LABELED_COUNT="TRACER_LABELED_COUNT",
        TRACER_INFUSION_RATE="TRACER_INFUSION_RATE",
        TRACER_INFUSION_CONCENTRATION="TRACER_INFUSION_CONCENTRATION",
    )

    def __init__(self, sample_table_headers=DefaultSampleTableHeaders):
        self.headers = sample_table_headers
        self.blank = ""

    def load_sample_table(self, data):
        for row in data:

            # Skip BLANK rows
            if row[self.headers.TISSUE_NAME] == self.blank:
                print("Skipping row: Tissue field is empty, assuming blank sample")
                continue

            # Tissue
            tissue, created = Tissue.objects.get_or_create(
                name=row[self.headers.TISSUE_NAME]
            )
            if created:
                print(f"Created new record: Tissue:{tissue}")
            try:
                tissue.full_clean()
                tissue.save()
            except Exception as e:
                print(f"Error saving record: Tissue:{tissue}")
                raise (e)

            # Study
            study, created = Study.objects.get_or_create(
                name=row[self.headers.STUDY_NAME]
            )
            if created:
                if self.headers.STUDY_DESCRIPTION:
                    study.description = row[self.headers.STUDY_DESCRIPTION]
                print(f"Created new record: Study:{study}")
            try:
                study.full_clean()
                study.save()
            except Exception as e:
                print(f"Error saving record: Study:{study}")
                raise (e)

            # Animal
            animal, created = Animal.objects.get_or_create(
                name=row[self.headers.ANIMAL_NAME]
            )
            if created:
                print(f"Created new record: Animal:{animal}")
            if self.headers.ANIMAL_GENOTYPE:
                animal.genotype = row[self.headers.ANIMAL_GENOTYPE]
            if self.headers.ANIMAL_WEIGHT:
                animal.body_weight = row[self.headers.ANIMAL_WEIGHT]
            if self.headers.ANIMAL_FEEDING_STATUS:
                animal.feeding_status = row[self.headers.ANIMAL_FEEDING_STATUS]
            if self.headers.ANIMAL_AGE:
                animal.age = row[self.headers.ANIMAL_AGE]
            if self.headers.ANIMAL_DIET:
                animal.diet = row[self.headers.ANIMAL_DIET]
            if self.headers.ANIMAL_STATE:
                animal.state = row[self.headers.ANIMAL_STATE]
            if self.headers.ANIMAL_SEX:
                animal_sex_string = row[self.headers.ANIMAL_SEX]
                if animal_sex_string in animal.SEX_CHOICES:
                    animal_sex = animal_sex_string
                else:
                    animal_sex = value_from_choices_label(
                        animal_sex_string, animal.SEX_CHOICES
                    )
                animal.sex = animal_sex
            if self.headers.TRACER_COMPOUND_NAME:
                try:
                    tracer_compound_name = row[self.headers.TRACER_COMPOUND_NAME]
                    tracer_compound = Compound.objects.get(name=tracer_compound_name)
                    animal.tracer_compound = tracer_compound
                except Compound.DoesNotExist as e:
                    print(
                        f"ERROR: {self.headers.TRACER_COMPOUND_NAME} not found: Compound:{tracer_compound_name}"
                    )
                    raise (e)
            if self.headers.TRACER_LABELED_ELEMENT:
                tracer_labeled_atom = value_from_choices_label(
                    row[self.headers.TRACER_LABELED_ELEMENT],
                    animal.TRACER_LABELED_ELEMENT_CHOICES,
                )
                animal.tracer_labeled_atom = tracer_labeled_atom
            if self.headers.TRACER_LABELED_COUNT:
                animal.tracer_labeled_count = row[self.headers.TRACER_LABELED_COUNT]
            if self.headers.TRACER_INFUSION_RATE:
                animal.tracer_infusion_rate = row[self.headers.TRACER_INFUSION_RATE]
            if self.headers.TRACER_INFUSION_CONCENTRATION:
                animal.tracer_infusion_concentration = row[
                    self.headers.TRACER_INFUSION_CONCENTRATION
                ]
            try:
                animal.full_clean()
                animal.save()
            except Exception as e:
                print(f"Error saving record: Animal:{animal}")
                raise (e)

            # Sample
            sample_name = row[self.headers.SAMPLE_NAME]
            try:
                sample = Sample.objects.get(name=sample_name)
            except Sample.DoesNotExist:
                print(f"Creating new record: Sample:{sample_name}")
                sample = Sample(
                    name=row[self.headers.SAMPLE_NAME],
                    researcher=row[self.headers.SAMPLE_RESEARCHER],
                    animal=animal,
                    tissue=tissue,
                )
                if self.headers.SAMPLE_DATE:
                    sample_date_str = row[self.headers.SAMPLE_DATE]
                    sample_date = dateutil.parser.parse(sample_date_str)
                    sample.date = sample_date
            try:
                sample.full_clean()
                sample.save()
            except Exception as e:
                print(f"Error saving record: Sample:{sample}")
                raise (e)
