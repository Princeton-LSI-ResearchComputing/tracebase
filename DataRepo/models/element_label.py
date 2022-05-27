# abstract class for models which are "labeled" with tracers; because we have
# not normalized column names, this is not a Django Abstract base class
# (https://docs.djangoproject.com/en/3.1/topics/db/models/#abstract-base-classes).
# This simply shares some configured variables/values.
class ElementLabel:

    # choice specifications
    CARBON = "C"
    NITROGEN = "N"
    HYDROGEN = "H"
    OXYGEN = "O"
    SULFUR = "S"
    LABELED_ELEMENT_CHOICES = [
        (CARBON, "Carbon"),
        (NITROGEN, "Nitrogen"),
        (HYDROGEN, "Hydrogen"),
        (OXYGEN, "Oxygen"),
        (SULFUR, "Sulfur"),
    ]

    MIN_MASS_NUMBER = 1
    MAX_MASS_NUMBER = 120
    MAX_LABELED_ATOMS = 20
    MAX_COMPOUND_POSITION = 20

    @classmethod
    def labeled_elements_list(cls):
        return [choice[0] for choice in cls.LABELED_ELEMENT_CHOICES]
