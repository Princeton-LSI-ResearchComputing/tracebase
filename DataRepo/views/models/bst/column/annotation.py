from typing import List, Optional, Type
from warnings import warn

from django.conf import settings
from django.db.models import Field, Model
from django.db.models.aggregates import Count
from django.db.models.expressions import Combinable, Expression

from DataRepo.models.utilities import (
    field_path_to_field,
    field_path_to_model_path,
    get_model_by_name,
    is_key_field,
    is_number_field,
    is_string_field,
    model_path_to_model,
    resolve_field_path,
)
from DataRepo.utils.exceptions import DeveloperWarning, trace
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.filterer.annotation import (
    BSTAnnotFilterer,
)
from DataRepo.views.models.bst.column.sorter.annotation import BSTAnnotSorter


class BSTAnnotColumn(BSTBaseColumn):
    """Class to extend BSTColumn to support annotations.

    BSTAnnotColumn sets 'name' to the name of the annotation field, which is used for sorting and filtering based on the
    column values.

    Note that in order for some BSTColumns' search and sort to work as expected, a BSTAnnotColumn should be used to
    convert them to a simple string or number annotation that is compatible with django's annotate method.

    For example, as a DateTimeField, imported_timestamp, will sort correctly on the server side, but Bootstrap Table
    will sort the page's worth of results using alphanumeric sorting.  Filtering will also be quirky, because of the
    differences between how the database compares a table field that is a datetime to a search string using LIKE versus
    Django's rendering of a datetime in a template.

    Specifically, postgres does not pad a date with zeroes, but django does (by default), so if you filter the column
    based on what you see (a date with a padded '0'), you will not get what you expect from the database.

    You can make the searching and sorting behavior consistent by supplying a function using the converter argument in
    the constructor, like this:

        BSTAnnotColumn(
            "imported_timestamp_str",
            converter=Func(
                F("imported_timestamp"),
                Value("YYYY-MM-DD HH:MI a.m."),
                output_field=CharField(),
                function="to_char",
            ),
        )

    See the BSTBaseColumn docstring for examples on how to customize filtering and sorting behavior.

    The BSTSorter and BSTFilterer provided by the base class will use the annotation field for their operations.
    """

    # Overrides BSTBaseColumn.is_annotation
    is_annotation = True

    def __init__(
        self,
        name: str,
        converter: Combinable,
        model: Optional[Type[Model]] = None,
        help_text: bool = True,
        **kwargs,
    ):
        """Constructor.

        Args:
            name (str): The name of the annotation (and name of the column).
            converter (Combinable): A method to combine, transform, or create database field values, e.g. to a
                CharField.  Converting to a CharField for example, can be necessary for searching and filtering because
                BST only does substring searches.  Note that BSTManyRelatedColumn uses an annotation/converter behind
                the scenes for many-related model fields to prevent sorting from increasing the number of resulting
                rows.
                Recommendations: Refrain from using Coalesce, as its performance is slow in certain contexts.  Consider
                    using a converter that utilizes Case/When instead.
                Example Functions to use (non-comprehensive):
                    Regular fields:
                        Lower
                        Upper
                        etc.
                    Many-related fields:
                        Count
                        Min
                        Max
                        etc.
                    Combining multiple fields:
                        Concat
                        Case/When
                        Coalesce (see recommendations above)
                        etc.
                    Build your own:
                        Func
                        etc.
            model (Optional[Type[Model]]): If provided, field paths will be attempted to be extracted from the converter
                and then 2 things will happen:
                1. If exactly 1 field path exists and help_text is True, it will be used to extract the model field's
                    help_text and populate (or add to) the column header's tooltip.
                2. self.related_model_paths will be set, which can be used to populate the arguments to prefetch_related
                    in order to make queries faster when filtering on an annotations value.
            help_text (bool) [True]: Whether to use the field extracted from the converter's help_text to populate the
                tooltip.  Ignored if model is None.
        Exceptions:
            None
        Returns:
            None
        """
        self.converter = converter

        # self.model is used in conjunction with the field_path extracted from self.converter to set an initial value
        # for self.related_model.  This is dependent on whether a single field_path can be extracted from
        # self.converter.  But note that there is a chance that this won't work to create a link (or create an incorrect
        # link), because it assumes that the output of the annotation is a key field if the extracted field_path from
        # the converter is a key field.  This may not be the case.  This can be overridden by the output_field (also
        # derived from the converter), if it is a key field.
        self.model: Optional[Type[Model]] = model

        # NOTE: self.related_model will only be set if the output of the converter is a foreign key.
        # It enables get_model_object to be able to return a model object of the related_model when supplied with the
        # annotation value (an 'id').
        self.related_model: Optional[Model] = None

        # related_model_paths is used to populate the arguments to prefetch_related so that annotations are constructed
        # efficiently
        self.related_model_paths: List[str] = []

        # output_field is used to set (/override) self.related_model if it is a key field (e.g. ForeignKey or
        # ManyToManyField) (whether model is provided or not).
        output_field: Optional[Field] = None
        self.is_fk = False

        try:
            field_paths = []
            for fp in resolve_field_path(converter, all=True):
                if fp not in field_paths:
                    field_paths.append(fp)
        except TypeError as te:
            field_paths = []
            if model is not None and settings.DEBUG:
                warn(
                    f"{trace(te)}\nUnable to get help_text from field from model '{model.__name__}' in annotation "
                    f"'{name}' expression '{converter}'.  {te}"
                )

        if model is None and any("__" in fp for fp in field_paths) and settings.DEBUG:
            related_model_fps = [fp for fp in field_paths if "__" in fp]
            warn(
                (
                    f"model is None and the annotation contains field paths to related models ({related_model_fps}).  "
                    "Resulting queries may be slow."
                ),
                DeveloperWarning,
            )

        # If we have a model, see if we can extract a field from the combinable in order to populate the tooltip with
        # the field's help_text
        if isinstance(model, type):

            for field_path in field_paths:
                model_path = field_path_to_model_path(model, field_path)
                # The returned model path can be an emoty string (which refers to self.model, which we don't need here)
                if model_path != "" and model_path not in self.related_model_paths:
                    self.related_model_paths.append(model_path)

            if len(field_paths) == 1 and help_text:
                # This gives us the field that the annotation is based on, which could be a reverse relation (that has
                # no help_text attribute)
                field = field_path_to_field(model, field_paths[0], real=False)
                if (
                    # Excluding Count annotations is a cop-out.  There's got to be a better way to not incorporate
                    # help_text when it doesn't make sense.
                    not isinstance(self.converter, Count)
                    and hasattr(field, "help_text")
                    and field.help_text is not None
                ):
                    # If no tooltip was provided
                    if "tooltip" not in kwargs.keys() or kwargs["tooltip"] is None:
                        kwargs.update({"tooltip": field.help_text})
                    else:
                        kwargs.update(
                            {"tooltip": field.help_text + f"\n\n{kwargs['tooltip']}"}
                        )

                # If this is a key field, we will assume that the annotation output is the ID of a record from that
                # model, so we can link it.
                if is_key_field(field_paths[0], model=model):
                    self.related_model = model_path_to_model(model, field_paths[0])
                    # This *might* be a foreign key (i.e. is_fk = True), but we don't know for sure yet.  We would only
                    # be guessing based on the field the annotation is based on.  We set is_fk below once we do know for
                    # sure based on the output_field.

        if isinstance(self.converter, Expression):
            try:
                if isinstance(self.converter.output_field, type):
                    output_field = self.converter.output_field()
                else:
                    output_field = self.converter.output_field
            except AttributeError as ae:
                raise AttributeError(
                    f"Missing required output_field in expression '{self.converter}'.\nPlease supply the "
                    f"'output_field' argument with a Field instance to the expression.\nOriginal error: {ae}"
                ).with_traceback(ae.__traceback__)

            if not is_number_field(output_field) and not is_string_field(output_field):
                kwargs["sortable"] = False
                kwargs["searchable"] = False
        else:
            kwargs["sortable"] = False
            kwargs["searchable"] = False

        # # If we have an output field that is a key field (overriding any model as could have been derived from a
        # field_path extracted from the converter)
        if output_field is not None and is_key_field(output_field):
            self.related_model = get_model_by_name(output_field.related_model)
            self.is_fk = True

        super().__init__(name, **kwargs)

    def create_sorter(self, **kwargs) -> BSTAnnotSorter:
        if "_server_sorter" not in kwargs.keys() or kwargs["_server_sorter"] is None:
            kwargs["_server_sorter"] = (
                BSTAnnotSorter.get_server_sorter_matching_expression(self.converter)
            )
        if "name" not in kwargs.keys() or kwargs["name"] is None:
            kwargs["name"] = self.name
        return BSTAnnotSorter(self.converter, **kwargs)

    def create_filterer(
        self, field: Optional[str] = None, **kwargs
    ) -> BSTAnnotFilterer:
        field_path = field if field is not None else self.name
        return BSTAnnotFilterer(field_path, **kwargs)

    def get_model_object(self, id: int):
        """If the field extracted from the converter is a foreign key and its output field is a foreign key, call this
        method, supplying it the annotation value), to get the model object.

        Assumptions:
            1. If self.related_model is not None, then the output of the annotation is a foreign key value that can be
                looked up.
        Limitations:
            1. Does not raise an exception if the record is not found
        Args:
            id (int): A model key value
        Exceptions:
            None
        Returns:
            (Optional[Model])
        """
        fk_warning: Optional[str] = None
        if not self.is_fk:
            fk_warning = f"get_model_object called on a non-foreign key annotation column ('{self.name}')."

        if not isinstance(id, int):
            if fk_warning is not None:
                fk_warning += "\n"
            fk_warning = (
                f"get_model_object was supplied a {type(id).__name__}: '{id}' instead of the expected integer for "
                f"annotation column '{self.name}'."
            )

        if (
            self.related_model is not None
            and issubclass(self.related_model, Model)
            and isinstance(id, int)
        ):
            obj = self.related_model.objects.filter(pk=id).first()
            if settings.DEBUG:
                if obj is None:
                    warn(
                        (
                            f"{trace()}\n{self.related_model.__name__} record not found using annotation value '{id}' "
                            f"from annotation column '{self.name}'."
                        ),
                        DeveloperWarning,
                    )
                elif fk_warning is not None:
                    warn(f"{trace()}\n{fk_warning}", DeveloperWarning)

            return obj

        elif fk_warning is not None and settings.DEBUG:
            warn(f"{trace()}\n{fk_warning}", DeveloperWarning)

        return None
