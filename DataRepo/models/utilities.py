import importlib
import re
import warnings

from chempy import Substance
from chempy.util.periodic import atomic_number
from django.apps import apps
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import IntegrityError
from django.db.models import Model, Q
from django.forms.models import model_to_dict

# Generally, child tables are at the top and parent tables are at the bottom
ALL_MODELS_IN_SAFE_DELETION_ORDER = [
    "Compound",
    "CompoundSynonym",
    "LCMethod",
    "Tissue",
    "PeakDataLabel",
    "PeakData",
    "PeakGroup",
    "PeakGroupLabel",
    "MSRunSample",
    "MSRunSequence",
    "ArchiveFile",
    "DataType",
    "DataFormat",
    "FCirc",
    "Sample",
    "Animal",
    "AnimalLabel",
    "TracerLabel",
    "Tracer",
    "Infusate",
    "InfusateTracer",
    "Protocol",
    "Study",
]


def value_from_choices_label(label, choices):
    """
    Return the choices value for a given label
    """
    # Search choices by label
    result = None
    for choices_value, choices_label in choices:
        if label == choices_label:
            result = choices_value
    # If search by label failed, check if we already have a valid value
    if result is None:
        if label in dict(choices):
            result = label
    # If we didn't fine anything, but something was provided it's invalid
    if label is not None and result is None:
        raise ValidationError(
            f"'{label}' is not a valid selection, must be one of {choices}"
        )
    return result


def atom_count_in_formula(formula, atom):
    """
    Return the count of supplied atom in the compound.
    Returns None if atom is not a recognized symbol.
    Returns 0 if the atom is recognized, but not found in the compound.
    """
    substance = Substance.from_formula(formula)
    try:
        count = substance.composition.get(atomic_number(atom))
    except (ValueError, AttributeError):
        warnings.warn(f"{atom} not found in list of elements")
        count = None
    else:
        if count is None:
            # Valid atom, but not in formula
            count = 0
    return count


def get_all_models():
    """
    Retrieves all models (that were explicitly defined, i.e. no hidden related models) from DataRepo and returns them
    as a list ordered in a way that they can all have their contents deleted without running afould of "restrict"
    constraints
    """
    module = importlib.import_module("DataRepo.models")
    mdls = [
        getattr(module, class_name) for class_name in ALL_MODELS_IN_SAFE_DELETION_ORDER
    ]
    return mdls


def get_model_fields(model):
    """
    Retrieves all non-auto- and non-relation- fields from the supplied model and returns as a list
    """
    return [
        f
        for f in model._meta.get_fields()
        if f.get_internal_type() != "AutoField" and not getattr(f, "is_relation")
    ]


# TODO: When SampleTableLoader inherits from TableLoader, remove this method (already copied to loader.pu)
def get_unique_constraint_fields(model):
    """Returns a list of lists of names of fields and Q expressions (if constraint.condition is defined) that represent
    all of a model's unique constraints.  The purpose is to be able to use the fields to query for unique records that
    caused a unique constraint violation so that the conflicting values can be extracted and described.

    Agrs:
        model (Type[Model]): The model class whose unique constraint fields are to be extract extracted

    Exceptions:
        None

    Returns:
        uflds (list of str and Q objects): The field names in the unique constraint and a Q object from the
            UniqueConstraint.condition (if defined)
    """
    uflds = []
    if hasattr(model._meta, "constraints"):
        for constraint in model._meta.constraints:
            if type(constraint).__name__ == "UniqueConstraint":
                fields = list(constraint.fields)
                uflds.append(fields)
                if (
                    hasattr(constraint, "condition")
                    and constraint.condition is not None
                ):
                    fields.append(constraint.condition)
    return uflds


# TODO: When SampleTableLoader inherits from TableLoader, remove this method (already copied to loader.pu)
def get_unique_fields(model, fields=None):
    """
    Returns a list of non-auto-field names where unique is True.

    If fields (list of field names) is provided, the returned field names are limited to the list provided.
    """
    return [
        f.name if hasattr(f, "name") else f.field_name
        for f in get_non_auto_model_fields(model)
        if (fields is None or field_in_fieldnames(f, fields))
        and hasattr(f, "unique")
        and f.unique
    ]


# TODO: When SampleTableLoader inherits from TableLoader, remove this method (already copied to loader.pu)
def get_enumerated_fields(model, fields=None):
    """
    Returns a list of non-auto-field names where choices is populated.

    If fields (list of field names) is provided, the returned field names are limited to the list provided.
    """
    return [
        f.name if hasattr(f, "name") else f.field_name
        for f in get_non_auto_model_fields(model)
        if (fields is None or field_in_fieldnames(f, fields))
        and hasattr(f, "choices")
        and f.choices
    ]


# TODO: When SampleTableLoader inherits from TableLoader, remove this method (already copied to loader.pu)
def field_in_fieldnames(fld, fld_names):
    """
    Accessory function to get_unique_fields and get_enumerated_fields.  This only exists in order to avoid JSCPD errors.
    """
    # Relation fields do not have "name" attributes.  Instead, they have "field_name" attributes.  The values of both
    # are the attributes of the model object that we are after (because they can be used in queries).  It is assumed
    # that a field is guaranteed to have one or the other.
    return (hasattr(fld, "name") and fld.name in fld_names) or (
        hasattr(fld, "field_name") and fld.field_name in fld_names
    )


# TODO: When SampleTableLoader inherits from TableLoader, remove this method (already copied to loader.pu)
def get_non_auto_model_fields(model):
    """
    Retrieves all non-auto-fields from the supplied model and returns as a list of actual fields.
    """
    return [f for f in model._meta.get_fields() if f.get_internal_type() != "AutoField"]


def get_all_fields_named(target_field):
    """
    Dynamically retrieves all fields from any model with a specific name
    """
    models = get_all_models()
    found_fields = []
    for model in models:
        fields = list(get_model_fields(model))
        for field in fields:
            if field.name == target_field:
                found_fields.append([model, field])
    return found_fields


def dereference_field(field_name, model_name):
    mdl = get_model_by_name(model_name)
    fld = getattr(mdl, field_name)
    deref_field = field_name
    # If this is a foreign key (i.e. it's a model reference, not an actual DB field)
    if fld.field.__class__.__name__ == "ForeignKey":
        deref_field += "__pk"
    return deref_field


def get_model_by_name(model_name):
    return apps.get_model("DataRepo", model_name)


def create_is_null_field(field_with_null):
    """
    The Django ORM's order_by always puts null fields at the end, which can be a problem if you want the last record
    ordered by a date field.  This method will return a dict that contains the `select` and `order_by arguments for the
    .extra(method, along with the name of the added "is null field that you can use as an argument to `order_by`)`.

    Example: extra_args, is_null_field = create_is_null_field("msrun_sequence__date")
    MSRunSequence.objects.extra(**extra_args).order_by(f"-{is_null_field}", "msrun_sequence__date")

    Note, adding .annotate() doesn't seem to work, or rather I couldn't get it to work. See:
    https://stackoverflow.com/questions/7749216/django-order-by-date-but-have-none-at-end

    See the Django documentation for `extra()` and note that it is currently deprecated, and will go away in version 5.
    TODO: Attempt again to solve the problem this function works around (see docstring)
    """
    is_null_field_name = f"{field_with_null}_is_null"

    # Build an SQL field name from the ORM field name
    field_with_null_path_list = field_with_null.split("__")
    if len(field_with_null_path_list) > 1:
        field = field_with_null_path_list.pop()
        table = field_with_null_path_list.pop()
        field_with_null_ref = '"DataRepo_' + table + '".'
        field_with_null_ref += f'"{field}"'
    else:
        field_with_null_ref = field_with_null

    select_val = {is_null_field_name: f"{field_with_null_ref} IS NULL"}
    order_by_val = [is_null_field_name, field_with_null]

    return (
        {
            "select": select_val,
            "order_by": order_by_val,
        },
        is_null_field_name,
    )


def model_as_dict(obj):
    return model_to_dict(obj)


def exists_in_db(mdl_obj):
    """
    This takes a model object and returns a boolean to indicate whether the object exists in the database.

    Note, it does not assert that the values of the fields are the same.
    """
    if not hasattr(mdl_obj, "pk"):
        return False
    try:
        type(mdl_obj).objects.get(pk__exact=mdl_obj.pk)
    except Exception as e:
        if issubclass(type(e), ObjectDoesNotExist):
            return False
        raise e
    return True


def update_rec(rec: Model, rec_dict: dict):
    """Update the supplied model record using the fields and values in the supplied rec_dict.
    This could be accomplished using a queryset.update() call, but if it changes a field that was used in the original
    query, and that query no longer matches, you cannot iterate through the records of the queryset to save the changes
    you've made, thus the need for this method.
    Args:
        rec (Model)
        rec_dict (dict): field values keyed on field name
    Exceptions:
        None
    Returns:
        None
    """
    for fld, val in rec_dict.items():
        setattr(rec, fld, val)
    rec.full_clean()
    rec.save()


# TODO: When SampleTableLoader inherits from TableLoader, remove this method (already copied to loader.pu)
def check_for_inconsistencies(rec, rec_dict, rownum=None, sheet=None, file=None):
    """
    This function compares the supplied database model record with the dict that was used to (get or) create a record
    that resulted (or will result) in an IntegrityError (i.e. a unique constraint violation).  Call this method inside
    an `except IntegrityError` block, e.g.:
        try:
            rec_dict = {field values for record creation}
            rec, created = Model.objects.get_or_create(**rec_dict)
        except IntegrityError as ie:
            rec = Model.objects.get(name="unique value")
            conflicts.extend(check_for_inconsistencies(rec, rec_dict))
    (It can also be called pre-emptively by querying for only a record's unique field and supply the record and a dict
    for record creation.  E.g.:
        rec_dict = {field values for record creation}
        rec = Model.objects.get(name="unique value")
        new_conflicts = check_for_inconsistencies(rec, rec_dict)
        if len(new_conflicts) == 0:
            Model.objects.create(**rec_dict)
    The purpose of this function is to provide helpful information in an exception (i.e. repackage an IntegrityError) so
    that users working to resolve the error can quickly identify and resolve the issue.
    """
    # This was moved here (from its prior global location at the top) to avoid circular import
    from DataRepo.utils.exceptions import ConflictingValueError

    conflicting_value_errors = []
    differences = {}
    for field, new_value in rec_dict.items():
        orig_value = getattr(rec, field)
        if orig_value != new_value:
            differences[field] = {
                "orig": orig_value,
                "new": new_value,
            }
    if len(differences.keys()) > 0:
        conflicting_value_errors.append(
            ConflictingValueError(
                rec,
                differences,
                rec_dict=rec_dict,
                rownum=rownum,
                sheet=sheet,
                file=file,
            )
        )
    return conflicting_value_errors


# TODO: When SampleTableLoader inherits from TableLoader, remove this method (already copied to loader.pu)
def handle_load_db_errors(
    exception,
    model,
    rec_dict,
    aes=None,
    conflicts_list=None,
    missing_list=None,
    rownum=None,
    sheet=None,
    file=None,
    fld_to_col=None,
):
    """Handles IntegrityErrors and ValidationErrors raised during database loading.  Put in `except` block.

    The purpose of this function is to provide helpful information in an exception (i.e. repackage an IntegrityError or
    a ValidationError) so that users working to resolve errors can quickly identify and resolve data issues.  It calls
    check_for_inconsistencies.

    This function evaluates whether the supplied exception is the result of either a field value conflict or a
    validation error (triggered by a full_clean).  It will either buffer a ConflictingValue error in either the supplied
    conflicts_list or AggregatedErrors object, or raise an exception.

    Args:
        exception (Exception): Exception, e.g. obtained from `except` block
        model (Model): Model being loaded when the exception occurred
        rec_dict (dict): Fields and their values that were passed to either `create` or `get_or_create`
        aes (AggregatedErrors): Aggregated errors object
        conflicts_list (list): List to which ConflictingValueError exception objects will be added.
        missing_list (list): List to which RequiredValuesError exception objects will be added.
        rownum (int): Line or row number of the text or excel file that was being loaded when the exception occurred.
        sheet (str): Name of the Excel sheet that was being loaded when the exception occurred.
        file (str): Name (path optional) of the file that was being loaded when the exception occurred.
        fld_to_col (dict): Supply if you want to map field names (keys from the rec_dict) to column header
            names in the file.  Field names parsed from IntegrityErrors will be used for the column name value (which
            you can customize in a string for example, to represent multiple columns that were used to produce the field
            value).

    Raises (or buffers):
        ValueError
        InfileDatabaseError
        ConflictingValueError
        RequiredValuesError

    Returns:
        boolean indicating whether an error was handled(/buffered).
    """
    # This was moved here (from its prior global location at the top) to avoid circular import
    from DataRepo.utils.exceptions import (
        InfileDatabaseError,
        RequiredValueError,
    )

    # Either aes or conflicts_list is required
    if aes is None and conflicts_list is None:
        raise ValueError(
            "Either aes and/or conflicts_list is required by handle_load_db_errors()."
        )

    # We may or may not use estr and exc, but we're pre-making them here to reduce code duplication
    estr = str(exception)
    exc = InfileDatabaseError(
        exception, rec_dict, rownum=rownum, sheet=sheet, file=file
    )

    if isinstance(exception, IntegrityError):
        if "duplicate key value violates unique constraint" in estr:
            if find_conflicts(
                model,
                rec_dict,
                aes=aes,
                conflicts_list=conflicts_list,
                rownum=rownum,
                sheet=sheet,
                file=file,
            ):
                return True
        elif "violates not-null constraint" in estr:
            regexp = re.compile(r"^null value in column \"(?P<colname>[^\"]+)\"")
            match = re.search(regexp, estr)
            if match:
                fldname = match.group("fldname")
                colname = fldname
                # Convert the database column name to the file column header, if available
                if fld_to_col is not None and colname in fld_to_col.keys():
                    colname = fld_to_col[fldname]
                err = RequiredValueError(
                    column=colname,
                    rownum=rownum,
                    model_name=model.__name__,
                    field_name=fldname,
                    sheet=sheet,
                    file=file,
                )
                if missing_list is not None:
                    missing_list.append(err)
                    return True
                elif aes:
                    aes.buffer_error(err)
                    return True
        elif aes is not None:
            # Repackage any IntegrityError with useful info
            aes.buffer_error(exc)
            return True

        # Return False if we weren't able to handle the exception
        return False

    elif isinstance(exception, ValidationError):
        if "is not a valid choice" in estr:
            choice_fields = get_enumerated_fields(model, fields=rec_dict.keys())
            for choice_field in choice_fields:
                if choice_field in estr and rec_dict[choice_field] is not None:
                    if aes is not None:
                        # Only include error once
                        if not aes.exception_type_exists(InfileDatabaseError):
                            aes.buffer_error(exc)
                        else:
                            # NOTE: This is not perfect.  There can be multiple field values with issues.  Repeated
                            # calls to this function could potentially reference the same record and contain an error
                            # about a different field.  We only buffer/raise one of them because the details include the
                            # entire record and dict causing the issue(s).
                            already_buffered = False
                            for existing_exc in aes.get_exception_type(
                                InfileDatabaseError
                            ):
                                if existing_exc.rec_dict != rec_dict:
                                    already_buffered = True
                            if not already_buffered:
                                aes.buffer_error(exc)
                        # Whether we buffered or not, the error was identified and handled (by either buffering or
                        # ignoring a duplicate)
                        return True
                    # Since we weren't supplied an AggregatedErrors object and this is an exception supported by this
                    # function, raise the exception
                    raise exc
        elif "Constraint " in estr and " is violated." in estr:
            # E.g. ValidationError: {'__all__': ['Constraint “unique_msrunsample_placeholder” is violated.']}
            if find_conflicts(
                model,
                rec_dict,
                aes=aes,
                conflicts_list=conflicts_list,
                rownum=rownum,
                sheet=sheet,
                file=file,
            ):
                return True

    # If we get here, we did not identify the error as one we knew what to do with
    return False


def find_conflicts(
    model, rec_dict, aes=None, conflicts_list=None, rownum=None, sheet=None, file=None
):
    """Helper to handle_load_db_errors.  In handle_load_db_errors, there are 2 ways unique constraint violations can
    occur, both due to conflicts.  One is from an IntegrityError and the other (a code-check) is from a ValidationError
    arising from qa call to rec.full_clean().  This method finds the existing db record using the unique fields and
    fields inside various UniqueConstraints, and describes the differences with the attempted load of the data from the
    dict that caused either exception.

    Args:
        model (Model): The module whose record creation produced an exception
        rec_dict (dict): The dict supplied to a get_or_create or create that produced the exception
        aes (Optional[AggregatedErrors]): Where to buffer errors
        conflicts_list (Optional[list]): Where to put ConflictingValueError exceptions
        rownum (Optional[int]): Input file location associated with the error.
        sheet (Optional[str]): Input file location associated with the error.
        file (Optional[str]): Input file location associated with the error.

    Exceptions:
        Raises:
            None
        Buffers:
            ConflictingValueError (as returned by check_for_inconsistencies)

    Returns:
        boolean: Whether exceptions were buffered or not
    """
    # TODO: Update the corresponding TableLoader.handle_load_db_errors method with this update
    # Create a list of lists of unique fields and unique combos of fields
    # First, get unique fields and force them into a list of lists (so that we only need to loop once)
    unique_combos = [[f] for f in get_unique_fields(model, fields=rec_dict.keys())]
    # Now add in the unique field combos from the model's unique constraints
    unique_combos.extend(get_unique_constraint_fields(model))

    # Create a set of the fields in the dict causing the error so that we can only check unique its fields
    field_set = set(rec_dict.keys())

    # We're going to loop over unique records until we find one that conflicts with the dict
    for combo_fields in unique_combos:
        # Only proceed if we have all the values
        combo_set = set([f for f in combo_fields if isinstance(f, str)])
        if not combo_set.issubset(field_set):
            continue

        # Retrieve the record with the conflicting value(s) that caused the unique constraint error using the
        # unique fields
        q = Q()
        for uf in combo_fields:
            if isinstance(uf, Q):
                q &= uf
            elif rec_dict[uf] is not None:
                # We do not need to check None values.  None != None
                q &= Q(**{f"{uf}__exact": rec_dict[uf]})
        qs = model.objects.filter(q)
        if qs.count() == 1:
            rec = qs.first()
            errs = check_for_inconsistencies(
                rec, rec_dict, rownum=rownum, sheet=sheet, file=file
            )
            if len(errs) > 0:
                if conflicts_list is not None:
                    conflicts_list.extend(errs)
                    return True
                elif aes:
                    for err in errs:
                        aes.buffer_error(err)
                    return True

    return False
