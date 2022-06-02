from typing import Dict, List

from django.conf import settings
from django.db.models import Model  # , Manager

auto_updates = True
update_buffer = []
performing_mass_autoupdates = False
updater_list: Dict[str, List] = {}


def disable_autoupdates():
    global auto_updates
    auto_updates = False


def enable_autoupdates():
    global auto_updates
    auto_updates = True
    if performing_mass_autoupdates:
        raise StaleAutoupdateMode()


def are_autoupdates_enabled():
    return auto_updates


def disable_mass_autoupdates():
    global performing_mass_autoupdates
    performing_mass_autoupdates = False


def enable_mass_autoupdates():
    global performing_mass_autoupdates
    performing_mass_autoupdates = True
    if auto_updates:
        raise StaleAutoupdateMode()


def clear_update_buffer(generation=None, label_filters=[]):
    """
    Clears buffered auto-updates.  Use after having performed DB updates when auto_updates was False to no perform
    auto-updates.  This methid is called automatically during the execution of mass autoupdates.

    If a generation is provided (see the generation argument of the field_updater_function decorator), only buffered
    auto-updates labeled with that generation are cleared.  Note that each record can have multiple auto-update fields
    and thus multiple generation values.  Only the max generation (a leaf) is used in this check because it is assumed
    leaves are updated first during a mass update and that an auto-update updates every maintained field.

    If label_filters is supplied, only buffered auto-updates whose "update_label" is in the label_filters are cleared.
    Note that each record can have multiple auto-update fields and thus multiple update_label values.  Any label match
    will mark this buffered auto-update for removal.

    Note that if both generation and label_filters are supplied, only buffered auto-updates that meet both conditions
    are cleared.
    """
    global update_buffer
    if generation is None and len(label_filters) == 0:
        update_buffer = []
        return
    new_buffer = []
    gen_warns = 0
    for buffered_item in update_buffer:
        filtered_updaters = filter_updaters(
            buffered_item.get_my_updaters(),
            generation,
            label_filters,
            filter_in=False,
        )

        max_gen = 0
        # We should issue a warning if the remaining updaters contain a greater generation, because updates and buffer
        # clear should happen from leaf to root.  And we should only check those which have a target label.
        if generation is not None:
            max_gen = get_max_generation(filtered_updaters, label_filters)

        if len(filtered_updaters) > 0:
            new_buffer.append(buffered_item)
            if max_gen > generation:
                gen_warns += 1

    if gen_warns > 0:
        label_str = ""
        if len(label_filters) > 0:
            label_str = f"with labels: [{', '.join(label_filters)}] "
        print(
            f"WARNING: {gen_warns} records {label_str}in the buffer are younger than the generation supplied: "
            f"{generation}.  Generations should be cleared in order from leaf (largest generation number) to root (0)."
        )

    # populate the buffer with what's left
    update_buffer = new_buffer


def updater_list_has_labels(updaters_list, label_filters):
    """
    Returns True if any updater dict in updaters_list has 1 of any of the update_labels in the label_filters list.
    """
    for updater_dict in updaters_list:
        label = updater_dict["update_label"]
        has_a_label = label is not None
        if has_a_label and label in label_filters:
            return True
    return False


def field_updater_function(
    generation, update_field_name=None, parent_field_name=None, update_label=None
):
    """
    This is a decorator factory for functions in a Model class that are identified to be used to update a supplied
    field and field of any linked parent record (for when the record is changed).  This function returns a decorator
    that takes the decorated function.  That function should return a value compatible with the field type supplied.
    These decoratored functions are identified by the MaintainedModel class, whose save and delete methods override the
    parent model and call the decorated functions to update field supplied to the factory function.  It also propagates
    the updates to the linked dependent model's save methods (if the parent key is supplied), the assumption being that
    a change to "this" record's maintained field necessetates a change to another maintained field in the linked parent
    record.

    The generation input is an integer indicating the hierarchy level.  E.g. if there is no parent, `generation` should
    be 0.  Each subsequence generation should increment generation.  It us used to populate update_buffer when
    auto_updates is False, so that mass updates can be triggered after all data is loaded.

    Note that a class can have multiple fields to update and that those updates (according to their decorators) can
    trigger subsequent updates in different "parent" records.  Records are always updated from the changed record,
    upward.  If multiple update fields trigger updates to different parents, they are trigger in descending order of
    their "generation" value.  However, this only becomes relevant when the global variable `auto_updates` is False,
    mass database changes are made (buffering the auto-updates), and then auto-updates are explicitly triggered.

    Note, if there are many decorated methods updating different fields, and all of the "parent" fields are the same,
    only 1 of those decorators needs to set a parent field.
    """

    if update_field_name is None and parent_field_name is None:
        raise Exception(
            "Either an update_field_name or parent_field_name argument is required."
        )

    # The actual decorator (because a decorator can only take 1 argument (the decorated function).  The "decorator"
    # above is more akin to a global function call that returns this decorator that is immediately applied to the
    # decorated function.
    def decorator(fn):
        # Get the name of the class the function belongs to
        class_name = fn.__qualname__.split(".")[0]
        if parent_field_name is None and generation != 0:
            raise InvalidRootGeneration(
                class_name, update_field_name, fn.__name__, generation
            )
        func_dict = {
            "update_function": fn.__name__,
            "update_field": update_field_name,
            "parent_field": parent_field_name,
            "update_label": update_label,  # Used as a filter to trigger specific series' of (mass) updates
            "generation": generation,  # Used to update from leaf to root for mass updates
        }

        # No way to ensure supplied fields exist, so while that would be nice to handle here, that will have to be
        # handled in MaintanedModel when objects are created

        # Add this info to our global updater_list
        if class_name in updater_list:
            updater_list[class_name].append(func_dict)
        else:
            updater_list[class_name] = [func_dict]

        # Provide some debug feedback
        if settings.DEBUG:
            local_msg = ""
            if update_field_name is not None:
                local_msg = f" maintain {class_name}.{update_field_name}'s value"
                if parent_field_name is not None:
                    local_msg += " and also"
            parent_msg = ""
            if parent_field_name is not None:
                parent_msg = (
                    f" trigger updates of maintained fields in model reference by foreign key: {class_name}."
                    f"{parent_field_name}"
                )
            print(
                f"Added field_updater_function decorator to function {fn.__qualname__} in order to{local_msg}"
                f"{parent_msg}."
            )

        return fn

    # This returns the actual decorator function which will immediately run on the decorated function
    return decorator


# This class and its override of the create method works, but is commented because it is redundant to the override of
# __init__ in MaintainedModel.  Some stack users advised against overrideing __init__ (though I'm sketical as to why),
# but over-riding create does not capture explicit settings of fields via all means.  If the consensus is to not
# override __init__, I can fall back to this strategy.
# class MaintainedModelManager(Manager):
#     """
#     This class is to over-ride the create method and prevent developers from explicitly setting values for
#     automatically maintained fields.
#     """
#     def create(self, **obj_data):
#         class_name = self.model.__name__
#         for updater_dict in updater_list[class_name]:
#             update_fld = updater_dict["update_field"]
#             if update_fld in obj_data:
#                 update_fcn = updater_dict["update_function"]
#                 raise MaintainedFieldNotSettable(class_name, update_fld, update_fcn)
#         return super().create(**obj_data)


class MaintainedModel(Model):
    """
    This class maintains database field values for a django.models.Model class whose values can be derived using a
    function.  If a record changes, the decorated function is used to update the field value.  It can also propagate
    changes of records in linked models.  Every function in the derived class decorated with the
    `@field_updater_function` decorator (defined above, outside this class) will be called and the associated field
    will be updated.  Only methods that take no arguments are supported.  This class overrides the class's save and
    delete methods as triggers for the updates.
    """

    # This data member provides to means to override of the create method, which works, but is commented because it is
    # redundant to the override of __init__ in MaintainedModel.  Some stack users advised against overrideing __init__
    # (though I'm sketical as to why), but over-riding create does not capture explicit settings of fields via all
    # means.  If the consensus is to not override __init__, I can fall back to this strategy.
    # objects = MaintainedModelManager()

    def __init__(self, *args, **kwargs):
        """
        This over-ride of the constructor is to prevent developers from explicitly setting values for automatically
        maintained fields.
        """
        class_name = self.__class__.__name__
        for updater_dict in updater_list[class_name]:
            update_fld = updater_dict["update_field"]
            if update_fld in kwargs:
                update_fcn = updater_dict["update_function"]
                raise MaintainedFieldNotSettable(class_name, update_fld, update_fcn)
        super().__init__(*args, **kwargs)

    def save(self, *args, **kwargs):
        """
        This is an override of the derived model's save method that is being used here to automatically update
        maintained fields.
        """

        # If auto-updates are turned on, a cascade of updates to linked models will occur, but if they are turned off,
        # the update will be buffered, to be manually triggered later (e.g. upon completion of loading), which
        # mitigates repeated updates to the same record
        if auto_updates is False and performing_mass_autoupdates is False:
            # Set the changed value triggering this update
            super().save(*args, **kwargs)
            self.buffer_update()
            return

        # Update the fields that change due to the above change (if any)
        self.update_decorated_fields()

        # Now save the updated values
        super().save(*args, **kwargs)

        # We don't need to check performing_mass_autoupdates, because propagating changes during buffered updates is
        # handled differently (in a breadth-first fashion) to mitigate repeated updates of the same parent record
        if auto_updates is True:
            # Percolate changes up to the parents (if any)
            self.call_parent_updaters()

    def delete(self, *args, **kwargs):
        """
        This is an override of the derived model's delete method that is being used here to automatically update
        maintained fields.
        """
        # Delete the record triggering this update
        super().delete(*args, **kwargs)  # Call the "real" delete() method.

        # If auto-updates are turned on, a cascade of updates to linked models will occur, but if they are turned off,
        # the update will be buffered, to be manually triggered later (e.g. upon completion of loading), which
        # mitigates repeated updates to the same record
        if auto_updates is False:
            self.buffer_parent_update()
            return

        # Percolate changes up to the parents (if any)
        self.call_parent_updaters()

    def update_decorated_fields(self):
        """
        Updates every field identified in each field_updater_function decorator using the decorated function that
        generates its value.
        """
        for updater_dict in self.get_my_updaters():
            update_fun = getattr(self, updater_dict["update_function"])
            update_fld = updater_dict["update_field"]

            # If there is a maintained field(s) in this model
            if update_fld is not None:
                current_val = None
                # Get the field to make sure it exists in the model, and save the current value for reporting
                try:
                    current_val = getattr(self, update_fld)
                except AttributeError:
                    raise BadModelField(
                        self.__class__.__name__, update_fld, update_fun.__qualname__
                    )
                new_val = update_fun()
                setattr(self, update_fld, new_val)

                # Report the auto-update
                if settings.DEBUG:
                    if current_val is None or current_val == "":
                        current_val = "<empty>"
                    print(
                        f"Auto-updated {self.__class__.__name__}.{update_fld} using {update_fun.__qualname__} from "
                        f"[{current_val}] to [{new_val}]"
                    )

    def call_parent_updaters(self):
        """
        This calls parent record's `save` method to trigger updates to their maintained fields (if any) and further
        propagate those changes up the hierarchy (if those records have parents)
        """
        parents = self.get_parent_instances()
        for parent_inst in parents:
            parent_inst.save()

    def get_parent_instances(self):
        """
        Returns a list of parent records to the current record (self) (and the parent relationship is stored in the
        updater_list global variable, indexed by class name) based on the parent keys indicated in every decorated
        updater method.

        Limitation: This does not return `through` model instances, though it will return parents of a through model if
        called from a through model object.  If a through model contains decorated methods to maintain through model
        fields, they will only ever update when the through model object is specifically saved and will not be updated
        when their "child" object changes.  This will have to be modified to return through model instances if such
        updates come to be required.
        """
        parents = []
        for updater_dict in self.get_my_updaters():
            update_fun = getattr(self, updater_dict["update_function"])
            parent_fld = updater_dict["parent_field"]

            # If there is a parent that should update based on this change
            if parent_fld is not None:

                # Get the parent instance and catch the case where it doesn't exist
                try:
                    tmp_parent_inst = getattr(self, parent_fld)
                except AttributeError:
                    raise BadModelField(
                        self.__class__.__name__, parent_fld, update_fun.__qualname__
                    )

                # if a parent record exists
                if tmp_parent_inst is not None:

                    # Make sure that the (direct) parnet (or M:M related parent) *isa* MaintainedModel
                    if isinstance(tmp_parent_inst, MaintainedModel):

                        parent_inst = tmp_parent_inst
                        if parent_inst not in parents:
                            parents.append(parent_inst)

                    elif tmp_parent_inst.__class__.__name__ == "ManyRelatedManager":

                        # NOTE: This is where the `through` model is skipped
                        if tmp_parent_inst.count() > 0 and isinstance(
                            tmp_parent_inst.first(), MaintainedModel
                        ):

                            for mm_parent_inst in tmp_parent_inst.all():
                                if mm_parent_inst not in parents:
                                    parents.append(mm_parent_inst)

                        elif tmp_parent_inst.count() > 0:
                            raise NotMaintained(tmp_parent_inst.first(), self)

                    else:
                        raise NotMaintained(tmp_parent_inst, self)
        return parents

    @classmethod
    def get_my_updaters(self):
        """
        Retrieves all the updater information of each decorated function of the calling model from the global
        updater_list variable.
        """
        my_updaters = []
        if self.__name__ in updater_list:
            my_updaters = updater_list[self.__name__]
        else:
            raise NoDecorators(self.__name__)
        return my_updaters

    def buffer_update(self):
        """
        This is called when MaintainedModel.save is called (if auto_updates is False), so that maintained fields can be
        updated after loading code finishes (by calling the global method: perform_buffered_updates)
        """
        update_buffer.append(self)

    def buffer_parent_update(self):
        """
        This is called when MaintainedModel.delete is called (if auto_updates is False), so that maintained fields can
        beupdated after loading code finishes (by calling the global method: perform_buffered_updates)
        """
        parents = self.get_parent_instances()
        for parent_inst in parents:
            update_buffer.append(parent_inst)

    class Meta:
        abstract = True


def buffer_size(generation=None, label_filters=[]):
    """
    Returns the number of buffered records that contain at least 1 decorated function matching the filter criteria
    (generation and label).
    """
    cnt = 0
    for buffered_item in update_buffer:
        updaters_list = filter_updaters(
            buffered_item.get_my_updaters(),
            generation=generation,
            label_filters=label_filters,
        )
        cnt += len(updaters_list)
    return cnt


def get_max_buffer_generation(label_filters=[]):
    """
    Takes a list of label filters and searches the buffered records to return the max generation found among the
    decorated functions (matching the filter criteria) associated with the buffered model object's class.

    The purpose is so that records can be updated breadth first (from leaves to root).
    """
    exploded_updater_dicts = []
    for buffered_item in update_buffer:
        exploded_updater_dicts += filter_updaters(
            buffered_item.get_my_updaters(), label_filters=label_filters
        )
    return get_max_generation(exploded_updater_dicts, label_filters=label_filters)


def get_max_generation(updaters_list, label_filters=[]):
    """
    Takes a list of updaters and a list of label filters and returns the max generation found in the updaters list.
    """
    max_gen = None
    for updater_dict in sorted(
        filter_updaters(updaters_list, label_filters=label_filters),
        key=lambda x: x["generation"],
        reverse=True,
    ):
        gen = updater_dict["generation"]
        if max_gen is None or gen > max_gen:
            max_gen = gen
            break
    return max_gen


def filter_updaters(updaters_list, generation=None, label_filters=[], filter_in=True):
    """
    Returns a sublist of the supplied updaters_list the meets both the filter criteria (generation matches and
    update_label is in the label_filters), if those filters were supplied.
    """
    new_updaters_list = []
    no_filters = len(label_filters) == 0
    no_generation = generation is None
    for updater_dict in updaters_list:
        gen = updater_dict["generation"]
        label = updater_dict["update_label"]
        has_label = label is not None
        if (no_generation or generation == gen) and (
            no_filters or (has_label and label in label_filters)
        ):
            if filter_in:
                new_updaters_list.append(updater_dict)
        elif filter_in is False:
            new_updaters_list.append(updater_dict)
    return new_updaters_list


def perform_buffered_updates(label_filters=[]):
    """
    Performs a mass update of records in the buffer in a breadth-first fashion without repeated updates to the same
    record over and over.
    """
    global update_buffer
    global performing_mass_autoupdates

    if auto_updates:
        raise InvalidAutoUpdateMode()

    # This allows our updates to be saved, but prevents propagating changes up the hierarchy in a depth-first fashion
    performing_mass_autoupdates = True
    # Get the largest generation value
    youngest_generation = get_max_buffer_generation(label_filters)
    # Track what's been updated to prevent repeated updates triggered by multiple child updates
    updated = {}

    # For every generation from the youngest leaves/children to root/parent
    for gen in sorted(range(youngest_generation + 1), reverse=True):

        # Each generation will potentially add parent records to the update_buffer.  We will locally buffer those
        # parents to be included in the next outer loop
        add_to_buffer = []

        # For each record in the buffer whose label-filtered max generation level updater matches the current
        # generation being updated
        for buffer_item in sorted(
            update_buffer,
            key=lambda x: get_max_generation(x.get_my_updaters(), label_filters),
            reverse=True,
        ):
            updater_dicts = buffer_item.get_my_updaters()

            # Leave the loop when the max generation present changes so that we can update the updated buffer with the
            # parent-triggered updates that were locally buffered during the execution of this loop
            max_gen = get_max_generation(updater_dicts, label_filters)
            if max_gen < gen:
                break

            # Track updated records to avoid repeated updates
            key = f"{buffer_item.__class__.__name__}.{buffer_item.pk}"

            # Try to perform the update. It could fail if the affected record was deleted
            try:
                no_filters = len(label_filters) == 0
                if key not in updated and (
                    no_filters or updater_list_has_labels(updater_dicts, label_filters)
                ):
                    # Saving the record while performing_mass_autoupdates is True, causes auto-updates of every field
                    # included among the model's decorated functions.  It does not only update the fields indicated in
                    # decorators that contain the labels indicated in the label_filters.  The filters are only used to
                    # decide which records should be updated.  Currently, this is not an issue because we only have 1
                    # update_label in use.  And if/when we add another label, it will only end up causing extra
                    # repeated updates of the same record.
                    buffer_item.save()

                    # keep track that this record was updated
                    updated[key] = True

                    # Add parent records to the local buffer (add_to_buffer)
                    tmp_buffer = buffer_item.get_parent_instances()
                    if len(tmp_buffer) > 0:

                        for tmp_buffer_item in tmp_buffer:
                            if (
                                tmp_buffer_item not in update_buffer
                                and tmp_buffer_item not in add_to_buffer
                            ):
                                add_to_buffer.append(tmp_buffer_item)

            except Exception as e:
                raise AutoUpdateFailed(e)

        # Clear this generation from the buffer
        clear_update_buffer(generation=gen, label_filters=label_filters)
        # Add newly buffered records
        update_buffer += add_to_buffer

    # We're done performing buffered updates
    performing_mass_autoupdates = False


def get_all_updaters():
    all_updaters = []
    for class_name in updater_list:
        all_updaters += updater_list[class_name]
    return all_updaters


def get_classes(generation=None, label_filters=[]):
    """
    Retrieve a list of classes containing maintained fields that match the given criteria
    """
    class_list = []
    for class_name in updater_list:
        if (
            len(filter_updaters(updater_list[class_name], generation, label_filters))
            > 0
        ):
            class_list.append(class_name)
    return class_list


class NotMaintained(Exception):
    def __init__(self, parent, caller):
        message = (
            f"Class {parent.__class__.__name__} or {caller.__class__.__name__} must inherit from "
            f"{MaintainedModel.__name__}."
        )
        super().__init__(message)
        self.parent = parent
        self.caller = caller


class BadModelField(Exception):
    def __init__(self, cls, fld, fcn):
        message = (
            f"The {cls} class does not have a field named '{fld}'.  Make sure the fields supplied to the "
            f"@field_updater_function decorator of the function: {fcn}."
        )
        super().__init__(message)
        self.cls = cls
        self.fld = fld
        self.fcn = fcn


class MaintainedFieldNotSettable(Exception):
    def __init__(self, cls, fld, fcn):
        message = (
            f"{cls}.{fld} cannot be explicitly set.  Its value is maintained by {fcn} because it has a "
            "@field_updater_function decorator."
        )
        super().__init__(message)
        self.cls = cls
        self.fld = fld
        self.fcn = fcn


class InvalidRootGeneration(Exception):
    def __init__(self, cls, fld, fcn, gen):
        message = (
            f"Invalid generation: [{gen}] for {cls}.{fld} supplied to @field_updater_function decorator of function "
            f"[{fcn}].  Since the parent_field_name was `None` or not supplied, generation must be 0."
        )
        super().__init__(message)
        self.cls = cls
        self.fld = fld
        self.fcn = fcn
        self.gen = gen


class NoDecorators(Exception):
    def __init__(self, cls):
        message = (
            f"Class [{cls}] does not have any field maintenance functions yet.  Please add the field_updater_function "
            "decorator to a method in the class or remove the base class 'MaintainedModel' and an parent fields "
            "referencing this model in other models.  If this model has no field to update but its parent does, "
            "create a decorated placeholder method that sets its parent_field and generation only."
        )
        super().__init__(message)


class AutoUpdateFailed(Exception):
    def __init__(self, err):
        message = (
            "Autoupdate failed.  If the record was deleted, a catch for the exception should be added and ignored (or "
            f"the code should be edited to avoid it).  The triggering exception: [{err}]."
        )
        super().__init__(message)


class InvalidAutoUpdateMode(Exception):
    def __init__(self):
        message = (
            "Autoupdate mode must remain disabled during a mass update of maintained fields so that parent updates are "
            "not triggered in a depth-first fashion."
        )
        super().__init__(message)


class StaleAutoupdateMode(Exception):
    def __init__(self):
        message = (
            "Autoupdate mode enabled during a mass update of maintained fields.  Automated update of the global "
            "variable performing_mass_autoupdates may have been interrupted during execution of "
            "perform_buffered_updates."
        )
        super().__init__(message)
