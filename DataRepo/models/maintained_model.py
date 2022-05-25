from typing import Dict, List

from django.conf import settings
from django.db.models import Model  # , Manager

auto_updates = True
update_buffer = []
performing_buffered_updates = False
updater_list: Dict[str, List] = {}


def disable_autoupdates():
    global auto_updates
    auto_updates = False


def enable_autoupdates():
    global auto_updates
    auto_updates = True


def clear_update_buffer(generation=None, label_filters=[]):
    global update_buffer
    if generation is None:
        update_buffer = []
        return
    new_buffer = []
    gen_warns = 0
    no_filters = len(label_filters) == 0
    for buffered_item in update_buffer:
        updaters_list = buffered_item.get_my_updaters()
        max_gen = get_max_generation(updaters_list, label_filters)
        if generation is None or max_gen != generation:
            if (generation is not None and no_filters) or (
                generation is None
                and (
                    no_filters
                    or not updater_list_has_labels(updaters_list, label_filters)
                )
            ):
                new_buffer.append(buffered_item)
                if max_gen > generation:
                    gen_warns += 1
    if gen_warns > 0:
        print(
            f"WARNING: {gen_warns} records in the buffer are younger than the generation supplied: {generation}.  "
            "Generations should be cleared in order from youngest (/largest generation number/leaf) to oldest(/root)."
        )
    update_buffer = new_buffer


def updater_list_has_labels(updaters_list, label_filters):
    """
    Returns True if any updater dict in updaters_list has 1 of any of the update_labels in the label_filters list
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
        # No way to ensure supplied fields exist, so this is handled in MaintanedModel via .save and .delete
        if class_name in updater_list:
            updater_list[class_name].append(func_dict)
        else:
            updater_list[class_name] = [func_dict]
        if settings.DEBUG:
            local_msg = ""
            if update_field_name is not None:
                local_msg = f" maintain {class_name}.{update_field_name}'s value"
                if parent_field_name is not None:
                    local_msg += " and also"
            parent_msg = ""
            if parent_field_name is not None:
                # print(f"class: {class_name} fn info: {dir(fn)}")
                # print(f"fn info: {fn.__module__}")
                parent_msg = (
                    f" trigger updates of maintained fields in model reference by foreign key: {class_name}."
                    f"{parent_field_name}"
                )
            print(
                f"Added field_updater_function decorator to function {fn.__qualname__} in order to{local_msg}"
                f"{parent_msg}."
            )
        return fn

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
        if auto_updates is False and performing_buffered_updates is False:
            # Set the changed value triggering this update
            super().save(*args, **kwargs)
            print(f"Saved {self.__class__.__name__} ID {self.id}")
            print(f"Buffered autoupdates to {self.__class__.__name__}")
            self.buffer_update()
            return

        # Update the fields that change due to the above change (if any)
        self.update_decorated_fields()

        # Now save the updated values
        super().save(*args, **kwargs)

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
            if update_fld is not None:
                current_val = None
                # Get the field to make sure it exists in the model
                try:
                    current_val = getattr(self, update_fld)
                except AttributeError:
                    raise BadModelField(
                        self.__class__.__name__, update_fld, update_fun.__qualname__
                    )
                new_val = update_fun()
                setattr(self, update_fld, new_val)
                if current_val is None or current_val == "":
                    current_val = "<empty>"
                print(
                    f"Auto-updated {self.__class__.__name__}.{update_fld} using {update_fun.__qualname__} from "
                    f"[{current_val}] to [{new_val}]"
                )
                check_val = getattr(self, update_fld)
                print(f"Check: {check_val}")
                if check_val != new_val:
                    raise Exception("There's a problem")

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
        Returns 2 lists: a list of parent records of the current record (self) (as stored in the updater_list global
        variable) and a same-sized list of each record's decorator info, as stored by the decorator in the
        updater_list global variable.

        Limitation: This does not return `through` model instances.  If a through model contains decorated methods to
        maintain through model fields, this will have to be modified to return through model instances.  Note though
        that changes to through model record will still propagate to linked parent records when the through model
        contains decorators.  It's just that changes to fields in the through models cannot be triggered by changes to
        child records.  Currently, this is not the case in the model structure.
        """
        parents = []
        for updater_dict in self.get_my_updaters():
            update_fun = getattr(self, updater_dict["update_function"])
            parent_fld = updater_dict["parent_field"]
            if parent_fld is not None:
                print(f"Looking in {self.__class__.__name__} for {parent_fld}")
                try:
                    tmp_parent_inst = getattr(self, parent_fld)
                except AttributeError:
                    raise BadModelField(
                        self.__class__.__name__, parent_fld, update_fun.__qualname__
                    )
                if tmp_parent_inst is not None:
                    if isinstance(tmp_parent_inst, MaintainedModel):
                        parent_inst = tmp_parent_inst
                        if parent_inst not in parents:
                            parents.append(parent_inst)
                    elif tmp_parent_inst.__class__.__name__ == "ManyRelatedManager":
                        # NOTE: This skips the `through` model
                        if tmp_parent_inst.count() > 0 and isinstance(
                            tmp_parent_inst.first(), MaintainedModel
                        ):
                            for mm_parent_inst in tmp_parent_inst.all():
                                if mm_parent_inst not in parents:
                                    parents.append(mm_parent_inst)
                        elif tmp_parent_inst.count() > 0:
                            raise NotMaintained(tmp_parent_inst.first(), self)
                        # Nothing to to do if there are no linked records
                    else:
                        raise NotMaintained(tmp_parent_inst, self)
        return parents

    @classmethod
    def get_my_updaters(self):
        """
        Retrieves all the updater functions of the calling model from the global updater_list variable.
        """
        if self.__name__ in updater_list:
            return updater_list[self.__name__]
        else:
            if settings.DEBUG:
                print(
                    f"Class [{self.__name__}] does not have any field maintenance functions."
                )
            return []

    def buffer_update(self):
        """
        This is called when MaintainedModel.save is called if auto_updates is False, so that maintained fields can be
        updated after loading code finishes (by calling the global method: perform_buffered_updates)
        """
        # Figure out the greatest generation number
        # Leaves in the hierarchy are updated first
        gen = None
        updaters_list = self.get_my_updaters()
        for updater_dict in updaters_list:
            if gen is None or gen < updater_dict["generation"]:
                gen = updater_dict["generation"]
        update_buffer.append(self)
        # Note, supporting multiple hierarchies will require more thought. Right now, we're assuming the same or non-
        # conflicting hierarchies

    def buffer_parent_update(self):
        """
        This is called when MaintainedModel.delete is called if auto_updates is False, so that maintained fields can be
        updated after loading code finishes (by calling the global method: perform_buffered_updates)
        """
        parents = self.get_parent_instances()
        for parent_inst in parents:
            update_buffer.append(parent_inst)

    def get_parent_updates(self):
        """
        This is used by perform_buffered_updates to append parent updates to a new locally cached update buffer, in
        order to not repeatedly update the same record via multiple update triggers (e.g. an update triggerted by a
        parent and an update triggered by a direct update to the same record).  This works because
        perform_buffered_updates does not propagate updates per update (depth-first traversal), but rather does a
        breadth-first update.
        """
        local_buffer = []
        parents = self.get_parent_instances()
        for parent_inst in parents:
            local_buffer.append(parent_inst)
        return local_buffer

    class Meta:
        abstract = True


def buffer_size(generation=None, label_filters=[]):
    cnt = 0
    no_filters = len(label_filters) == 0
    for buffered_item in update_buffer:
        updaters_list = buffered_item.get_my_updaters()
        max_gen = get_max_generation(updaters_list, label_filters)
        if generation is None or max_gen == generation:
            if (
                generation is not None
                or no_filters
                or updater_list_has_labels(updaters_list, label_filters)
            ):
                cnt += 1
    return cnt


def get_max_buffer_generation(label_filters=[]):
    youngest_generation = None
    no_filters = len(label_filters) == 0
    exploded_updater_dicts = []
    for buffered_item in update_buffer:
        exploded_updater_dicts += buffered_item.get_my_updaters()
    # Get the largest generation value
    for updater_dict in sorted(
        exploded_updater_dicts, key=lambda x: x["generation"], reverse=True
    ):
        label = updater_dict["update_label"]
        gen = updater_dict["generation"]
        if no_filters or label in label_filters:
            if youngest_generation is None or gen > youngest_generation:
                youngest_generation = gen
                break
    return youngest_generation


def get_max_generation(updaters_list, label_filters=[]):
    max_gen = None
    no_filters = len(label_filters) == 0
    for updater_dict in updaters_list:
        if no_filters or updater_dict["update_label"] in label_filters:
            gen = updater_dict["generation"]
            if max_gen is None or gen > max_gen:
                max_gen = gen
    return max_gen


def perform_buffered_updates(label_filters=[]):
    """
    Performs a mass update of records in the buffer in a breadth-first fashion without repeated updates to the same
    record over and over.
    """
    global update_buffer
    global performing_buffered_updates
    # This prevents propagating changes up the hierarchy in a depth-first fashion
    performing_buffered_updates = True

    # Get the largest generation value
    youngest_generation = get_max_buffer_generation(label_filters)

    updated = {}

    for gen in sorted(range(youngest_generation + 1), reverse=True):
        # Each generation will potentially add parent records to the update_buffer, which we handle here locally
        print(f"Performing buffered updates for generation {gen}")
        add_to_buffer = []
        # For each record in the buffer
        for buffer_item in sorted(
            update_buffer,
            key=lambda x: get_max_generation(x.get_my_updaters(), label_filters),
            reverse=True,
        ):
            updater_dicts = buffer_item.get_my_updaters()
            max_gen = get_max_generation(updater_dicts, label_filters)
            # Leave the loop when the generation changes so that we can update the buffer with parent-triggered updates
            # that are guaranteed to be lesser generation numbers
            if max_gen < gen:
                break
            # Track updated records to avoid repeated updates
            key = f"{buffer_item.__class__.__name__}.{buffer_item.pk}"
            print(f"Checking if we have updated key: {key} already")
            # Try to perform the update. It could fail if the affected record was deleted
            try:
                no_filters = len(label_filters) == 0
                if key not in updated and (
                    no_filters or updater_list_has_labels(updater_dicts, label_filters)
                ):
                    print("Saving")
                    buffer_item.save()
                    updated[key] = True
                    tmp_buffer = buffer_item.get_parent_updates()
                    if len(tmp_buffer) > 0:
                        print(
                            f"Parents exist {len(tmp_buffer)}: [{tmp_buffer[0]}] type: [{type(tmp_buffer[0])}]"
                        )
                        for tmp_buffer_item in tmp_buffer:
                            if (
                                tmp_buffer_item not in update_buffer
                                and tmp_buffer_item not in add_to_buffer
                            ):
                                print(f"Parent: ({tmp_buffer_item})")
                                print(
                                    f"Adding new update triggers from child {buffer_item.id} to parent: "
                                    f"{tmp_buffer_item.id} ({tmp_buffer_item})"
                                )
                                add_to_buffer.append(tmp_buffer_item)
                            else:
                                print(
                                    f"Parent: ({tmp_buffer_item}) is already in the global buffer: {update_buffer} or "
                                    f"the local buffer: {add_to_buffer}"
                                )
                    else:
                        print("No parents")
                else:
                    print("Not saving")
            except Exception as e:
                print(
                    "WARNING: Buffered record update failed.  The record may have been deleted.  If it was, you may "
                    f"ignore this warning.  {e}"
                )
                raise e
        # Clear this generation from the buffer
        clear_update_buffer(generation=gen, label_filters=label_filters)
        print(f"Old update_buffer: {update_buffer}")
        update_buffer += add_to_buffer
        print(f"New update_buffer: {update_buffer}")


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
