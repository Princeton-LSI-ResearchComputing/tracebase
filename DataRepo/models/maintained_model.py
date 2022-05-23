from typing import Dict, List

from django.conf import settings
from django.db.models import Model  # , Manager

auto_updates = True
update_buffer = []
updater_list: Dict[str, List] = {}


def field_updater_function(generation, update_field_name=None, parent_field_name=None):
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
    """

    if update_field_name is None and parent_field_name is None:
        raise Exception(
            "Either an update_field_name or parent_field_name argument is required."
        )

    # The actual decorator
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
            "generation": generation,
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
        # Set the changed value triggering this update
        super().save(*args, **kwargs)

        if auto_updates is False:
            self.buffer_parent_update()
            return

        # Update the fields that change due to the above change (if any)
        self.update_decorated_fields()
        # Now save the updated values (i.e. save again)
        # super().save(*args, **kwargs)
        # Percolate changes up to the parents (if any)
        self.call_parent_updaters()

    def delete(self, *args, **kwargs):
        # Delete the record triggering this update
        super().delete(*args, **kwargs)  # Call the "real" delete() method.

        if auto_updates is False:
            self.buffer_parent_update()
            return

        # Percolate changes up to the parents (if any)
        self.call_parent_updaters()

    def update_decorated_fields(self):
        """
        Updates every field identified in each field_updater_function decorator that generates its value
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
        parents, generations = self.get_parent_instances()

        for parent_inst in parents:
            if isinstance(parent_inst, MaintainedModel):
                print(
                    f"Calling the linked {parent_inst.__class__.__name__} instance's save method."
                )
                parent_inst.save()
            elif parent_inst.__class__.__name__ == "ManyRelatedManager":
                if parent_inst.count() > 0 and isinstance(
                    parent_inst.first(), MaintainedModel
                ):
                    print(
                        f"Calling every M:M linked {parent_inst.first().__class__.__name__} instance's save method."
                    )
                    for mm_parent_inst in parent_inst.all():
                        print(
                            f"Calling every M:M linked {mm_parent_inst.__class__.__name__} instance's save method."
                        )
                        mm_parent_inst.save()
                elif parent_inst.count() > 0:
                    raise NotMaintained(parent_inst.first(), self)
                # Nothing to to do if there are no linked records
            else:
                raise NotMaintained(parent_inst, self)

    def get_parent_instances(self):
        parents = []
        generations = []
        for updater_dict in self.get_my_updaters():
            update_fun = getattr(self, updater_dict["update_function"])
            parent_fld = updater_dict["parent_field"]
            if parent_fld is not None:
                print(f"Looking in {self.__class__.__name__} for {parent_fld}")
                try:
                    parent_inst = getattr(self, parent_fld)
                except AttributeError:
                    raise BadModelField(
                        self.__class__.__name__, parent_fld, update_fun.__qualname__
                    )
                if parent_inst is not None and parent_inst not in parents:
                    parents.append(parent_inst)
                    generations.append(updater_dict["generation"])
        return parents, generations

    @classmethod
    def get_my_updaters(self):
        """
        Convenience method to retrieve all the updater functions of the calling model.
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
        # Figure out the greatest generation number
        # Leaves in the hierarchy are updated first
        gen = None
        for updater_dict in self.get_my_updaters():
            if gen is None or gen < updater_dict["generation"]:
                gen = updater_dict["generation"]
        update_buffer.append({"rec": self, "gen": gen})
        # Note, supporting multiple hierarchies will require more thought. Right now, we're assuming the same or non-
        # conflicting hierarchies

    def buffer_parent_update(self):
        parents, generations = self.get_parent_instances()
        for i, parent_inst in enumerate(parents):
            update_buffer.append({"rec": parent_inst, "gen": generations[i]})

    class Meta:
        abstract = True


def perform_buffered_updates():
    global update_buffer
    # For each record in the buffer
    for buffer_item in sorted(update_buffer, key=lambda x: x["gen"], reverse=True):
        # Try to perform the update. It could fail if the affected record was deleted
        try:
            buffer_item["rec"].save()
        except Exception as e:
            print(
                "WARNING: Buffered record update failed.  The record may have been deleted.  If it was, you may "
                f"ignore this warning.  {e}"
            )
    # Clear the buffer
    update_buffer = []


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
