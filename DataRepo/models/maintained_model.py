from typing import Dict, List

from django.conf import settings
from django.db.models import Model

updater_list: Dict[str, List] = {}


def field_updater_function(fn, update_field_name=None, parent_field_name=None):
    """
    This is a decorator for functions in a Model class that are identified to be used to update a supplied field and
    fields of any linked parent record (if the record is changed).  The function should return a value compatible with
    the field type supplied.  These decorators are identified by the MaintainedModel class, whose save and delete
    methods override the parent model and call the given functions to update the supplied field.  It also calls linked
    dependent models (if supplied) update methods.
    """

    if update_field_name is None and parent_field_name is None:
        raise Exception(
            "Either an update_field_name or parent_field_name argument is required."
        )

    # Get the name of the class the function belongs to
    class_name = fn.__qualname__.split(".")[0]
    func_dict = {
        "function": fn.__name__,
        "update_field": update_field_name,
        "parent_field": parent_field_name,
    }
    if class_name in updater_list:
        updater_list[class_name].append(func_dict)
    else:
        updater_list[class_name] = [func_dict]
    if settings.DEBUG:
        print(f"Added field_updater_function decorator to function {fn.__qualname__}")
    return fn


class MaintainedModel(Model):
    """
    This class maintains database field values for a django.models.Model class whose values can be derived using a
    function.  If a record changes, the decorated function is used to update the field value.  It can also propagate
    changes of records in linked models.  Every function in the derived class decorated with the
    `@field_updater_function` decorator (defined above, outside this class) will be called and the associated field
    will be updated.  Only methods that take no arguments are supported.  This class overrides the class's save and
    delete methods as triggers for the updates.
    """

    def save(self, *args, **kwargs):
        # Set the changed value triggering this update
        super().save(*args, **kwargs)
        # Update the fields that change due to the above change (if any)
        self.update_decorated_fields()
        # Now save the updated values (i.e. save again)
        super().save(*args, **kwargs)
        # Percolate changes up to the parents (if any)
        self.call_parent_updaters()

    def delete(self, *args, **kwargs):
        # Delete the record triggering this update
        super().delete(*args, **kwargs)  # Call the "real" delete() method.
        # Percolate changes up to the parents (if any)
        self.call_parent_updaters()

    def update_decorated_fields(self):
        """
        Updates every field identified in each field_updater_function decorator that generates its value
        """
        for updater_dict in self.get_my_updaters():
            update_fun = getattr(self, updater_dict["function"])
            update_fld = updater_dict["update_field"]
            if update_fld is not None:
                setattr(self, update_fld, update_fun())

    def call_parent_updaters(self):
        """
        Cascading cache deletion from self, downward. Call from a root record to delete all belonging to the same root
        parent
        """
        parents = []
        for updater_dict in self.get_my_updaters():
            parent_fld = getattr(self, updater_dict["parent_field"])
            if parent_fld is not None and parent_fld not in parents:
                parents.append(parent_fld)

        for parent_fld in parents:
            parent_instance = getattr(self, parent_fld)
            if isinstance(parent_instance, MaintainedModel):
                parent_instance.save()
            else:
                raise Exception(
                    f"Parent class {parent_instance.__class__.__name__} or {self.__class__.__name__} must inherit "
                    f"from {MaintainedModel.__name__}."
                )

    @classmethod
    def get_my_updaters(cls):
        """
        Convenience method to retrieve all the updater functions of the calling model.
        """
        if cls.__name__ in updater_list:
            return updater_list[cls.__name__]
        else:
            if settings.DEBUG:
                print(
                    f"Class [{cls.__name__}] does not have any field maintenance functions."
                )
            return []

    class Meta:
        abstract = True
