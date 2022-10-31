import warnings
from collections import defaultdict
from typing import Dict, List

from django.conf import settings
from django.db.models import Model
from django.db.models.signals import m2m_changed
from django.db.transaction import TransactionManagementError
from django.db.utils import IntegrityError
from psycopg2.errors import ForeignKeyViolation

auto_updates = True
update_buffer = []
performing_mass_autoupdates = False
buffering = True
updater_list: Dict[str, List] = defaultdict(list)


def disable_autoupdates():
    """
    Do not allow record changes to trigger the auto-update of maintained fields.  Instead, buffer those updates.
    """
    global auto_updates
    auto_updates = False


def enable_autoupdates():
    """
    Allow record changes to trigger the auto-update of maintained fields and no longer buffer those updates.
    """
    global auto_updates
    auto_updates = True
    if performing_mass_autoupdates:
        raise StaleAutoupdateMode()


def are_autoupdates_enabled():
    return auto_updates


def disable_mass_autoupdates():
    """
    Allow autoupdates to once again be able to trigger updates to parent record fields.
    """
    global performing_mass_autoupdates
    performing_mass_autoupdates = False


def enable_mass_autoupdates():
    """
    This prevents changes from being propagated to parents.  It is mostly only used internally, but is also used in the
    rebuild_maintained_fields script.  This can only be enabled when autoupdates are disabled,
    """
    global performing_mass_autoupdates
    performing_mass_autoupdates = True
    if auto_updates:
        raise StaleAutoupdateMode()


def disable_buffering():
    """
    Do not allow record changes to buffer pending changes to maintained fields.
    """
    global buffering
    buffering = False


def enable_buffering():
    """
    Allow record changes to buffer pending changes to maintained fields.
    """
    global buffering
    buffering = True


def clear_update_buffer(generation=None, label_filters=[]):
    """
    Clears buffered auto-updates.  Use after having performed DB updates when auto_updates was False to no perform
    auto-updates.  This method is called automatically during the execution of mass autoupdates.

    If a generation is provided (see the generation argument of the maintained_field_function decorator), only buffered
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


def maintained_model_relation(
    generation, parent_field_name=None, child_field_names=[], update_label=None
):
    """
    Use this decorator to add connections between classes when it does not have any maintained fields.  For example,
    if you only want to maintain 1 field in 1 class, but you want changes in a related class to trigger updates to
    that field, apply this decorator to the class and set either the parent_field_name and/or the child_field_names to
    trigger those updates of the maintained fields in that related model class.

    Refer to the doc string of the maintained_field_function decorator below for a description of the parameters.

    Example:

    class ModelA(MaintainedModel):
        ...
    @maintained_model_relation(
        generation=1,
        parent_field_name="modela",
        child_field_names=["modelcs", "modelds"],
        update_label="values",
    )
    class ModelB(MaintainedModel):
        modela=ForeignKey(...)
    class ModelC(MaintainedModel):
        modelb = ForeignKey(... related_name="modelcs")
    class ModelD(MaintainedModel):
        modelb = ForeignKey(... related_name="modelds")

    The class decorator in the above example links ModelB to Models A, C, and D.  So if a ModelB object changes, it
    will trigger auto-updated to maintained fields (not shown) in its child model records (first) and it's parent model
    A records.  Likewise, it will pass on triggered updates from those classes if they are set to pass on changes to
    modelB though the parent/chold fields in their decorators.
    """
    # Validate args
    if generation != 0:
        # Make sure internal nodes have parent fields
        if parent_field_name is None:
            raise Exception("parent_field is required if generation is not 0.")
    elif generation == 0 and parent_field_name is not None:
        raise ValueError("parent_field must not have a value when generation is 0.")
    if parent_field_name is None and len(child_field_names) == 0:
        raise ValueError(
            "One or both of parent_field_name or child_field_names is required."
        )

    def decorator(cls):

        func_dict = {
            "update_function": None,
            "update_field": None,
            "parent_field": parent_field_name,
            "child_fields": child_field_names,
            "update_label": update_label,  # Used as a filter to trigger specific series' of (mass) updates
            "generation": generation,  # Used to update from leaf to root for mass updates
        }

        # Add this info to our global updater_list
        class_name = cls.__name__
        updater_list[class_name].append(func_dict)

        # No way to ensure supplied fields exist because the models aren't actually loaded yet, so while that would
        # be nice to handle here, it will have to be handled in MaintanedModel when objects are created

        # Provide some debug feedback
        if settings.DEBUG:
            msg = f"Added maintained_model_relation decorator {class_name} to update"
            if update_label is not None:
                msg += f" '{update_label}'-related"
            if parent_field_name is not None:
                msg += f" parent: {class_name}.{parent_field_name}"
                if len(child_field_names) > 0:
                    msg += " and"
            if len(child_field_names) > 0:
                msg += f"children: [{', '.join([class_name + '.' + c for c in child_field_names])}]"
            print(f"{msg}.")

        return cls

    return decorator


def maintained_field_function(
    generation,
    update_field_name=None,
    parent_field_name=None,
    update_label=None,
    child_field_names=[],
):
    """
    This is a decorator factory for functions in a Model class that are identified to be used to update a supplied
    field and field of any linked parent/child record (for when the record is changed).  This function returns a
    decorator that takes the decorated function.  That function should not use the value of another maintained field in
    its calculation because the order of update is not guaranteed to occur in a favorable series.  It should return a
    value compatible with the field type supplied.

    These decorated functions are identified by the MaintainedModel class, whose save and delete methods override the
    parent model and call the decorated functions to update field supplied to the factory function.  It also propagates
    the updates to the linked dependent model's save methods (if the parent and/or child field name is supplied), the
    assumption being that a change to "this" record's maintained field necessitates a change to another maintained
    field in the linked parent record.  Parent and child field names should only be supplied if a change to "this"
    record means that related foields in parent/child records will need to be recomputed.  There is no need to supply
    parent/child field names if that is not the case.

    The generation input is an integer indicating the hierarchy level.  E.g. if there is no parent, `generation` should
    be 0.  Each subsequence generation should increment generation.  It is used to populate update_buffer when
    auto_updates is False, so that mass updates can be triggered after all data is loaded.

    Note that a class can have multiple fields to update and that those updates (according to their decorators) can
    trigger subsequent updates in different "parent"/"child" records.  If multiple update fields trigger updates to
    different parents, they are triggered in a depth-first fashion.  Child records are updated first, then parents.  If
    a child links back to a parent, already-updated records prevent repeated/looped updates.  However, this only
    becomes relevant when the global variable `auto_updates` is False, mass database changes are made (buffering the
    auto-updates), and then auto-updates are explicitly triggered.

    Note, if there are many decorated methods updating different fields, and all of the "parent"/"child" fields are the
    same, only 1 of those decorators needs to set a parent field.
    """

    if update_field_name is None and (parent_field_name is None and generation != 0):
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
            "child_fields": child_field_names,
            "update_label": update_label,  # Used as a filter to trigger specific series' of (mass) updates
            "generation": generation,  # Used to update from leaf to root for mass updates
        }

        # No way to ensure supplied fields exist because the models aren't actually loaded yet, so while that would be
        # nice to handle here, it will have to be handled in MaintanedModel when objects are created

        # Add this info to our global updater_list
        updater_list[class_name].append(func_dict)

        # Provide some debug feedback
        if settings.DEBUG:
            msg = f"Added maintained_field_function decorator to function {fn.__qualname__} to"
            if update_field_name is not None:
                msg += f" maintain {class_name}.{update_field_name}"
                if parent_field_name is not None or len(child_field_names) > 0:
                    msg += " and"
            if parent_field_name is not None:
                msg += (
                    f" trigger updates to parent: {class_name}." f"{parent_field_name}"
                )
            if parent_field_name is not None and len(child_field_names) > 0:
                msg += " and "
            if child_field_names is not None and len(child_field_names) > 0:
                msg += (
                    f" trigger updates to children: "
                    f"{', '.join([class_name + '.' + c for c in child_field_names])}"
                )
            print(f"{msg}.")

        return fn

    # This returns the actual decorator function which will immediately run on the decorated function
    return decorator


def m2m_propagation_handler(**kwargs):
    """
    Additions to M:M related models do not require a .save() to be called afterwards, thus additions like:
        peakgroup.compounds.add(cmpd)
    do not propagate a change to MSRun as is necessary for automatic field maintenance, expressly because
    peakgroup.save() is not called.  To deal with this, and trigger the necessary automatic updates of maintained
    fields, an m2m_changed signal is attached to all M:M fields in MaintainedModel.__init__ to tell us when a
    MaintainedModel has an M:M field that has been added to.  That causes this method to be called, and from here we
    can propagate the changes.
    """
    obj = kwargs.pop("instance", None)
    act = kwargs.pop("action", None)

    if act.startswith("post_") and isinstance(obj, MaintainedModel) and auto_updates:
        obj.call_dfs_related_updaters()


class MaintainedModel(Model):
    """
    This class maintains database field values for a django.models.Model class whose values can be derived using a
    function.  If a record changes, the decorated function/class is used to update the field value.  It can also
    propagate changes of records in linked models.  Every function in the derived class decorated with the
    `@maintained_field_function` decorator (defined above, outside this class) will be called and the associated field
    will be updated.  Only methods that take no arguments are supported.  This class overrides the class's save and
    delete methods and uses m2m_changed signals as triggers for the updates.
    """

    # used to determine whether the fields have been validated
    maintained_model_initialized: Dict[str, bool] = {}

    def __init__(self, *args, **kwargs):
        """
        This over-ride of the constructor is to prevent developers from explicitly setting values for automatically
        maintained fields.  It also performs a one-time validation check of the updater_dicts.
        """
        class_name = self.__class__.__name__
        for updater_dict in updater_list[class_name]:

            # Ensure the field being set is not a maintained field

            update_fld = updater_dict["update_field"]
            if update_fld and update_fld in kwargs:
                raise MaintainedFieldNotSettable(
                    class_name, update_fld, updater_dict["update_function"]
                )

            # Validate the field values in the updater_list

            # First, create a signature to use to make sure we only check once
            # The creation of a decorator signature allows multiple decorators to be added to 1 class (or function) and
            # only have each one's updater info validated once.
            decorator_signature = ".".join(
                [
                    str(x)
                    for x in [
                        updater_dict["update_label"],
                        updater_dict["update_function"],
                        updater_dict["update_field"],
                        str(updater_dict["generation"]),
                        updater_dict["parent_field"],
                        ",".join(updater_dict["child_fields"]),
                    ]
                ]
            )
            if decorator_signature not in self.maintained_model_initialized:

                if settings.DEBUG:
                    print(
                        f"Validating {self.__class__.__name__} updater: {updater_dict}"
                    )

                self.maintained_model_initialized[decorator_signature] = True
                # Now we can validate the fields
                flds = {}
                if updater_dict["update_field"]:
                    flds[updater_dict["update_field"]] = "update field"
                if updater_dict["parent_field"]:
                    flds[updater_dict["parent_field"]] = "parent field"
                for cfld in updater_dict["child_fields"]:
                    flds[cfld] = "child field"
                bad_fields = []
                for field in flds.keys():
                    try:
                        getattr(self.__class__, field)
                    except AttributeError:
                        bad_fields.append({"field": field, "type": flds[field]})
                if len(bad_fields) > 0:
                    raise BadModelFields(
                        self.__class__.__name__,
                        bad_fields,
                        updater_dict["update_function"],
                    )
                try:
                    # Connect the m2m_propagation_handler to any m2m field change events
                    for m2m_field in self.__class__._meta.many_to_many:
                        m2m_field_ref = getattr(self.__class__, m2m_field.name)
                        through_model = getattr(m2m_field_ref, "through")

                        if settings.DEBUG:
                            print(
                                f"Adding propagation handler to {self.__class__.__name__}.{m2m_field.name}.through"
                            )

                        m2m_changed.connect(
                            m2m_propagation_handler,
                            sender=through_model,
                        )
                    # m2m_changed.connect(toppings_changed, sender=Pizza.toppings.through)
                except AttributeError as ae:
                    if "has no attribute 'many_to_many'" not in str(ae):
                        raise ae
                    # Else - no propagation handler needed

        super().__init__(*args, **kwargs)

    def save(self, *args, **kwargs):
        """
        This is an override of the derived model's save method that is being used here to automatically update
        maintained fields.
        """
        # Custom argument: propagate - Whether to propagate updates to related model objects - default True
        propagate = kwargs.pop(
            "propagate", True
        )  # Used internally. Do not supply unless you know what you're doing.

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

        # If the auto-update resulted in no change or if there exists stale buffer contents for objects that were
        # previously saved, it can produce an error about unique constraints.  TransactionManagementErrors shpould have
        # been handled before we got here so that this can proceed to effect the original change that prompted the
        # save.
        try:
            super().save(*args, **kwargs)
        except (IntegrityError, ForeignKeyViolation) as uc:
            # If this is a unique constraint error during a mass autoupdate
            if performing_mass_autoupdates and (
                "violates foreign key constraint" in str(uc)
                or "duplicate key value violates unique constraint" in str(uc)
            ):
                # Errors about unique constraints during mass autoupdates are often due to stale buffer contents
                raise LikelyStaleBufferError(self)
            else:
                raise uc

        # We don't need to check performing_mass_autoupdates, because propagating changes during buffered updates is
        # handled differently (in a breadth-first fashion) to mitigate repeated updates of the same parent record
        if auto_updates and propagate:
            # Percolate changes up to the parents (if any)
            self.call_dfs_related_updaters()

    def delete(self, *args, **kwargs):
        """
        This is an override of the derived model's delete method that is being used here to automatically update
        maintained fields.
        """
        # Custom argument: propagate - Whether to propagate updates to related model objects - default True
        propagate = kwargs.pop("propagate", True)

        # Delete the record triggering this update
        super().delete(*args, **kwargs)  # Call the "real" delete() method.

        # If auto-updates are turned on, a cascade of updates to linked models will occur, but if they are turned off,
        # the update will be buffered, to be manually triggered later (e.g. upon completion of loading), which
        # mitigates repeated updates to the same record
        if auto_updates is False:
            self.buffer_parent_update()
            return

        if propagate:
            # Percolate changes up to the parents (if any)
            self.call_dfs_related_updaters()

    def get_using_db(self):
        """
        If an instance method makes an unrelated database query and a specific database is currently in use, this
        method will return that database to be used in the fresh query's `.using()` call.  Otherwise, django's code
        base will set the ModelState to the default database, which may differ from where the current model object came
        from.
        """
        db = settings.DEFAULT_DB
        if hasattr(self, "_state") and hasattr(self._state, "db"):
            db = self._state.db
        return db

    def update_decorated_fields(self):
        """
        Updates every field identified in each maintained_field_function decorator using the decorated function that
        generates its value.
        """
        for updater_dict in self.get_my_updaters():
            update_fld = updater_dict["update_field"]

            # If there is a maintained field(s) in this model
            if update_fld is not None:
                try:
                    update_fun = getattr(self, updater_dict["update_function"])
                    try:
                        old_val = getattr(self, update_fld)
                    except Exception as e:
                        if isinstance(e, TransactionManagementError):
                            raise e
                        warnings.warn(
                            f"{e.__class__.__name__} error getting current value: [{str(e)}].  Possibly due to this"
                            "being triggered by a deleted record that is linked in a related model's maintained field."
                        )
                        old_val = "<error>"
                    new_val = update_fun()
                    setattr(self, update_fld, new_val)

                    # Report the auto-update
                    if old_val is None or old_val == "":
                        old_val = "<empty>"
                    print(
                        f"Auto-updated {self.__class__.__name__}.{update_fld} in {self.__class__.__name__}.{self.pk} "
                        f"using {update_fun.__qualname__} from [{old_val}] to [{new_val}]."
                    )
                except TransactionManagementError as tme:
                    self.transaction_management_warning(
                        tme, self, None, updater_dict, "self"
                    )

    def call_dfs_related_updaters(self, updated=None):
        if not updated:
            updated = []
        # Assume I've been called after I've been updated, so app myself to the updated list
        self_sig = f"{self.__class__.__name__}.{self.id}"
        updated.append(self_sig)
        updated = self.call_child_updaters(updated=updated)
        updated = self.call_parent_updaters(updated=updated)
        return updated

    def call_parent_updaters(self, updated):
        """
        This calls parent record's `save` method to trigger updates to their maintained fields (if any) and further
        propagate those changes up the hierarchy (if those records have parents). It skips triggering a parent's update
        if that child was the object that triggered its update, to avoid looped repeated updates.
        """
        parents = self.get_parent_instances()
        for parent_inst in parents:
            # If the current instance's update was triggered - and was triggered by the same parent instance whose
            # update we're about to trigger
            parent_sig = f"{parent_inst.__class__.__name__}.{parent_inst.id}"
            if parent_sig not in updated:

                if settings.DEBUG:
                    self_sig = f"{self.__class__.__name__}.{self.pk}"
                    print(
                        f"Propagating change from child {self_sig} to parent {parent_sig}"
                    )

                # Don't let the save call propagate, because we cannot rely on it returning the updated list (because
                # it could be overridden by another class that doesn't return it (at least, that's my guess as to why I
                # was getting back None when I tried it.)
                parent_inst.save(propagate=False)
                # Instead, we will propagate manually:
                self_sig = f"{self.__class__.__name__}.{self.pk}"
                updated = parent_inst.call_dfs_related_updaters(updated=updated)

        return updated

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
            parent_fld = updater_dict["parent_field"]

            # If there is a parent that should update based on this change
            if parent_fld is not None:

                # Get the parent instance
                tmp_parent_inst = getattr(self, parent_fld)

                # if a parent record exists
                if tmp_parent_inst is not None:

                    try:
                        # Make sure that the (direct) parnet (or M:M related parent) *isa* MaintainedModel
                        if isinstance(tmp_parent_inst, MaintainedModel):

                            parent_inst = tmp_parent_inst
                            if parent_inst not in parents:
                                parents.append(parent_inst)

                        elif (
                            tmp_parent_inst.__class__.__name__ == "ManyRelatedManager"
                            or tmp_parent_inst.__class__.__name__ == "RelatedManager"
                        ):

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

                    except TransactionManagementError as tme:
                        self.transaction_management_warning(
                            tme,
                            self,
                            tmp_parent_inst,
                            updater_dict,
                            "parent",
                            parent_fld,
                        )

        return parents

    def call_child_updaters(self, updated):
        """
        This calls child record's `save` method to trigger updates to their maintained fields (if any) and further
        propagate those changes up the hierarchy (if those records have parents). It skips triggering a child's update
        if that child was the object that triggered its update, to avoid looped repeated updates.
        """
        children = self.get_child_instances()
        for child_inst in children:

            # If the current instance's update was triggered - and was triggered by the same child instance whose
            # update we're about to trigger
            child_sig = f"{child_inst.__class__.__name__}.{child_inst.id}"
            if child_sig not in updated:

                if settings.DEBUG:
                    self_sig = f"{self.__class__.__name__}.{self.pk}"
                    print(
                        f"Propagating change from parent {self_sig} to child {child_sig}"
                    )

                # Don't let the save call propagate, because we cannot rely on it returning the updated list (because
                # it could be overridden by another class that doesn't return it (at least, that's my guess as to why I
                # was getting back None when I tried it.)
                child_inst.save(propagate=False)

                # Instead, we will propagate manually:
                updated = child_inst.call_dfs_related_updaters(updated=updated)

        return updated

    def get_child_instances(self):
        """
        Returns a list of child records to the current record (self) (and the child relationship is stored in the
        updater_list global variable, indexed by class name) based on the child keys indicated in every decorated
        updater method.

        Limitation: This does not return `through` model instances, though it will return children of a through model
        if called from a through model object.  If a through model contains decorated methods to maintain through model
        fields, they will only ever update when the through model object is specifically saved and will not be updated
        when their "child" object changes.  This will have to be modified to return through model instances if such
        updates come to be required.
        """
        children = []
        updaters = self.get_my_updaters()
        for updater_dict in updaters:
            child_flds = updater_dict["child_fields"]

            # If there is a child that should update based on this change
            for child_fld in child_flds:

                # Get the child instance
                tmp_child_inst = getattr(self, child_fld)

                # if a child record exists
                if tmp_child_inst is not None:

                    # Make sure that the (direct) parnet (or M:M related child) *isa* MaintainedModel
                    if isinstance(tmp_child_inst, MaintainedModel):

                        child_inst = tmp_child_inst
                        if child_inst not in children:
                            children.append(child_inst)

                    elif (
                        tmp_child_inst.__class__.__name__ == "ManyRelatedManager"
                        or tmp_child_inst.__class__.__name__ == "RelatedManager"
                    ):

                        try:
                            # NOTE: This is where the `through` model is skipped
                            if tmp_child_inst.count() > 0 and isinstance(
                                tmp_child_inst.first(), MaintainedModel
                            ):

                                for mm_child_inst in tmp_child_inst.all():

                                    if mm_child_inst not in children:
                                        children.append(mm_child_inst)

                            elif tmp_child_inst.count() > 0:
                                raise NotMaintained(tmp_child_inst.first(), self)
                        except TransactionManagementError as tme:
                            self.transaction_management_warning(
                                tme,
                                self,
                                tmp_child_inst,
                                updater_dict,
                                "child",
                                child_fld,
                            )

                    else:
                        raise NotMaintained(tmp_child_inst, self)
                else:
                    raise Exception(
                        f"Unexpected child reference for field [{child_fld}] is None."
                    )

        return children

    @classmethod
    def get_my_updaters(self):
        """
        Retrieves all the updater information of each decorated function of the calling model from the global
        updater_list variable.
        """
        my_updaters = []
        class_name = self.__name__
        if class_name in updater_list:
            my_updaters = updater_list[class_name]
        else:
            raise NoDecorators(class_name)

        return my_updaters

    def buffer_update(self):
        """
        This is called when MaintainedModel.save is called (if auto_updates is False), so that maintained fields can be
        updated after loading code finishes (by calling the global method: perform_buffered_updates)
        """
        # Do not buffer if it's already buffered.  Note, this class isn't designed to support auto-updates in a
        # sepecific order.  All auto-update functions should use non-auto-update fields.
        if buffering and self not in update_buffer:
            update_buffer.append(self)

    def buffer_parent_update(self):
        """
        This is called when MaintainedModel.delete is called (if auto_updates is False), so that maintained fields can
        be updated after loading code finishes (by calling the global method: perform_buffered_updates)
        """
        if buffering:
            parents = self.get_parent_instances()
            for parent_inst in parents:
                update_buffer.append(parent_inst)

    def transaction_management_warning(
        self,
        tme,
        triggering_rec,
        acting_rec=None,
        update_dict=None,
        relationship="self",
        triggering_field=None,
    ):
        """
        Debugging TransactionManagementErrors can be difficult and confusing, so this warning is an attempt to aid that
        debugging effort in the future by encapsulating what I have learned to provide insights in how best to address
        those issues.
        """

        if relationship == "self" and acting_rec is not None:
            raise ValueError("acting_rec must be None if relationship is 'self'.")
        elif relationship != "self" and (
            acting_rec is None or triggering_field is None
        ):
            raise ValueError(
                "acting_rec and triggering_field are required is relationship is not 'self'."
            )

        warning_str = "Ignoring TransactionManagementError and skipping auto-update.  Details:\n\n"

        if relationship == "self":
            warning_str += f"\t- Record being updated: [{triggering_rec.__class__.__name__}.{triggering_rec.id}].\n"
        else:
            warning_str += f"\t- Record being updated: [{acting_rec.__class__.__name__}.{acting_rec.id}].\n"
            warning_str += f"\t- Triggering {relationship} field: "
            warning_str += f"[{triggering_rec.__class__.__name__}.{triggering_field}.{triggering_rec.id}].\n"

        if update_dict is not None:
            warning_str += f"\t- Update field: [{update_dict['update_field']}].\n"
            warning_str += f"\t- Update function: [{update_dict['update_function']}].\n"

        explanation = (
            "Generally, auto-updates do not cause a problem, but in certain situations (particularly in tests), auto-"
            "updates inside an atomic transaction block can cause a problem due to performing queries on a record "
            "that is not yet really saved.  For tests (a special case), this can usually be avoided by keeping "
            "database loads isolated inside setUpTestData and the test function itself.  Note, querys inside setUp() "
            "can trigger this error.  If this is occurring outside of a test run, to avoid errors, the entire "
            "transaction should be done without autoupdates by calling disable_autoupdates() before the transaction "
            "block, and after the atomic transaction block, call perform_mass_autoupdates() to make the updates.  If "
            "this is a warning, note that auto-updates can be fixed afterwards by running:\n\n\tpython manage.py "
            "rebuild_maintained_fields\n\n."
        )

        warning_str += f"\n{explanation}\n\nThe error that occurred: [{str(tme)}]."

        warnings.warn(warning_str)

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


def perform_buffered_updates(labels=None, using=None):
    """
    Performs a mass update of records in the buffer in a depth-first fashion without repeated updates to the same
    record over and over.  It goes through the buffer in the order added and triggers each record's DFS updates, which
    returns the signatures of every updated record.  Those updates are maintained through the traversal of the entire
    buffer and checked before each update, thereby preventing repeated updates.  If a record has already been updated,
    the records it triggers updates to are not propagated either.  The goal is to trigger the updates in the order
    they were designed to follow governed by the parent/child links created in each decorator.

    Note that this can fail if a record is changed and then its child (who triggers its parent) is changed (each being
    added to the buffer during a mass auto-update).  This however is not expected to happen, as mass auto-update is
    used for loading, which if done right, doesn't change child records after parent records have been added.
    """
    global update_buffer
    if labels is None:
        label_filters = []
    else:
        label_filters = labels
    db = using

    if auto_updates:
        raise InvalidAutoUpdateMode()

    if len(update_buffer) == 0:
        return

    # This allows our updates to be saved, but prevents propagating changes up the hierarchy in a depth-first fashion
    enable_mass_autoupdates()
    # Track what's been updated to prevent repeated updates triggered by multiple child updates
    updated = []
    new_buffer = []
    no_filters = len(label_filters) == 0

    # For each record in the buffer
    for buffer_item in update_buffer:
        updater_dicts = buffer_item.get_my_updaters()

        # Track updated records to avoid repeated updates
        key = f"{buffer_item.__class__.__name__}.{buffer_item.pk}"

        # Try to perform the update. It could fail if the affected record was deleted
        try:
            if key not in updated and (
                no_filters or updater_list_has_labels(updater_dicts, label_filters)
            ):
                # Saving the record while performing_mass_autoupdates is True, causes auto-updates of every field
                # included among the model's decorated functions.  It does not only update the fields indicated in
                # decorators that contain the labels indicated in the label_filters.  The filters are only used to
                # decide which records should be updated.  Currently, this is not an issue because we only have 1
                # update_label in use.  And if/when we add another label, it will only end up causing extra
                # repeated updates of the same record.
                if db:
                    buffer_item.save(using=db, propagate=False)
                else:
                    buffer_item.save(propagate=False)

                # Propagate the changes (if necessary), keeping track of what is updated and what's not.
                # Note: all the manual changes are assumed to have been made already, so auto-updates only need to
                # be issued once per record
                updated = buffer_item.call_dfs_related_updaters(updated=updated)

            elif key not in updated and buffer_item not in new_buffer:

                new_buffer.append(buffer_item)

        except Exception as e:
            disable_mass_autoupdates()
            raise AutoUpdateFailed(buffer_item, e, updater_dicts, db)

    # Eliminate the updated items from the buffer
    update_buffer = new_buffer

    # We're done performing buffered updates
    disable_mass_autoupdates()


def get_all_updaters():
    """
    Retrieve a flattened list of all updater dicts.
    Used by rebuild_maintained_fields.
    """
    all_updaters = []
    for class_name in updater_list:
        all_updaters += updater_list[class_name]
    return all_updaters


def get_classes(generation=None, label_filters=[]):
    """
    Retrieve a list of classes containing maintained fields that match the given criteria.
    Used by rebuild_maintained_fields.
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


class BadModelFields(Exception):
    def __init__(self, cls, flds, fcn=None):
        fld_strs = [f"{d['field']} ({d['type']})" for d in flds]
        message = (
            f"The {cls} class does not have field(s): ['{', '.join(fld_strs)}'].  "
        )
        if fcn:
            message += (
                f"Make sure the fields supplied to the @maintained_field_function decorator of the function: {fcn} "
                f"are valid {cls} fields."
            )
        else:
            message += (
                f"Make sure the fields supplied to the @maintained_model_relation class decorator are valid {cls} "
                "fields."
            )
        super().__init__(message)
        self.cls = cls
        self.flds = flds
        self.fcn = fcn


class MaintainedFieldNotSettable(Exception):
    def __init__(self, cls, fld, fcn):
        message = (
            f"{cls}.{fld} cannot be explicitly set.  Its value is maintained by {fcn} because it has a "
            "@maintained_field_function decorator."
        )
        super().__init__(message)
        self.cls = cls
        self.fld = fld
        self.fcn = fcn


class InvalidRootGeneration(Exception):
    def __init__(self, cls, fld, fcn, gen):
        message = (
            f"Invalid generation: [{gen}] for {cls}.{fld} supplied to @maintained_field_function decorator of "
            f"function [{fcn}].  Since the parent_field_name was `None` or not supplied, generation must be 0."
        )
        super().__init__(message)
        self.cls = cls
        self.fld = fld
        self.fcn = fcn
        self.gen = gen


class NoDecorators(Exception):
    def __init__(self, cls):
        message = (
            f"Class [{cls}] does not have any field maintenance functions yet.  Please add the "
            "maintained_field_function decorator to a method in the class or remove the base class 'MaintainedModel' "
            "and an parent fields referencing this model in other models.  If this model has no field to update but "
            "its parent does, create a decorated placeholder method that sets its parent_field and generation only."
        )
        super().__init__(message)


class AutoUpdateFailed(Exception):
    def __init__(self, model_object, err, updaters, db=None):
        database = "unspecified" if db is None else db
        updater_flds = [
            d["update_field"] for d in updaters if d["update_field"] is not None
        ]
        try:
            obj_str = str(model_object)
        except Exception:
            # Displaying the offending object is optional, so catch any error
            obj_str = "unknown"
        message = (
            f"Autoupdate of the {model_object.__class__.__name__} model's fields [{', '.join(updater_flds)}] in the "
            f"{database} database failed for record {obj_str}.  Potential causes: 1. The record was created and "
            "deleted before the buffered update (a catch for the exception should be added and ignored).  2. The "
            "autoupdate buffer is stale and auto-updates are being attempted on partial or deleted records (be sure "
            "to call clear_update_buffer if only updating a subset of the buffered contents).  The triggering "
            f"{err.__class__.__name__} exception: [{err}]."
        )
        super().__init__(message)


class InvalidAutoUpdateMode(Exception):
    def __init__(self):
        message = (
            "Autoupdate mode must remain disabled during a mass update of maintained fields so that parent updates "
            "are not triggered in a depth-first fashion."
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


class LikelyStaleBufferError(Exception):
    def __init__(self, model_object):
        message = (
            f"Uutoupdates to {model_object.__class__.__name__} encountered a unique constraint violation.  Note, this "
            "often happens when the auto-update buffer contains stale records.  Be sure the buffer is empty before "
            "calling disable_autoupdates() (if it is intended to be empty)."
        )
        super().__init__(message)
