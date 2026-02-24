import importlib
import warnings
from collections import defaultdict
from contextlib import contextmanager
from threading import local
from typing import Dict, List, Optional, Type

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db import ProgrammingError, transaction
from django.db.models import Model
from django.db.models.signals import m2m_changed


class MaintainedModelCoordinator:
    """
    This class loosely mimmics a "connection" in a Django "database instrumentation" design.  But instead of providing a
    way to apply custom wrappers to database queries, it provides a way to tell MaintainedModel how it should behave
    (i.e. its running mode based on "context") during calls to save, delete, and m2m_propagation_handler.  It does this
    by providing a context manager.

    There are 5 running modes that determine when and if autoupdates should occur:
        always: (the default, meaning both lazy and immediate)
        lazy: Auto-updates occuras query results are iterated over (see MaintainedModel.from_db()).
        immediate: Auto-updates occur immediately upon record creation (e.g. via calls to save()).
        deferred: Auto-updates are buffered upon save() and occur when the last nested context has been exited (see
            MaintainedModel.deferred).
        disabled: No buffering or autoupdates are performed at all (see MaintainedModel.disabled).

    Attributes:
        None
    """

    def __init__(self, auto_update_mode=None, **kwargs):
        if auto_update_mode is None:
            auto_update_mode = "always"
        self.auto_update_mode = auto_update_mode
        if auto_update_mode == "always":
            # Updates both on save and on query.  (Deferred will update, but later.)
            self.lazy_updates = True
            self.immediate_updates = True
            self.buffering = False
        elif auto_update_mode == "lazy":
            self.lazy_updates = True
            self.immediate_updates = False
            self.buffering = False
        elif auto_update_mode == "immediate":
            self.lazy_updates = False
            self.immediate_updates = True
            self.buffering = False
        elif auto_update_mode == "deferred":
            self.lazy_updates = False
            self.immediate_updates = False
            self.buffering = True
        elif auto_update_mode == "disabled":
            self.lazy_updates = False
            self.immediate_updates = False
            self.buffering = False
        else:
            raise ValueError(
                f"Invalid auto_update_mode: [{auto_update_mode}].  Valid values are: [always, lazy, immediate, "
                "deferred, and disabled]."
            )

        # This tracks whether the underlying modes (autoupdates and buffering) have been overridden or not, e.g. by a
        # parent context.  This is only used to override an immediate or lazy mode to a deferred mode.  disabled cannot
        # be overridden.
        self.overridden = False

        # These allow the user to turn on or off specific groups of auto-updates.
        label_filters = kwargs.pop("label_filters", [])
        self.default_label_filters = sorted(label_filters)

        filter_in = kwargs.pop("filter_in", True)
        self.default_filter_in = filter_in

        self.nondefault_filtering_exists = not (
            (label_filters is None or len(label_filters) == 0) and filter_in is True
        )

        # This is for buffering a large quantity of auto-updates in order to get speed improvements during loading
        self.update_buffer = []

    def __str__(self):
        return self.auto_update_mode

    def _defer_override(self):
        if self.auto_update_mode in ["always", "immediate", "lazy"]:
            print(f"Deferring {self.auto_update_mode} coordinator")
            self.auto_update_mode = "deferred"
            self.overridden = True
            self.lazy_updates = False
            self.immediate_updates = False
            self.buffering = True
        else:
            raise ValueError(
                f"Cannot set a defer override of a [{self.auto_update_mode}] mode MaintainedModelCoordinator."
            )

    def _disable_override(self):
        current_mode = self.get_mode()
        print(f"Disabling {current_mode} coordinator.")
        self.auto_update_mode = "disabled"
        self.overridden = True
        self.lazy_updates = False
        self.immediate_updates = False
        self.buffering = False

    def get_mode(self):
        return self.auto_update_mode

    def are_immediate_updates_enabled(self):
        return self.immediate_updates

    def are_lazy_updates_enabled(self):
        return self.lazy_updates

    def are_autoupdates_enabled(self):
        return self.immediate_updates or self.lazy_updates

    def buffer_size(self, generation=None, label_filters=None, filter_in=None):
        """
        Returns the number of buffered records that contain at least 1 decorated function matching the filter criteria
        (generation and label).
        """
        if label_filters is None:
            label_filters = []
        if filter_in is None:
            filter_in = True
        cnt = 0
        for buffered_item in self.update_buffer:
            updaters_list = buffered_item._filter_updaters(
                buffered_item.get_my_updaters(),
                generation=generation,
                label_filters=label_filters,
                filter_in=filter_in,
            )
            cnt += 1 if len(updaters_list) else 0
        return cnt

    def clear_update_buffer(self, generation=None, label_filters=None, filter_in=None):
        """
        Clears buffered auto-updates.  Use after having performed buffered updates to prevent unintended auto-updates.
        This method is called automatically during the execution of mass autoupdates.

        If a generation is provided (see the generation argument of the MaintainedModel.setter decorator), only
        buffered auto-updates labeled with that generation are cleared.  Note that each record can have multiple auto-
        update fields and thus multiple generation values.  Only the max generation (a leaf) is used in this check
        because it is assumed leaves are updated first during a mass update and that an auto-update updates every
        maintained field.

        If label_filters is supplied, only buffered auto-updates whose "update_label" is in the label_filters are
        cleared.
        Note that each record can have multiple auto-update fields and thus multiple update_label values.  Any label
        match will mark this buffered auto-update for removal.

        Note that if both generation and label_filters are supplied, only buffered auto-updates that meet both
        conditions are cleared.
        """
        if filter_in is None:
            filter_in = True
        if label_filters is None:
            label_filters = (
                []
            )  # Clear everything by default, regardless of default filters
            filter_in = True
        if generation is None and (label_filters is None or len(label_filters) == 0):
            self.update_buffer = []
            return

        new_buffer = []
        gen_warns = 0
        for buffered_item in self.update_buffer:
            # Buffered items are entire model objects.  We are going to filter model objects when they DO match the
            # filtering criteria.  A model object matches the filtering criteria based on whether ANY of its updaters
            # (fields to be updated specified by the decorators on the methods that produce their values) match the
            # filtering criteria.  If the model object DOES NOT have a field that meets the label filtering criteria,
            # it should remain in the buffer.  For example, if there are 2 fields that are auto-updated in the buffered
            # model object, and one's decorator has a "name" label and the other has an "fcirc_calc" label, and the
            # supplied label_filters is ["name"] and filter_in is True, then the matching updater WILL be returned by
            # this filter operation and the buffered item will be left out of the new_buffer.  If a model object in the
            # buffer does NOT have the "name" label in any of its updaters, it will be added to the new_buffer.
            matching_updaters = buffered_item._filter_updaters(
                buffered_item.get_my_updaters(),
                generation=generation,
                label_filters=label_filters,
                filter_in=filter_in,
            )

            max_gen = 0
            # We should issue a warning if the remaining updaters left in the buffer contain a greater generation,
            # because updates and buffer clears should happen from leaf to root.  And we should only check those which
            # have a target label.
            if generation is not None:
                max_gen = buffered_item.get_max_generation(
                    matching_updaters, label_filters, filter_in
                )

            # If the buffered item didn't have any updaters that met the filtering criteria, keep it in the buffer
            if len(matching_updaters) == 0:
                new_buffer.append(buffered_item)
                # There are no matching filters among the updaters of the buffered_item, but the max generation MUST be
                # auto-updated first in order for breadth-first mass autoupdates to happen in the proper order, so if
                # we're keeping a generation higher than the current filter generation being cleared, this is a problem.
                if generation is not None and max_gen > generation:
                    gen_warns += 1

        if gen_warns > 0:
            label_str = ""
            if label_filters is not None and len(label_filters) > 0:
                label_str = f"with labels: [{', '.join(label_filters)}] "
            print(
                f"WARNING: {gen_warns} records {label_str}in the buffer are younger than the generation supplied: "
                f"{generation}.  Generations should be cleared in order from leaf (largest generation number) to root "
                "(0)."
            )

        # populate the buffer with what's left
        self.update_buffer = new_buffer

    def _peek_update_buffer(self, index=0):
        return self.update_buffer[index]

    def buffer_update(self, mdl_obj):
        """
        This is called when MaintainedModel.save (or delete) is called (if immediate_updates is False), so that
        maintained fields can be updated after loading code finishes (by calling the global method:
        perform_buffered_updates).

        It will only buffer the model object if the filters attached to it (originally from a coordinator (possibly a
        child coordinator with different labels) match any of the labels in the class's autoupdate fields, set in their
        decorators.
        """

        # See if this class contains a field with a matching label (if a populated label_filters array was supplied)
        if (
            mdl_obj.label_filters is not None
            and len(mdl_obj.label_filters) > 0
            and not mdl_obj.updater_list_has_matching_labels(
                mdl_obj.get_my_updaters(),
                mdl_obj.label_filters,
                mdl_obj.filter_in,
            )
        ):
            # Do not buffer - nothing to update
            return

        # Do not buffer if it's already buffered.  Note, this class isn't designed to support auto-updates in a
        # sepecific order.  All auto-update functions should use non-auto-update fields.
        if self.buffering:
            if mdl_obj not in self.update_buffer:
                self.update_buffer.append(mdl_obj)
            else:
                # This allows the same object to be updated more than once (in the order encountered) if the fields to
                # be auto-updated in each instance, differ.  This can cause redundant updates (e.g. when a field matches
                # the filters in both cases), but given the possibility that update order may depend on the update of
                # related records, it's better to be on the safe side and do each auto-update, so...
                # If this is the same object but a different set of fields will be updated...
                # NOTE: Django model object equivalence (obj1 == obj2) compares primary key values, so even though the
                # object attributes "filter_in" and "label_filters" may differ in the object, we still have to
                # explicitly check them.
                for same_obj in [bo for bo in self.update_buffer if bo == mdl_obj]:
                    if (
                        same_obj.filter_in != mdl_obj.filter_in
                        or same_obj.label_filters != same_obj.label_filters
                    ):
                        self.update_buffer.append(mdl_obj)
                        break

    # Added transaction.atomic, because even after catching an intentional AutoUpdateFailed in test
    # DataRepo.tests.models.test_infusate.MaintainedModelImmediateTests.test_error_when_buffer_not_clear and ending the
    # test successfully, the django post test teardown code was re-encountering the exception and I'm not entirely sure
    # why.  It probably has to do with the context manager code.  The entire trace had no reference to any code in this
    # repo.  Adding transaction.atomic here prevents that exception from happening...
    @transaction.atomic
    def perform_buffered_updates(self, label_filters=None, filter_in=None):
        """
        Performs a mass update of records in the buffer in a depth-first fashion without repeated updates to the same
        record over and over.  It goes through the buffer in the order added and triggers each record's DFS updates,
        which returns the signatures of every updated record.  Those updates are maintained through the traversal of
        the entire buffer and checked before each update, thereby preventing repeated updates.  If a record has already
        been updated, the records it triggers updates to are not propagated either.  The goal is to trigger the updates
        in the order they were designed to follow governed by the parent/child links created in each decorator.

        Note that this can fail if a record is changed and then its child (who triggers its parent) is changed (each
        being added to the buffer during a mass auto-update).  This however is not expected to happen, as mass auto-
        update is used for loading, which if done right, doesn't change child records after parent records have been
        added.

        WARNING: label_filters and filter_in should only be supplied if you know what you are doing.  Every model
        object buffered for autoupdate saved its filtering criteria that were in effect when it was buffered and that
        filtering criteria will be applied to selectively update only the fields matching the filtering criteria as
        applied to each field's "update_label" in its method's decorator.
        """
        # The default is to use the filters saved with the buffered model objects
        # This is so that a parent deferred coordinator who receives a child coordinor's buffer, can do those updates,
        # which may only be for certain fields whose update dicts have the given labels.
        use_object_label_filters = True
        # If filters were explicitly supplied
        if label_filters is not None:
            use_object_label_filters = False
            if self.filter_in is None:
                filter_in = self.default_filter_in
        # Else - the filters will be set at each iteration of the buffered item loop below

        if self.are_autoupdates_enabled():
            raise StaleAutoupdateMode()

        if len(self.update_buffer) == 0:
            return

        # Track what's been updated to prevent repeated updates triggered by multiple child updates
        updated = []
        new_buffer = []
        no_filters = label_filters is None or len(label_filters) == 0

        # For each record in the buffer
        buffer_item: MaintainedModel
        for buffer_item in self.update_buffer:
            updater_dicts = buffer_item.get_my_updaters()

            if use_object_label_filters:
                label_filters = self.label_filters
                filter_in = self.filter_in
                if label_filters is None:
                    label_filters = self.default_label_filters
                    filter_in = self.default_filter_in

            # Track updated records to avoid repeated updates
            key = f"{buffer_item.__class__.__name__}.{buffer_item.pk}"

            # Try to perform the update. It could fail if the affected record was deleted
            try:
                if key not in updated and (
                    no_filters
                    or buffer_item.updater_list_has_matching_labels(
                        updater_dicts, label_filters, filter_in
                    )
                ):
                    # Saving the record while mass_updates is True, causes auto-updates of every field
                    # included among the model's decorated functions.  It does not only update the fields indicated in
                    # decorators that contain the labels indicated in the label_filters.  The filters are only used to
                    # decide which records should be updated.  Currently, this is not an issue because we only have 1
                    # update_label in use.  And if/when we add another label, it will only end up causing extra
                    # repeated updates of the same record.
                    buffer_item.save(mass_updates=True)

                    # Propagate the changes (if necessary), keeping track of what is updated and what's not.
                    # Note: all the manual changes are assumed to have been made already, so auto-updates only need to
                    # be issued once per record
                    updated = buffer_item.call_dfs_related_updaters(
                        updated=updated,
                        mass_updates=True,
                        label_filters=label_filters,
                        filter_in=filter_in,
                    )

                elif key not in updated and buffer_item not in new_buffer:
                    new_buffer.append(buffer_item)

            except Exception as e:
                # Any exception can be raised from the derived model's decorated updater function
                raise AutoUpdateFailed(buffer_item, e, updater_dicts)

        # Eliminate the updated items from the buffer
        self.update_buffer = new_buffer


class MaintainedModel(Model):
    """
    This class maintains database field values for a django.models.Model class whose values can be derived using a
    function.  If a record changes, the decorated function/class is used to update the field value.  It can also
    propagate changes of records in linked models.  Every function in the derived class decorated with the
    `@MaintainedModel.setter` decorator (defined above, outside this class) will be called and the associated field
    will be updated.  Only methods that take no arguments are supported.  This class extends the class's save and
    delete methods and uses m2m_changed signals as triggers for the updates.
    """

    # Thread-safe mutable class attributes.  Thread data is initialized via _check_set_coordinator_thread_data
    data = local()

    # Track whether the fields from the decorators have been validated
    # This is only ever initialized once, the first time every derived class is ever instantiated, so it's loosely an
    # immutable class attribute (though technically mutable), as it would only ever change if new models are added and
    # is only used to decide when a derived class's usage of MaintainedModel is invalid
    maintained_model_initialized: Dict[str, bool] = {}

    # Track the metadata recorded by each derived model class's setter and relation decorators
    # Similar to maintained_model_initialized, this is loosely an immutable class attribute (though technically
    # mutable), as it is only ever set when the setter and relation decorators are created, which only ever happens once
    # This does not need to be thread-safe because it will not ever change after all the decorators have been registered
    updater_list: Dict[str, List] = defaultdict(list)

    # A dict of class name keys and class values used by get_classes needed for rebuild_maintained_fields. This is
    # initialized via MaintainedModel's decorators as a way to avoid needing the module path of all the models as was
    # formerly done.
    model_classes: Dict[str, Model] = defaultdict(Model)

    # A dict saving the package (referenced elsewhere as "model_path") that holds each model so that it's class can be
    # retrieved when needed.
    model_packages: Dict[str, str] = defaultdict(str)

    def __init__(self, *args, **kwargs):
        """
        This over-ride of the constructor is to prevent developers from explicitly setting values for automatically
        maintained fields.  It also performs a one-time validation check of the updater_dicts.
        """
        self._maintained_model_setup(**kwargs)
        super().__init__(*args, **kwargs)

    def _maintained_model_setup(self, **kwargs):
        """
        This method exists because if a developer calls Model.objects.create(), __init__ is not called.  This method is
        called both from __init__() and save().
        """

        # Make sure the class has been fully initialized
        self._check_set_coordinator_thread_data()

        # The coordinator keeps track of the running mode, buffer and filters in use
        coordinator = self.get_coordinator()

        # Members added by MaintainedModel - the coordinator values are set when the coordinator is instantiated.  They
        # are recorded in the object so that during perform_buffered_updates, we will know what field(s) to update when
        # it processes the object.  An update would not have been buffered if the model did not contain a maintained
        # field matching the label filtering.  And label filtering can change during the buffering process (e.g.
        # different loaders), which is why this is necessary.
        self.label_filters = coordinator.default_label_filters
        self.filter_in = coordinator.default_filter_in

        class_name = self.__class__.__name__

        # Register the class with the coordinator if not already registered
        if class_name not in MaintainedModel.model_classes.keys():
            print(
                f"Registering class {class_name} as a MaintainedModel from _maintained_model_setup: {type(self)}"
            )
            MaintainedModel.model_classes[class_name] = type(self)
            MaintainedModel.model_packages[class_name] = type(self).__module__

        for updater_dict in MaintainedModel.updater_list[class_name]:
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
            if decorator_signature not in MaintainedModel.maintained_model_initialized:
                if settings.DEBUG:
                    print(
                        f"Validating {self.__class__.__name__} updater: {updater_dict}"
                    )

                MaintainedModel.maintained_model_initialized[decorator_signature] = True
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
                            self.m2m_propagation_handler,
                            sender=through_model,
                        )
                    # m2m_changed.connect(toppings_changed, sender=Pizza.toppings.through)
                except AttributeError as ae:
                    if "has no attribute 'many_to_many'" not in str(ae):
                        raise ae
                    # Else - no propagation handler needed

    def save(self, *args, **kwargs):
        """
        This is an extension of the derived model's save method that is being used here to automatically update
        maintained fields.
        """
        # via_query: Whether this is coming from the from_db method or not (implying no record change) - default False
        via_query = kwargs.pop("via_query", False)
        # The following custom arguments are used internally.  Do not supply unless you know what you're doing.
        # mass_updates: Whether auto-updating buffered model objects - default False
        mass_updates = kwargs.pop("mass_updates", False)
        # propagate: Whether to propagate updates to related model objects - default True
        propagate = kwargs.pop("propagate", not mass_updates and not via_query)
        # fields_to_autoupdate: List of fields to auto-update. - default None = update all maintained fields
        fields_to_autoupdate = kwargs.pop("fields_to_autoupdate", None)

        # If the object is None, then what has happened is, there was a call to create an object off of the class.  That
        # means that __init__ was not called, so we are going to handle the initialization of MaintainedModel (including
        # the setting of the coordinator and the disallowing of setting values for maintained fields with a call to
        # _maintained_model_setup).
        if self is None:
            # The coordinator keeps track of the running mode, buffer and filters in use
            self._maintained_model_setup(**kwargs)

        # Retrieve the current coordinator
        coordinator = self.get_coordinator()

        # Record whether/when we have made the super-save call, so that we don't do it twice when the developer's code
        # is calling save just to trigger an auto-update.
        # Note, super_save_called will already have been initialized if the object was saved and buffered and mass auto-
        # update is being performed.  Save on an object can be called a second time from the developer's code
        # (presumably after having made subsequent changes), so to support that we must reset to False, but we don't
        # want to do it during a mass auto-update.
        if not hasattr(self, "super_save_called") or not mass_updates:
            self.super_save_called = False

        # If auto-updates are turned on, a cascade of updates to linked models will occur, but if they are turned off,
        # the update will be buffered, to be manually triggered later (e.g. upon completion of loading), which
        # mitigates repeated updates to the same record
        if not coordinator.are_autoupdates_enabled():
            # If autoupdates are happening (and it's not a mass-autoupdate (assumed because mass_updates can only be
            # true if immediate_updates is False)), set the label filters based on the currently set global conditions
            # so that only fields matching the filters will be updated.  An explicit setting of the label_filters in the
            # coordinator overrides the update_label of the decorators applied in the model.  This is so a specific mass
            # update can be manually achieved via a targeted script.  If there is not label_filters set in the
            # coordinator, it falls back to the update_labels belonging to the model class's decorators, and propagation
            # will only follow those paths.
            if (
                coordinator.default_label_filters is None
                or len(coordinator.default_label_filters) == 0
            ):
                self.label_filters = self.get_my_update_labels()
                self.filter_in = True
            else:
                self.label_filters = coordinator.default_label_filters
                self.filter_in = coordinator.default_filter_in

            if not mass_updates:
                # Set the changed value triggering this update
                super().save(*args, **kwargs)
                self.super_save_called = True

                # The global label filters applied above will be remembered during mass autoupdate of the buffer
                coordinator.buffer_update(self)

                return
        # Otherwise, we are performing a mass auto-update and want to update using the previously set filter conditions

        # Calling super.save (if necessary) so that the call to update_decorated_fields can traverse reverse
        # relations without a ValueError exception that is a new behavior/constraint as of Django 4.2.  But, if we got
        # here due to a call to Model.objects.create(), (which calls super.save() from deep in the Django code (and that
        # sets the primary key)), so it means that if we were to call it here again, it would cause a unique constraint-
        # related exception.  That's why we do not call it here if the primary key is set - so we can avoid those
        # exceptions.  Note, self.pk is None if the record was deleted after being buffered and we encounter it during
        # mass update.
        if self.pk is None and mass_updates is False:
            super().save(*args, **kwargs)
            self.super_save_called = True
        # Note, we should not save during mass autoupdate because while the object was in the buffer, it could have been
        # deleted from the database and saving it would unintentionally re-add it to the database.

        if (
            # If this was triggered from a query and lazy mode is off
            (via_query and not coordinator.are_lazy_updates_enabled())
            # If this was not triggered from a query and lazy mode is on (and immediate mode is off)
            or (
                not via_query
                and coordinator.are_lazy_updates_enabled()
                and not coordinator.are_immediate_updates_enabled()
            )
        ):
            # Remove the super_save_called if it was added (see the delattr comment near the bottom)
            if hasattr(self, "super_save_called"):
                delattr(self, "super_save_called")
            # We don't want to perform autoupdates when lazy updates is False and via_query is True, so return
            return

        # Update the fields that change due to the the triggering change (if any)
        # This only executes either when immediate_updates or mass_updates is true - both cannot be true
        changed = self.update_decorated_fields(fields_to_autoupdate)

        # If the autoupdate changed a value in a maintained field or (super().save() has not been called yet and this
        # was not a save call from a query (i.e. from_db)), we need to save the changed result (or follow through on the
        # external code's call to save).
        # This either saves both explicit changes and auto-update changes (when immediate_updates is true) or it only
        # saves the auto-updated values (when mass_updates is true)
        if changed is True or (self.super_save_called is False and not via_query):
            if self.super_save_called or mass_updates is True:
                if mass_updates is True:
                    # Intentionally trigger an exception if the buffer is stale (i.e. if the record was deleted)
                    self.exists_in_db(raise_exception=True)
                # This is a subsequent call to save due to the auto-update, so we don't want to use the original
                # arguments (which may direct save that it needs to do an insert).  If you do supply arguments in this
                # case, you can end up with an IntegrityError due to unique constraints from the ID being the same.
                super().save()
            else:
                super().save(*args, **kwargs)

        # If the developer wants to make more changes to this object and call save again, we need to remove the
        # super_save_called attribute.  This will happen when autoupdate mode is immediate or if deferred (but when
        # deferred, only)
        delattr(self, "super_save_called")

        # We don't need to check mass_updates, because propagating changes during buffered updates is handled elsewhere
        # to mitigate repeated updates of the same related record.
        # Only propagate in immediate mode, not lazy.  In lazy updates, no data in the record is changing other than
        # maintained fields, and updates only need to propagate if not-maintained fields have changed, because
        # propagation is intended to only trigger when other values depend on the values in the triggering record.  And
        # since updater methods SHOULD NOT rely on maintained fields, there is no change in a query that should affect
        # other maintained fields.
        if coordinator.are_immediate_updates_enabled() and propagate:
            # Percolate (non-maintained field) record changes to the related models so they can change their maintained
            # fields whose values are dependent on this record's non-maintained fields
            self.call_dfs_related_updaters(
                label_filters=self.get_my_update_labels(), filter_in=True
            )

    def delete(self, *args, **kwargs):
        """
        This is an extension of the derived model's delete method that is being used here to automatically update
        maintained fields.
        """
        # Custom argument: propagate - Whether to propagate updates to related model objects - default True
        # Used internally. Do not supply unless you know what you're doing.
        propagate = kwargs.pop("propagate", True)
        # Custom argument: mass_updates - Whether auto-updating buffered model objects - default False
        # Used internally. Do not supply unless you know what you're doing.
        mass_updates = kwargs.pop("mass_updates", False)

        self_sig = self.get_record_signature()

        # Retrieve the current coordinator
        coordinator = self.get_coordinator()

        # If auto-updates are turned on, a cascade of updates to linked models will occur, but if they are turned off,
        # the update will be buffered, to be manually triggered later (e.g. upon completion of loading), which
        # mitigates repeated updates to the same record
        # mass_updates is checked for consistency, but perform_buffered_updates does not call delete()
        if (
            coordinator.are_immediate_updates_enabled() is False
            and mass_updates is False
        ):
            # If autoupdates are happening (and it's not a mass-autoupdate), set the label filters based on the
            # currently set global conditions so that only fields matching the filters will be updated.  An explicit
            # setting of the label_filters in the coordinator overrides the update_label of the decorators applied in
            # the model.  This is so a specific mass update can be manually achieved via a targeted script.  If there is
            # not label_filters set in the coordinator, it falls back to the update_labels belonging to the model
            # class's decorators, and propagation will only follow those paths.
            if (
                coordinator.default_label_filters is None
                or len(coordinator.default_label_filters) == 0
            ):
                self.label_filters = self.get_my_update_labels()
                self.filter_in = True
            else:
                self.label_filters = coordinator.default_label_filters
                self.filter_in = coordinator.default_filter_in

            if coordinator.buffering:
                parents = self.get_parent_instances()
                for parent_inst in parents:
                    coordinator.buffer_update(parent_inst)

            # Delete the record triggering this update after having buffered the parents (because the parent could be a
            # M:M relation and cannot be retrieved if this record has already been deleted.
            retval = super().delete(*args, **kwargs)  # Call the "real" delete() method.

            return retval
        elif coordinator.are_immediate_updates_enabled():
            # If autoupdates are happening (and it's not a mass-autoupdate (assumed because mass_updates can only be
            # true if immediate_updates is False)), set the label filters based on the currently set global conditions
            # so that only fields matching the filters will be updated.  An explicit setting of the label_filters in the
            # coordinator overrides the update_label of the decorators applied in the model.  This is so a specific mass
            # update can be manually achieved via a targeted script.  If there is not label_filters set in the
            # coordinator, it falls back to the update_labels belonging to the model class's decorators, and propagation
            # will only follow those paths.
            if (
                coordinator.default_label_filters is None
                or len(coordinator.default_label_filters) == 0
            ):
                self.label_filters = self.get_my_update_labels()
                self.filter_in = True
            else:
                self.label_filters = coordinator.default_label_filters
                self.filter_in = coordinator.default_filter_in
        # Otherwise, we are performing a mass auto-update and want to update the previously set filter conditions

        # Delete the record triggering this update.  In the event we were buffering, we had to do that above and return.
        # Otherwise, we do it here.
        retval = super().delete(*args, **kwargs)  # Call the "real" delete() method.

        if coordinator.are_immediate_updates_enabled() and propagate:
            # Percolate changes up to the parents (if any) and mark the deleted record as updated.  Here, we ignore any
            # label_filters set by the coordinator, because that's only for mass autoupdates, and supply the
            # update_labels explicitly present in the model for the label_filters argument (and defaulting filter_in to
            # True).
            self.call_dfs_related_updaters(
                updated=[self_sig],
                label_filters=self.get_my_update_labels(),
                filter_in=True,
            )

        return retval

    @classmethod
    def from_db(cls, *args, **kwargs):
        """
        This is an extension of Model.from_db.  Model.from_db takes arguments: db, field_names, values.  It is used to
        convert SQL query results into Model objects.  This over-ride uses this opportunity to perform lazy-auto-updates
        of Model fields.  Note, it will not change the query results in terms of records.  I.e. if the maintained field
        value is stale (e.g. it should be "x", but instead is null, and the query is "WHERE field = 'x'", the record
        will NOT be updated because it would not have been returned by the SQL query.  This lazy-auto-update will occur
        when a QuerySet is iterated over.  That's when `from_db` is called - at each iteration.

        This method checks the field_names for the presence of maintained fields, and if the corresponding value is
        None, it will trigger an auto-update and set the new value for the superclass method's model object creation.

        Note, one down-side of this implementation of lazy auto-updates is that if the auto-update results in the value
        being null/None, the code to auto-update the field will always execute (a waste of effort).

        This will not lazy-update DEFERRED field values.
        """
        # Instantiate the model object
        rec = super().from_db(*args, **kwargs)

        # If autoupdates are not enabled (i.e. we're not in "lazy" mode)
        if not cls.get_coordinator().are_lazy_updates_enabled():
            return rec

        # Get the field names
        queryset_fields = set(args[1])  # field_names

        # Intersect the queryset fields with the maintained fields
        common_fields = set(cls.get_my_update_fields()).intersection(queryset_fields)

        # Look for maintained field values that are None
        lazy_update_fields = [fld for fld in common_fields if getattr(rec, fld) is None]

        # If any maintained fields are to be lazy-updated
        if len(lazy_update_fields) > 0:
            cs = ", "
            flds_str = (
                ("s: " + cls.__name__ + ".{" + cs.join(lazy_update_fields) + "}")
                if len(lazy_update_fields) > 1
                else ": " + cls.__name__ + "." + lazy_update_fields[0]
            )
            print(f"Triggering lazy auto-update of field{flds_str}")
            # Trigger an auto-update
            rec.save(  # pylint: disable=unexpected-keyword-arg
                fields_to_autoupdate=lazy_update_fields, via_query=True
            )

        return rec

    @staticmethod
    def relation(
        generation, parent_field_name=None, child_field_names=[], update_label=None
    ):
        """
        Use this decorator to add connections between classes when it does not have any maintained fields.  For example,
        if you only want to maintain 1 field in 1 class, but you want changes in a related class to trigger updates to
        that field, apply this decorator to the class and set either the parent_field_name and/or the child_field_names
        to trigger those updates of the maintained fields in that related model class.

        Refer to the doc string of the MaintainedModel.setter decorator below for a description of the parameters.

        Example:

        class ModelA(MaintainedModel):
            ...
        @relation(
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
        will trigger auto-updated to maintained fields (not shown) in its child model records (first) and it's parent
        modelA records.  Likewise, it will pass on triggered updates from those classes if they are set to pass on
        changes to modelB though the parent/chold fields in their decorators.
        """
        # Validate args
        if generation != 0:
            # Make sure internal nodes have parent fields
            if parent_field_name is None:
                raise ConditionallyRequiredArgumentError(
                    "parent_field is required if generation is not 0."
                )
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

            class_name = cls.__name__

            # Register the class (and the module) with the coordinator if not already registered
            if class_name not in MaintainedModel.model_classes.keys():
                print(
                    f"Registering class {class_name} as a MaintainedModel from the relation decorator: {cls}"
                )
                MaintainedModel.model_classes[class_name] = cls
                MaintainedModel.model_packages[class_name] = cls.__module__

            # Register the updater with the coordinator
            MaintainedModel.updater_list[class_name].append(func_dict)

            # No way to ensure supplied fields exist because the models aren't actually loaded yet, so while that would
            # be nice to handle here, it will have to be handled in MaintanedModel when objects are created

            # Provide some debug feedback
            if settings.DEBUG:
                msg = f"Added MaintainedModel.relation decorator {class_name} to update"
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

    @staticmethod
    def setter(
        generation,
        update_field_name=None,
        parent_field_name=None,
        update_label=None,
        child_field_names=[],
    ):
        """
        This is a decorator factory for functions in a Model class that are identified to be used to update a supplied
        field and field of any linked parent/child record (for when the record is changed).  This function returns a
        decorator that takes the decorated function.  That function should not use the value of another maintained field
        in its calculation because the order of update is not guaranteed to occur in a favorable series.  It should
        return a value compatible with the field type supplied.

        These decorated functions are identified by the MaintainedModel class, whose save and delete methods extend
        the parent model and call the decorated functions to update field supplied to the factory function.  It also
        propagates the updates to the linked dependent model's save methods (if the parent and/or child field name is
        supplied), the assumption being that a change to "this" record's maintained field necessitates a change to
        another maintained field in the linked parent record.  Parent and child field names should only be supplied if a
        change to "this" record means that related foields in parent/child records will need to be recomputed.  There is
        no need to supply parent/child field names if that is not the case.

        The generation input is an integer indicating the hierarchy level.  E.g. if there is no parent, `generation`
        should be 0.  Each subsequence generation should increment generation.  It is used to populate update_buffer
        when immediate_updates is False, so that mass updates can be triggered after all data is loaded.

        Note that a class can have multiple fields to update and that those updates (according to their decorators) can
        trigger subsequent updates in different "parent"/"child" records.  If multiple update fields trigger updates to
        different parents, they are triggered in a depth-first fashion.  Child records are updated first, then parents.
        If a child links back to a parent, already-updated records prevent repeated/looped updates.  However, this only
        becomes relevant when the global variable `immediate_updates` is False, mass database changes are made
        (buffering the auto-updates), and then auto-updates are explicitly triggered.

        Note, if there are many decorated methods updating different fields, and all of the "parent"/"child" fields are
        the same, only 1 of those decorators needs to set a parent field.
        """

        if update_field_name is None and (
            parent_field_name is None and generation != 0
        ):
            raise ConditionallyRequiredArgumentError(
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

            # Try to register the model class.  If this fails, fallback methods will be used when it is needed later.
            if class_name not in MaintainedModel.model_packages.keys():
                models_path = (
                    MaintainedModel.get_model_package_name_from_member_function(fn)
                )
                # Register the class with the coordinator if not already registered
                if models_path is not None:
                    MaintainedModel.model_packages[class_name] = models_path

            # No way to ensure supplied fields exist because the models aren't actually loaded yet, so while that would
            # be nice to handle here, it will have to be handled in MaintanedModel when objects are created

            # Add this info to our global updater_list
            MaintainedModel.updater_list[class_name].append(func_dict)
            # It would be nice if we could register the class here, but getting the surrounding class from the function
            # is tricky and fragile.  The class will be registered when its first instance is created.

            # Provide some debug feedback
            if settings.DEBUG:
                msg = f"Added MaintainedModel.setter decorator to function {fn.__qualname__} to"
                if update_field_name is not None:
                    msg += f" maintain {class_name}.{update_field_name}"
                    if parent_field_name is not None or len(child_field_names) > 0:
                        msg += " and"
                if parent_field_name is not None:
                    msg += (
                        f" trigger updates to parent: {class_name}."
                        f"{parent_field_name}"
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

    def exists_in_db(self, raise_exception=False):
        """
        Asserts that the object hasn't been deleted from the database while it was in the buffer.  This can
        intentionally raise a DoesNotExist exception (e.g. during perform_buffered_updates) so that we don't end up re-
        saving a deleted object.
        https://stackoverflow.com/a/16613258/2057516
        """
        try:
            type(self).objects.get(pk__exact=self.pk)
        except Exception as e:
            if raise_exception is True:
                raise e
            if issubclass(type(e), ObjectDoesNotExist):
                return False
            raise e
        return True

    @staticmethod
    def get_model_package_name_from_member_function(fn):
        """
        This will TRY to obtain the package name (aka "models_path") from the supplied model's member function.  It
        does so by using the function's class attribute '__globals__' dict to access its __package__ (a string
        showing the path to where the model's class is defined).  If it fails in any particular version of Django,
        the fallback is to simply require the package name wherever it is needed.
        """
        try:
            models_path = fn.__globals__["__package__"]
            return models_path
        except Exception as e:
            print(
                "WARNING: MaintainedModel was unable to retrieve the model class's package from its member function: "
                f"{fn.__qualname__}.  MaintainedModel (or MaintainedModelCoordinator) functions which require the "
                "model class type before an instance of the model class has been created, will need to be supplied a "
                f"models_path, e.g. 'AppName.models'.  The error encoutnered was: {e.__class__.__name__}: {e}"
            )
        return None

    @classmethod
    def m2m_propagation_handler(cls, **kwargs):
        """
        Additions to M:M related models do not require a .save() to be called afterwards, thus additions like:
            peakgroup.compounds.add(cmpd)
        do not propagate a change to MSRunSample as is necessary for automatic field maintenance, expressly because
        peakgroup.save() is not called.  To deal with this, and trigger the necessary automatic updates of maintained
        fields, an m2m_changed signal is attached to all M:M fields in MaintainedModel.__init__ to tell us when a
        MaintainedModel has an M:M field that has been added to.  That causes this method to be called, and from here
        we can propagate the changes.
        """
        obj: MaintainedModel = kwargs.pop("instance", None)
        act: str = kwargs.pop("action", "")

        # Retrieve the current coordinator
        coordinator = cls.get_coordinator()

        if (
            act.startswith("post_")
            and isinstance(obj, MaintainedModel)
            and coordinator.are_immediate_updates_enabled()
        ):
            # Percolate (non-maintained field) record changes to the related models so they can change their maintained
            # fields whose values are dependent on this record's non-maintained fields.  Here, we ignore any
            # label_filters set by the coordinator, because that's only for mass autoupdates, and supply the
            # update_labels explicitly present in the model for the label_filters argument (and defaulting filter_in to
            # True).
            obj.call_dfs_related_updaters(
                label_filters=obj.get_my_update_labels(), filter_in=True
            )

    @classmethod
    def get_coordinator(cls) -> MaintainedModelCoordinator:
        return cls._get_current_coordinator()

    @classmethod
    def _get_current_coordinator(cls) -> MaintainedModelCoordinator:
        coordinator_stack = cls._get_coordinator_stack()
        if len(coordinator_stack) > 0:
            # Get the current coordinator
            current_coordinator = coordinator_stack[-1]
            # Call the last coordinator on the stack
            return current_coordinator
        else:
            return cls._get_default_coordinator()

    @classmethod
    def _get_default_coordinator(cls):
        cls._check_set_coordinator_thread_data()
        return cls.data.default_coordinator

    @classmethod
    def _get_coordinator_stack(cls):
        """
        Checks that the coodrinator thread data is initialized and returns the coordinator_stack list
        """
        cls._check_set_coordinator_thread_data()
        return cls.data.coordinator_stack

    @classmethod
    def _check_set_coordinator_thread_data(cls):
        """
        Make sure the thread has been fully initialized and initialize it if not
        Without this, you get an AttributeError: '_thread._local' object has no attribute 'coordinator_stack'
        from django.db.models.base's from_db class method when a model's DetailView is created.
        """
        if not hasattr(cls.data, "default_coordinator") or not hasattr(
            cls.data, "coordinator_stack"
        ):
            cls._reset_coordinators()

    @classmethod
    def _reset_coordinators(cls):
        """
        This clears out the coordinator stack so that any newly created MaintainedModel objects get the default
        coordinator, which is also reset to the default.  Added this method only for usage in testing.  Note, any
        previously created coordinators referenced by existing MaintainedModel objects will still have their
        coordinators, but if that running code is on the same thread and it queries the stack to pass its buffered
        model records up the stack, that will fail... but the prevailing theory is that that can't happen since we are
        using threading.local to store the stack.
        """
        cls.data.__setattr__("default_coordinator", MaintainedModelCoordinator())
        cls.data.__setattr__("coordinator_stack", [])

    @classmethod
    def _add_coordinator(cls, coordinator):
        """
        Only use in order to catch buffered items for testing.  Must be manually popped.
        """
        coordinator_stack = cls._get_coordinator_stack()
        coordinator_stack.append(coordinator)

    @classmethod
    def get_parent_deferred_coordinator(cls):
        """
        Return the parent coordinator from the stack whose mode is "deferred".
        This assumes that perform_buffered_updates is called inside a deferred_autoupdates block after the yielded
        code has finished its buffering, which is why it pops off the "current" coordinator.
        """
        # Create a copy of the list that contains the same objects - so that if you return a coordinator, it is one
        # that's in the coordinator_stack - so if you change it, you change the object in the stack.  This is what we
        # want, so that we can move items from the current coordinator's buffer to the parent coordinator's buffer.
        coordinator_stack = cls._get_coordinator_stack()
        parent_coordinators = coordinator_stack[:]
        try:
            parent_coordinators.pop()
        except IndexError:
            raise DeferredParentCoordinatorContextError()

        # Traverse the parent coordinators from immediate parent to distant parent.  Note, the stack doesn't include the
        # default_coordinator, which is assumed to not be mode "deferred".
        for coordinator in reversed(parent_coordinators):
            if coordinator.get_mode() == "deferred":
                return coordinator

        # Return None when there is no deferred parent (meaning that puffered autoupdates will be performed)
        return None

    @classmethod
    def is_parent_coordinator_disabled(cls):
        """
        Determine if any existing parent coordinator in the coordinator stack is disabled and change the current/new
        coordinator to disabled as well.
        This assumes that it is being called from inside custom_coordinator before the "current" coordinator has been
        pushed onto the coordinator_stack, which is why it doesn't pop the last coordinator off.
        """
        # Traverse the parent coordinators from immediate parent to distant parent.  Note, the stack doesn't include the
        # default_coordinator, which is assumed to not be mode "deferred".
        cls._check_set_coordinator_thread_data()
        default_coordinator = cls._get_default_coordinator()
        if default_coordinator.get_mode() == "disabled":
            return True
        coordinator_stack = cls._get_coordinator_stack()
        for coordinator in coordinator_stack:
            if coordinator.get_mode() == "disabled":
                return True

        # Return None when there is no deferred parent (meaning that puffered autoupdates will be performed)
        return False

    @classmethod
    @contextmanager
    def custom_coordinator(
        cls,
        coordinator,
        pre_mass_update_func=None,  # Only used for deferred coordinators
        post_mass_update_func=None,  # Only used for deferred coordinators
    ):
        """
        This method allows you to set a temporary coordinator using a context manager.  Under this context (using a with
        block), the supplied coordinator will be used instead of the default whenever a MaintainedModel object is
        instantiated.  It behaves differently based on the coordinator mode and will change the mode based on the
        hierarchy.  A disabled parent coordinator trumps deferred, immediate, and lazy.  A deferred coordinator trumps
        an immediate and lazy.  Deferred passes its buffer to the parent deferred coordinator.  These contexts can be
        nested.

        Use this method like this:
            deferred_filtered = MaintainedModelCoordinator(auto_update_mode="deferred")
            with MaintainedModel.custom_coordinator(deferred_filtered):
                do_things()
        """
        coordinator_stack = cls._get_coordinator_stack()
        # This assumes that the default_coordinator is not in mode "deferred"
        if len(coordinator_stack) == 0 and coordinator.buffer_size() > 0:
            raise UncleanBufferError()

        original_mode = coordinator.get_mode()
        effective_mode = original_mode
        default_coordinator = cls._get_default_coordinator()

        # If any parent context sets autoupdates to disabled, change the mode to disabled
        if (
            # If any parent coordinator is disabled, disable this one
            effective_mode != "disabled"
            and cls.is_parent_coordinator_disabled()
        ):
            effective_mode = "disabled"
            coordinator._disable_override()
        elif (
            # If the immediate parent coordinator is deferred, defer this one
            (effective_mode == "immediate" or effective_mode == "lazy")
            and (
                (
                    len(coordinator_stack) > 0
                    and coordinator_stack[-1].get_mode() == "deferred"
                )
                or (
                    len(coordinator_stack) == 0
                    and default_coordinator.get_mode() == "deferred"
                )
            )
        ):
            effective_mode = "deferred"
            coordinator._defer_override()

        coordinator_stack.append(coordinator)

        try:
            # This is all the code in the context
            # Any MaintainedModel object created in this context will get the last coordinator on the stack
            yield

            # If the above raised an exception, we will not get here...
            # If we are in fact in deferred mode, now is the time for the mass auto-update
            if effective_mode == "deferred":
                # Check if there exists a parent coordinator that is also deferred, because we only want to
                # perform buffered updates once we're in the last deferred context
                parent_deferred_coordinator = cls.get_parent_deferred_coordinator()
                # If there is a parent deferred coordinator
                if parent_deferred_coordinator is not None:
                    # Transfer the buffer to the next-to-last deferred coordinator
                    for buffered_item in coordinator.update_buffer:
                        parent_deferred_coordinator.buffer_update(buffered_item)
                else:
                    # Note, the pre/post mass update funcs are ignored if deferring updates to parents, so that
                    # must be specified in every decorator, as nested decorators will not bubble the functions
                    # up the coordinator stack.
                    if pre_mass_update_func is not None:
                        pre_mass_update_func()
                    coordinator.perform_buffered_updates()
                    if post_mass_update_func is not None:
                        post_mass_update_func()

        except Exception as e:
            # Empty the buffer just to be on the safe side.  This shouldn't technically be necessary since we are
            # popping it off the stack... but it guarantees clean usage, because the developer's code around the context
            # block still have a handle on the coordinator after it's popped off the stack.
            coordinator.clear_update_buffer()
            raise e
        finally:
            coordinator_stack.pop()

    @classmethod
    def defer_autoupdates(
        cls,
        label_filters=None,
        filter_in=True,
        disable_opt_names=None,
        pre_mass_update_func=None,
        post_mass_update_func=None,
    ):
        """
        Use this as a decorator to wrap a function to use a different coordinator and call mass auto-updates afterward.

        disable_opt_names - You can supply the name of any boolean options that should optionally disable autoupdates
        entirely (if any of them are True), e.g. "dry_run".

        Note, the pre/post mass update funcs are ignored if deferring updates to parents, so that must be specified in
        every decorator, as nested decorators will not bubble the functions up the coordinator stack.
        """

        # This takes the function being decorated
        def decorator(fn):
            # This wraps the function so we can apply a different coordinator
            def wrapper(*args, **kwargs):
                mode = "deferred"

                # If the arguments to the defer_autoupdates decorator included a disable_opt_names (e.g. ["dry_run"])
                if disable_opt_names and len(disable_opt_names) > 0:
                    # Check the value of each option and change the mode to "disabled" if *any* of them are True.
                    for disable_opt_name in disable_opt_names:
                        if (
                            disable_opt_name in kwargs.keys()
                            and kwargs[disable_opt_name]
                        ):
                            # This is if the option is in kwargs
                            mode = "disabled"
                            break
                        elif (
                            len(args) > 0
                            and hasattr(args[0], disable_opt_name)
                            and getattr(args[0], disable_opt_name)
                        ):
                            # This is if the option is an attribute in "self"
                            mode = "disabled"
                            break

                coordinator = MaintainedModelCoordinator(
                    auto_update_mode=mode,
                    label_filters=label_filters,
                    filter_in=filter_in,
                )

                with cls.custom_coordinator(
                    coordinator,
                    pre_mass_update_func=pre_mass_update_func,
                    post_mass_update_func=post_mass_update_func,
                ):
                    return fn(*args, **kwargs)

            return wrapper

        return decorator

    @classmethod
    def no_autoupdates(cls):
        """
        Use this as a decorator to wrap a function to completely disable all autoupdates and NOT perform a mass
        autoupdate afterward.
        """

        # This takes the function being decorated
        def decorator(fn):
            # This wraps the function so we can apply a different coordinator
            def wrapper(*args, **kwargs):
                coordinator = MaintainedModelCoordinator(auto_update_mode="disabled")
                with cls.custom_coordinator(coordinator):
                    return fn(*args, **kwargs)

            return wrapper

        return decorator

    @classmethod
    def get_all_updaters(cls):
        """
        Retrieve a flattened list of all updater dicts.
        Used by rebuild_maintained_fields.
        """
        all_updaters = []
        for class_name in cls.updater_list:
            all_updaters += cls.updater_list[class_name]
        return all_updaters

    @classmethod
    def _get_classes(
        cls,
        generation,
        label_filters,
        filter_in,
        models_path=None,
    ):
        """
        This method was made private, because it has no access to instance values for generation, label_filters, or
        filter_in.  Users are not expected to supply these.  All instance methods that internally call these must pass
        them.

        Retrieve a list of classes containing maintained fields that match the given criteria.
        Used by rebuild_maintained_fields and get_maintained_fields_query_dict.

        models_path is optional and must be a string like "DataRepo.models".  It's only required if called before any of
        the model classes have been instantiated and after the decorators have registered.
        """
        class_list = []
        for model_class_name in cls.updater_list.keys():
            if (
                len(
                    cls._filter_updaters(
                        updaters_list=cls.updater_list[model_class_name],
                        generation=generation,
                        label_filters=label_filters,
                        filter_in=filter_in,
                    )
                )
                > 0
            ):
                class_list.append(cls.get_model_class(model_class_name, models_path))
        return class_list

    @classmethod
    def get_model_class(cls, model_class_name, models_path=None):
        """
        models_path is optional and must be a string like "DataRepo.models".  It's only required if called before any of
        the model classes have been instantiated and after the decorators have registered.
        """
        if model_class_name in cls.model_classes.keys():
            return cls.model_classes[model_class_name]
        access_method = "determiend by the decorator(s)"
        try:
            if models_path is None:
                # The decorators *try* to record the models_path for each model.  If that was successful, we can use
                # that, otherwise, the models_path is required.
                if (
                    cls.model_packages
                    and model_class_name in cls.model_packages
                    and cls.model_packages[model_class_name] is not None
                ):
                    models_path = cls.model_packages[model_class_name]
                    module = importlib.import_module(models_path)
                else:
                    raise ValueError(
                        f"models_path is required because the class {model_class_name} hasn't been instantiated as a "
                        "MaintainedModel yet."
                    )
            else:
                access_method = "supplied"
                module = importlib.import_module(models_path)
            mdl_cls = getattr(module, model_class_name)
        except Exception as e:
            raise ValueError(
                f"The models_path {access_method} [{models_path}] resulted in an error when trying to retrieve the "
                f"class type [{model_class_name}].  Supply a different models_path.  Error encountered: "
                f"{type(e).__name__}: {e}"
            )
        return mdl_cls

    @classmethod
    def get_all_maintained_field_values(cls, models_path=None):
        """This method is intended be used for tests - to obtain every value of all maintained fields before and after a
        load to ensure that a failed load has no auto-update side-effects.  Results from every maintained field are
        stored in a single flat list for each model in a dict keyed on model.

        Args:
            models_path (Optional[str]): Python path to the models, E.g. "DataRepo.models".  It's only required if
                called before any of the model classes have been instantiated and after the decorators have registered.
        Exceptions:
            None
        Returns:
            all_values (Dict[str, Any]): All values from every maintained field, stored in a list keyed on the model
                name.
        """
        all_values = {}
        maintained_fields = cls.get_maintained_fields_query_dict(models_path)

        for key in maintained_fields.keys():
            mdl: Model = maintained_fields[key]["class"]
            flds = maintained_fields[key]["fields"]
            all_values[mdl.__name__] = []
            for fld in flds:
                all_values[mdl.__name__] += list(
                    mdl.objects.values_list(fld, flat=True)
                )

        return all_values

    @classmethod
    def get_maintained_fields_query_dict(cls, models_path=None):
        """
        Returns all of the model classes that have maintained fields and the names of those fields in a dict where the
        class name is the key and each value is a dict containing, for example:

        {"class": <model class reference>, "fields": [list of field names]}

        models_path is optional and must be a string like "DataRepo.models".  It's only required if called before any of
        the model classes have been instantiated and after the decorators have registered.
        """
        maintained_fields = defaultdict(lambda: defaultdict(list))

        # For each model class
        for mdl in cls._get_classes(None, None, None, models_path=models_path):
            mdl_name = mdl.__name__
            mdl_update_flds = []

            if mdl_name not in cls.updater_list:
                raise NoDecorators(mdl_name)

            for updater_dict in cls.updater_list[mdl_name]:
                if (
                    "update_field" in updater_dict.keys()
                    and updater_dict["update_field"]
                ):
                    mdl_update_flds.append(updater_dict["update_field"])

            if issubclass(mdl, MaintainedModel) and len(mdl_update_flds) > 0:
                maintained_fields[mdl_name]["class"] = mdl
                maintained_fields[mdl_name]["fields"] = mdl_update_flds

        return maintained_fields

    @classmethod
    def _filter_updaters(
        cls,
        updaters_list,
        generation,
        label_filters,
        filter_in,
    ):
        """
        This method was made private, because it has no access to instance values for generation, label_filters, or
        filter_in.  Users are not expected to supply these.  All instance methods that internally call these must pass
        them.

        Returns a sublist of the supplied updaters_list that meets both the filter criteria (generation matches and
        update_label is in the label_filters), if those filters were supplied.
        """
        # This will be the new buffer (in case we're being selective)
        new_updaters_list = []

        # Convenience variables to make the conditional easier to read
        no_filters = label_filters is None or len(label_filters) == 0
        no_generation = generation is None

        for updater_dict in updaters_list:
            gen = updater_dict["generation"]
            label = updater_dict["update_label"]
            has_label = label is not None
            if (
                filter_in
                and (
                    (no_generation or generation == gen)
                    and (no_filters or (has_label and label in label_filters))
                )
            ) or (
                not filter_in
                and (
                    (no_generation or generation != gen)
                    and (no_filters or not has_label or label not in label_filters)
                )
            ):
                new_updaters_list.append(updater_dict)

        return new_updaters_list

    @classmethod
    def updater_list_has_matching_labels(cls, updaters_list, label_filters, filter_in):
        """
        Returns True if any updater dict in updaters_list passes the label filtering criteria.
        """
        for updater_dict in updaters_list:
            label = updater_dict["update_label"]
            has_a_label = label is not None
            if filter_in:
                if has_a_label and label in label_filters:
                    return True
            elif not has_a_label or label not in label_filters:
                return True

        return False

    @classmethod
    def get_max_generation(cls, updaters_list, label_filters=None, filter_in=None):
        """
        Takes a list of updaters and a list of label filters and returns the max generation found in the updaters list.
        """
        if filter_in is None:
            filter_in = True
        if label_filters is None:
            label_filters = (
                []
            )  # Include everything by default, regardless of default filters
            filter_in = True
        max_gen = None
        for updater_dict in sorted(
            cls._filter_updaters(
                updaters_list,
                generation=None,
                label_filters=label_filters,
                filter_in=filter_in,
            ),
            key=lambda x: x["generation"],
            reverse=True,
        ):
            gen = updater_dict["generation"]
            if max_gen is None or gen > max_gen:
                max_gen = gen
                break
        return max_gen

    @classmethod
    def rebuild_maintained_fields(
        cls,
        models_path=None,
        label_filters=None,
        filter_in=None,
    ):
        """
        Performs a mass update of all fields of every record in a breadth-first fashion without repeated updates to the
        same record over and over.
        """
        if label_filters is None or filter_in is None:
            cur_coordinator = cls.get_coordinator()
            if label_filters is None:
                label_filters = cur_coordinator.default_label_filters
            if filter_in is None:
                filter_in = cur_coordinator.default_filter_in

        # Making the autoupdate mode "disabled" is a hack.  Mass autoupdate mode is specified via an argument to .save()
        # called "mass_updates", not an instance variable, but autoupdate mode must be false.  Buffering mode in this
        # case is False, but it's essentially ignored when mass_updates is True.  I should rethink the modes.
        # To look at it another way, nothing in the record is being changed that isn't an autoupdate field, so if we
        # were to "save" it in 2 steps (the original model method) and the autoupdate method, there would be an error
        # about either duplicate/existing records or unique constraint violation.  What we want is an auto-update save
        # only.
        coordinator = MaintainedModelCoordinator(
            auto_update_mode="disabled",
            label_filters=label_filters,
            filter_in=filter_in,
        )

        with cls.custom_coordinator(coordinator):
            # Get the largest generation value
            youngest_generation = cls.get_max_generation(
                cls.get_all_updaters(),
                label_filters=label_filters,
                filter_in=filter_in,
            )
            # Track what's been updated to prevent repeated updates triggered by multiple child updates
            updated = {}
            has_filters = len(label_filters) > 0

            # For every generation from the youngest leaves/children to root/parent
            for gen in sorted(range(youngest_generation + 1), reverse=True):
                # For every MaintainedModel derived class with decorated functions
                for mdl_cls in cls._get_classes(
                    gen,
                    label_filters,
                    filter_in,
                    models_path=models_path,
                ):
                    class_name = mdl_cls.__name__

                    try:
                        updater_dicts = mdl_cls.get_my_updaters()
                    except Exception as e:
                        if not issubclass(mdl_cls, __class__):
                            raise ModelNotMaintained(mdl_cls)
                        raise MissingMaintainedModelDerivedClass(class_name, e)

                    # Leave the loop when the max generation present changes so that we can update the updated buffer
                    # with the parent-triggered updates that were locally buffered during the execution of this loop
                    max_gen = cls.get_max_generation(
                        updater_dicts,
                        label_filters=label_filters,
                        filter_in=filter_in,
                    )
                    if max_gen < gen:
                        break

                    # No need to perform updates if none of the updaters match the label filters
                    if has_filters and not cls.updater_list_has_matching_labels(
                        updater_dicts, label_filters, filter_in
                    ):
                        break

                    # For each record in the database for this model
                    for rec in mdl_cls.objects.all():
                        # Track updated records to avoid repeated updates
                        key = f"{class_name}.{rec.pk}"

                        # Try to perform the update. It could fail if the affected record was deleted
                        try:
                            if key not in updated:
                                # Saving the record while performing_mass_autoupdates is True, causes auto-updates of
                                # every field included among the model's decorated functions.  It does not only update
                                # the fields indicated in decorators that contain the labels indicated in the
                                # label_filters.  The filters are only used to decide which records should be updated.
                                # Currently, this is not an issue because we only have 1 update_label in use.  And if/
                                # when we add another label, it will only end up causing extra repeated updates of the
                                # same record.
                                rec.save(mass_updates=True)

                                # keep track that this record was updated
                                updated[key] = True

                        except Exception as e:
                            raise AutoUpdateFailed(rec, e, updater_dicts)

    def update_decorated_fields(self, fields_to_autoupdate=None):
        """
        Updates every field identified in each MaintainedModel.setter decorator using the decorated function that
        generates its value.

        This uses 2 data members: self.label_filters and self.filter_in in order to determine which fields should be
        updated.  They are initially set when the object is created and refreshed when the object is saved to reflect
        the current filter conditions.  One exception of the refresh, is if performing a mass auto-update, in which
        case the filters the were in effect during buffering are used.
        """
        changed = False
        for updater_dict in self.get_my_updaters():
            update_fld = updater_dict["update_field"]
            update_label = updater_dict["update_label"]

            # from_db was implemented somewhat more carefully than save was.  The documentation for from_db warns about
            # "DEFERRED" fields, whose values aren't available.  After I implemented it, I realized that the deferred
            # fields are likely related to foreign keys, so this conditional probably doesn't effectively do anything.
            #
            # An intersection of the maintained fields and "available" fields is used here to tell MaintainedModel to
            # only auto-update a subset of fields.
            #
            # This conditional just skips fields that aren't in the available fields.
            if (
                fields_to_autoupdate is not None
                and update_fld not in fields_to_autoupdate
            ):
                continue

            # If there is a maintained field(s) in this model and...
            # If auto-updates are restricted to fields by their update_label and this field matches the label
            # filter criteria
            if update_fld is not None and (
                # There are no labels for filtering
                self.label_filters is None
                or len(self.label_filters) == 0
                # or the update_label matches a filter-in label
                or (
                    self.filter_in
                    and update_label is not None
                    and update_label
                    in self.label_filters  # pylint: disable=unsupported-membership-test
                    # For the pylint disable, see: https://github.com/pylint-dev/pylint/issues/3045
                )
                # or the update_label does not match a filter-out label
                or (
                    not self.filter_in
                    and (
                        update_label is None
                        or update_label
                        not in self.label_filters  # pylint: disable=unsupported-membership-test
                        # For the pylint disable, see: https://github.com/pylint-dev/pylint/issues/3045
                    )
                )
            ):
                update_fun = getattr(self, updater_dict["update_function"])
                try:
                    old_val = getattr(self, update_fld)
                except ObjectDoesNotExist as odne:
                    # This assumes that when this is raised, update_fld is a relation field (e.g. ForeignKeyField)
                    # It also assumes a .delete() of the referenced record is what caused this.  Note, in my test case,
                    # a serum Sample record was deleted.  A parent Animal record's update occurred via propagation of
                    # autoupdates.  It's last_serum_sample's on_delete was SET_NULL.  A getattr() on that field yields a
                    # KeyError referring to cached values accessed deep in the django core code.  It's trace was
                    # truncated, so I could not verify the source of the DoesNotExist exception, but catching
                    # ObjectDoesNotExist does catch it.  I think it's a pretty safe assumption, but...
                    # TODO: explicitly check if a delete had happened.  There may be a way to get the value of the
                    # foreign key without triggering a database query and set old_val to None.
                    warnings.warn(
                        f"{odne.__class__.__name__} error getting current value of relation field [{update_fld}]: "
                        f"[{str(odne)}].  This is likely being triggered by a deleted record whose relation used to "
                        "link to it, but its cache has not been cleared."
                    )
                    old_val = f"<error {type(odne).__name__}>"

                new_val = update_fun()

                setattr(self, update_fld, new_val)

                if old_val != new_val:
                    changed = True

                # Report the auto-update
                if old_val is None or old_val == "":
                    old_val = "<empty>"

                if changed:
                    print(
                        f"Auto-updated {self.__class__.__name__}.{update_fld} in {self.__class__.__name__}.{self.pk} "
                        f"using {update_fun.__qualname__} from [{old_val}] to [{new_val}]."
                    )
                else:
                    print(
                        f"Auto-update of {self.__class__.__name__}.{update_fld} in {self.__class__.__name__}.{self.pk} "
                        f"using {update_fun.__qualname__} resulted in the same value: [{new_val}]."
                    )

        return changed

    def get_record_signature(self):
        if self.pk is None:
            return None
        return f"{self.__class__.__name__}.{self.pk}"

    def call_dfs_related_updaters(
        self,
        updated=None,
        mass_updates=False,
        label_filters: Optional[List[str]] = None,
        filter_in=True,
    ):
        """This is a recursive method that propagates triggered updates both up and down the propagation path defined by
        each setter and relation decorator's child and parent fields.

        Args:
            updated (Optional[List[str]]): A list of model object signatures used to prevent repeated updates.
            mass_updates (bool) [False]: Whether we're in mass auto-update mode.
            label_filters (Optional[List[str]]): A list of update_labels that define the propagation path(s) that should
                be followed.
            filter_in (bool) [True]: When True, label_filters contains the labels where propagation should proceed.
                When False, label_filters are the paths that should be avoided.
        Exceptions:
            None
        Returns:
            updated (List[str]): A list of model object signatures that were updated.
        """
        # Assume I've been called after I've been updated, so add myself to the updated list
        if updated is None:
            updated = []
        self_sig = self.get_record_signature()
        if self_sig is not None and self_sig not in updated:
            updated.append(self_sig)
        updated = self.call_child_updaters(
            updated=updated,
            mass_updates=mass_updates,
            label_filters=label_filters,
            filter_in=filter_in,
        )
        updated = self.call_parent_updaters(
            updated=updated,
            mass_updates=mass_updates,
            label_filters=label_filters,
            filter_in=filter_in,
        )
        return updated

    def call_parent_updaters(
        self,
        updated,
        mass_updates=False,
        label_filters: Optional[List[str]] = None,
        filter_in=True,
    ):
        """This calls parent record's `save` method to trigger updates to their maintained fields (if any) and further
        propagate those changes up the hierarchy (if those records have parents). It skips triggering a parent's update
        if that child was the object that triggered its update, to avoid looped repeated updates.

        Args:
            updated (List[str]): A list of model object signatures used to prevent repeated updates.
            mass_updates (bool) [False]: Whether we're in mass auto-update mode.
            label_filters (Optional[List[str]]): A list of update_labels that define the propagation path(s) that should
                be followed.
            filter_in (bool) [True]: When True, label_filters contains the labels where propagation should proceed.
                When False, label_filters are the paths that should be avoided.
        Exceptions:
            None
        Returns:
            updated (List[str]): A list of model object signatures that were updated.
        """
        parents = self.get_parent_instances()
        parent_inst: MaintainedModel
        for parent_inst in parents:
            # Only follow propagation paths that have update_labels that match the active label_filters
            if (
                label_filters is not None
                and not parent_inst.updater_list_has_matching_labels(
                    parent_inst.get_my_updaters(), label_filters, filter_in
                )
            ):
                continue
            # If the current instance's update was triggered - and was triggered by the same parent instance whose
            # update we're about to trigger
            parent_sig = parent_inst.get_record_signature()
            if parent_sig not in updated:
                if settings.DEBUG:
                    self_sig = self.get_record_signature()
                    print(
                        f"Propagating change from child {self_sig} to parent {parent_sig}"
                    )

                # Don't let the save call propagate.  Previously, I was relying on save returning the updated list, but
                # since .save() could be overridden by another class that doesn't return anything, I was getting back
                # None (at least, that's my guess as to why I was getting back None when I tried it).  So instead, I
                # implemented the propagation outside of the .save calls using the call_dfs_related_updaters call
                # below.
                parent_inst.save(propagate=False, mass_updates=mass_updates)

                # Propagate manually
                updated = parent_inst.call_dfs_related_updaters(
                    updated=updated,
                    mass_updates=mass_updates,
                    label_filters=label_filters,
                    filter_in=filter_in,
                )

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
                    # Make sure that the (direct) parnet (or M:M related parent) *isa* MaintainedModel
                    if isinstance(tmp_parent_inst, MaintainedModel):
                        parent_inst = tmp_parent_inst
                        # We check "exists_in_db" in case these updates are propagated due to a delete, and any relation
                        # we are traversing, could have been deleted via a cascade
                        if parent_inst not in parents and parent_inst.exists_in_db():
                            parents.append(parent_inst)

                    elif (
                        tmp_parent_inst.__class__.__name__ == "ManyRelatedManager"
                        or tmp_parent_inst.__class__.__name__ == "RelatedManager"
                    ):
                        # NOTE: This is where the `through` model is skipped
                        try:
                            if tmp_parent_inst.count() > 0 and isinstance(
                                tmp_parent_inst.first(), MaintainedModel
                            ):
                                for mm_parent_inst in tmp_parent_inst.all():
                                    # We check "exists_in_db" in case these updates are propagated due to a delete, and
                                    # any relation we are traversing, could have been deleted via a cascade
                                    if (
                                        mm_parent_inst not in parents
                                        and mm_parent_inst.exists_in_db()
                                    ):
                                        parents.append(mm_parent_inst)

                            elif tmp_parent_inst.count() > 0:
                                raise NotMaintained(tmp_parent_inst.first(), self)
                        except ValueError as ve:
                            # If the ValueError has this substring, this means that the reverse "parent" relation does
                            # not have any links to the child instance, so there is no reason to append this parent.
                            # Unfortunately, there exists no way to test the emptiness of a reverse relation (without
                            # this exception) that I could find.
                            if "instance needs to have a primary key value" not in str(
                                ve
                            ):
                                raise ProgrammingError(
                                    "Django is raising a ValueError with a different(/new?) message when trying to "
                                    "access empty reverse relations.  The code needs to be updated to additionally "
                                    f"support this ValueError: '{str(ve)}'"
                                )

                    else:
                        raise NotMaintained(tmp_parent_inst, self)

        return parents

    def call_child_updaters(
        self,
        updated,
        mass_updates=False,
        label_filters: Optional[List[str]] = None,
        filter_in=True,
    ):
        """
        This calls child record's `save` method to trigger updates to their maintained fields (if any) and further
        propagate those changes up the hierarchy (if those records have parents). It skips triggering a child's update
        if that child was the object that triggered its update, to avoid looped repeated updates.

        Args:
            updated (List[str]): A list of model object signatures used to prevent repeated updates.
            mass_updates (bool) [False]: Whether we're in mass auto-update mode.
            label_filters (Optional[List[str]]): A list of update_labels that define the propagation path(s) that should
                be followed.
            filter_in (bool) [True]: When True, label_filters contains the labels where propagation should proceed.
                When False, label_filters are the paths that should be avoided.
        Exceptions:
            None
        Returns:
            updated (List[str]): A list of model object signatures that were updated.
        """
        children = self.get_child_instances()
        child_inst: MaintainedModel
        for child_inst in children:
            # Only follow propagation paths that have update_labels that match the active label_filters
            if (
                label_filters is not None
                and not child_inst.updater_list_has_matching_labels(
                    child_inst.get_my_updaters(), label_filters, filter_in
                )
            ):
                continue
            # If the current instance's update was triggered - and was triggered by the same child instance whose
            # update we're about to trigger
            child_sig = child_inst.get_record_signature()
            if child_sig not in updated:
                if settings.DEBUG:
                    self_sig = self.get_record_signature()
                    print(
                        f"Propagating change from parent {self_sig} to child {child_sig}"
                    )

                # Don't let the save call propagate, because we cannot rely on it returning the updated list (because
                # it could be overridden by another class that doesn't return it (at least, that's my guess as to why I
                # was getting back None when I tried it.)
                child_inst.save(propagate=False, mass_updates=mass_updates)

                # Instead, we will propagate manually:
                updated = child_inst.call_dfs_related_updaters(
                    updated=updated,
                    mass_updates=mass_updates,
                    label_filters=label_filters,
                    filter_in=filter_in,
                )

        return updated

    def get_child_instances(self):
        """Returns a list of child records to the current record (self) (and the child relationship is stored in the
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
                        # We check "exists_in_db" in case these updates are propagated due to a delete, and any relation
                        # we are traversing, could have been deleted via a cascade
                        if child_inst not in children and child_inst.exists_in_db():
                            children.append(child_inst)

                    elif self.pk is not None and (
                        tmp_child_inst.__class__.__name__ == "ManyRelatedManager"
                        or tmp_child_inst.__class__.__name__ == "RelatedManager"
                    ):
                        # NOTE: This is where the `through` model is skipped
                        if tmp_child_inst.count() > 0 and isinstance(
                            tmp_child_inst.first(), MaintainedModel
                        ):
                            for mm_child_inst in tmp_child_inst.all():
                                # We check "exists_in_db" in case these updates are propagated due to a delete, and any
                                # relation we are traversing, could have been deleted via a cascade
                                if (
                                    mm_child_inst not in children
                                    and mm_child_inst.exists_in_db()
                                ):
                                    children.append(mm_child_inst)

                        elif tmp_child_inst.count() > 0:
                            raise NotMaintained(tmp_child_inst.first(), self)

                    elif (
                        not isinstance(tmp_child_inst, MaintainedModel)
                        and tmp_child_inst.__class__.__name__ != "ManyRelatedManager"
                        and tmp_child_inst.__class__.__name__ != "RelatedManager"
                    ):
                        raise NotMaintained(tmp_child_inst, self)
                    # Otherwise, this is a *RelatedManager or self.pk is None, meaning it hasn't been created yet - and
                    # if the record hasn't been created yet, another model that links to it cannot have linked to it
                    # yet, so there cannot be any children to return (because to create the relation, you have to supply
                    # a created record).
                else:
                    raise ValueError(
                        f"Unexpected child reference for field [{child_fld}] is None."
                    )

        return children

    @classmethod
    def get_my_update_fields(cls):
        """
        Returns a list of update_fields of the current model that are marked via the MaintainedModel.setter
        decorators in the model.  Returns an empty list if there are none (e.g. if the only decorator in the model is
        the relation decorator on the class).
        """
        return [
            updater_dict["update_field"]
            for updater_dict in cls.get_my_updaters()
            if "update_field" in updater_dict.keys() and updater_dict["update_field"]
        ]

    @classmethod
    def get_my_updaters(cls):
        """
        Retrieves all the updater information of each decorated function of the calling model from the global
        updater_list variable.
        """
        updaters = []
        if cls.__name__ in cls.updater_list:
            updaters = cls.updater_list[cls.__name__]
        else:
            raise NoDecorators(cls.__name__)

        return updaters

    @classmethod
    def get_my_update_labels(cls):
        """Returns a list of 'update_label's from each decorated function of the calling model.

        Args:
            None
        Exceptions:
            NoDecorators
        Returns:
            update_labels (List[str])
        """
        update_labels: List[str] = []
        if cls.__name__ in cls.updater_list.keys():
            update_labels = [
                updater_dict["update_label"]
                for updater_dict in cls.updater_list[cls.__name__]
            ]
        else:
            raise NoDecorators(cls.__name__)

        return sorted(update_labels)

    class Meta:
        abstract = True


class NotMaintained(Exception):
    def __init__(self, parent, caller):
        message = (
            f"Class {parent.__class__.__name__} or {caller.__class__.__name__} must inherit from "
            f"{MaintainedModel.__name__}."
        )
        super().__init__(message)
        self.parent = parent
        self.caller = caller


class ModelNotMaintained(Exception):
    def __init__(self, model_class: Type[Model]):
        super().__init__(
            f"Model class '{model_class.__name__}' must inherit from {MaintainedModel.__name__}."
        )
        self.model_class = model_class


class BadModelFields(Exception):
    def __init__(self, cls, flds, fcn=None):
        fld_strs = [f"{d['field']} ({d['type']})" for d in flds]
        message = (
            f"The {cls} class does not have field(s): ['{', '.join(fld_strs)}'].  "
        )
        if fcn:
            message += (
                f"Make sure the fields supplied to the @MaintainedModel.setter decorator of the function: {fcn} "
                f"are valid {cls} fields."
            )
        else:
            message += (
                f"Make sure the fields supplied to the @relation class decorator are valid {cls} "
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
            "@MaintainedModel.setter decorator."
        )
        super().__init__(message)
        self.cls = cls
        self.fld = fld
        self.fcn = fcn


class InvalidRootGeneration(Exception):
    def __init__(self, cls, fld, fcn, gen):
        message = (
            f"Invalid generation: [{gen}] for {cls}.{fld} supplied to @MaintainedModel.setter decorator of "
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
            "MaintainedModel.setter decorator to a method in the class or remove the base class 'MaintainedModel' "
            "and an parent fields referencing this model in other models.  If this model has no field to update but "
            "its parent does, create a decorated placeholder method that sets its parent_field and generation only."
        )
        super().__init__(message)


class AutoUpdateFailed(Exception):
    def __init__(self, model_object, err, updaters):
        updater_flds = [
            d["update_field"] for d in updaters if d["update_field"] is not None
        ]
        if len(updater_flds) == 0:
            message = f"Propagation of autoupdates from the {model_object.__class__.__name__} model "
        else:
            message = f"Autoupdate of the {model_object.__class__.__name__} model's fields [{', '.join(updater_flds)}] "
        try:
            obj_str = str(model_object)
        except Exception:
            # Displaying the offending object is optional, so catch any error
            obj_str = "unknown"
        message += (
            f"in the database failed for record {obj_str}.  Potential causes:\n"
            "\t1. The record was created and deleted before the buffered update (a catch for the exception should be "
            "added and ignored).\n"
            "\t2. The autoupdate buffer is stale and auto-updates are being attempted on non-existent records.  There "
            "are multiple possible resolutions to this case.  If deletions of parent and child records are occurring "
            "together, a `no_autoupdates` decorator/context-manager can be applied to the deletion of the child "
            "records so that the mass autoupdates do not get called on the deleted parent records, or you can apply a "
            "`no_autoupdates` decorator to the entire model modification method (or call clear_update_buffer() at the "
            "end of the method), and run `rebuild_maintained_fields` afterwards.\n"
            f"The triggering {err.__class__.__name__} exception: [{err}]."
        )
        super().__init__(message)


class StaleAutoupdateMode(Exception):
    def __init__(self):
        message = (
            "An autoupdate mode was enabled during a mass update of maintained fields.  Automated update of the global "
            "variable mass_updates may have been interrupted during execution of "
            "perform_buffered_updates."
        )
        super().__init__(message)


class UncleanBufferError(Exception):
    def __init__(self, message=None):
        if message is None:
            message = (
                "The auto-update buffer is unexpectedly populated.  Make sure failed or suspended loads clean up the "
                "buffer when they finish."
            )
        super().__init__(message)


class DeferredParentCoordinatorContextError(Exception):
    def __init__(self, message=None):
        if message is None:
            message = (
                "MaintainedModel.get_parent_deferred_coordinator() must only be called when the coordinator_stack is "
                "populated, e.g. after a recently created coordinator has been added to the coordinator_stack.  This "
                "is so we can be sure we're not returning the coordinator being added to the stack.  The method makes "
                "a copy of the stack and pops off the coordinator that was just added in order to only inspect the "
                "parents."
            )
        super().__init__(message)


class MissingMaintainedModelDerivedClass(Exception):
    def __init__(self, cls, err):
        message = f"The {cls} class must be imported so that its eval works.  {err}"
        super().__init__(message)
        self.cls = cls


class ConditionallyRequiredArgumentError(Exception):
    pass
