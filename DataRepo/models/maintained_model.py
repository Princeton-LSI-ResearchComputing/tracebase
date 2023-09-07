import importlib
import warnings
from collections import defaultdict
from contextlib import contextmanager
from threading import local

from django.conf import settings
from django.db.models import Model
from django.db.models.signals import m2m_changed
from django.db.transaction import TransactionManagementError
from django.db.utils import IntegrityError
from psycopg2.errors import ForeignKeyViolation


class MaintainedModelCoordinator:
    """
    This class loosely mimmics a connection in a Django "database instrumentation" design.  But instead of providing a
    way to apply custom wrappers to database queries, it provides a way to wrap MaintainedModel's calls to save, delete,
    and m2m_propagation_handler by providing data that is used in those overridden methods via extra keyword arguments
    added to them.  It has default modes that are set, but those modes can be overridden by providing them to the
    constructor in a context manager.
    """

    # Track whether the fields from the decorators have been validated
    # This is only ever initialized once, the first time every derived class is ever instantiated, so it's loosely an
    # immutable class attribute (though technically mutable), as it would only ever change if new models are added and
    # is only used to decide when a derived class's usage of MaintainedModel is invalid
    maintained_model_initialized = {}

    # Track the metadata recorded by each derived model class's setter and relation decorators
    # Similar to maintained_model_initialized, this is loosely an immutable class attribute (though technically
    # mutable), as it is only ever set when the setter and relation decorators are created, which only ever happens once
    # This does not need to be thread-safe because it will not ever change after all the decorators have been registered
    updater_list = defaultdict(list)

    # A dict of class name keys and class values used by get_classes needed for rebuild_maintained_fields. This is
    # initialized via MaintainedModel's decorators as a way to avoid needing the module path of all the models as was
    # formerly done.
    model_classes = defaultdict(Model)

    def __init__(self, *args, **kwargs):
        auto_update_mode = kwargs.pop(
            "auto_update_mode", "immediate"  # possibilities: immediate, deferred, and disabled
        )
        self.auto_update_mode = auto_update_mode
        if auto_update_mode == "immediate":
            # These 3 modes are for when autoupdates should be immediately performed upon every individual record change
            self.auto_updates = True
            self.buffering = True
        elif auto_update_mode == "deferred":
            # These 3 modes are for when autoupdates should be buffered upon every individual record change
            self.auto_updates = False
            self.buffering = True
        elif auto_update_mode == "disabled":
            # These 3 modes are for when autoupdates will never be performed
            self.auto_updates = False
            self.buffering = False
        else:
            raise ValueError(
                f"Invalid auto_update_mode: [{auto_update_mode}].  Valid values are: [immediate, deferred, and "
                "disabled]."
            )

        # These allow the user to turn on or off specific groups of auto-updates.  See init_autoupdate_label_filters.
        label_filters = kwargs.pop("label_filters", None)
        self.default_label_filters = label_filters

        filter_in = kwargs.pop("filter_in", True)
        self.default_filter_in = filter_in

        self.nondefault_filtering_exists = not (
            label_filters == None
            and filter_in == True
        )

        # This is for buffering a large quantity of auto-updates in order to get speed improvements during loading
        self.update_buffer = []

    def get_mode(self):
        return self.auto_update_mode

    def are_autoupdates_enabled(self):
        return self.auto_updates

    # def disable_autoupdates(self):
    #     """
    #     Do not allow record changes to trigger the auto-update of maintained fields.  Instead, buffer those updates.
    #     """
    #     # If custom filtering is in effect, ensure filtering is re-initialized before auto-updates are re-enabled
    #     if self.auto_updates and self.nondefault_filtering_exists:
    #         raise InitFiltersAfterDisablingAutoupdates()

    #     self.auto_updates = False

    # def enable_autoupdates(self):
    #     """
    #     Allow record changes to trigger the auto-update of maintained fields and no longer buffer those updates.
    #     """
    #     # If custom filtering is in effect, ensure filtering is re-initialized before auto-updates are re-enabled
    #     if not self.auto_updates and self.nondefault_filtering_exists:
    #         raise ClearFiltersBeforeEnablingAutoupdates()

    #     self.auto_updates = True

    # def disable_buffering(self):
    #     """
    #     Do not allow record changes to buffer pending changes to maintained fields.
    #     """
    #     self.buffering = False

    # def enable_buffering(self):
    #     """
    #     Allow record changes to buffer pending changes to maintained fields.
    #     """
    #     self.buffering = True

    def buffer_size(self, generation=None, label_filters=None, filter_in=None):
        """
        Returns the number of buffered records that contain at least 1 decorated function matching the filter criteria
        (generation and label).
        """
        if label_filters is None:
            label_filters = self.default_label_filters
        if filter_in is None:
            filter_in = self.default_filter_in
        cnt = 0
        for buffered_item in self.update_buffer:
            updaters_list = self.filter_updaters(
                self.get_updater_dicts_by_model_name(buffered_item.__class__.__name__),
                generation=generation,
                label_filters=label_filters,
                filter_in=filter_in,
            )
            cnt += len(updaters_list)
        return cnt

    def filter_updaters(
        self,
        updaters_list,
        generation=None,
        label_filters=None,
        filter_in=None,
    ):
        """
        Returns a sublist of the supplied updaters_list that meets both the filter criteria (generation matches and
        update_label is in the label_filters), if those filters were supplied.
        """
        if label_filters is None:
            label_filters = self.default_label_filters
        if filter_in is None:
            filter_in = self.default_filter_in

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
        if label_filters is None:
            label_filters = self.default_label_filters
        if filter_in is None:
            filter_in = self.default_filter_in
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
            matching_updaters = self.filter_updaters(
                self.get_updater_dicts_by_model_name(buffered_item.__class__.__name__),
                generation=generation,
                label_filters=label_filters,
                filter_in=filter_in,
            )

            max_gen = 0
            # We should issue a warning if the remaining updaters left in the buffer contain a greater generation,
            # because updates and buffer clears should happen from leaf to root.  And we should only check those which
            # have a target label.
            if generation is not None:
                max_gen = self.get_max_generation(
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

    def get_max_generation(self, updaters_list, label_filters=None, filter_in=None):
        """
        Takes a list of updaters and a list of label filters and returns the max generation found in the updaters list.
        """
        if label_filters is None:
            label_filters = self.default_label_filters
        if filter_in is None:
            filter_in = self.default_filter_in
        max_gen = None
        for updater_dict in sorted(
            self.filter_updaters(
                updaters_list, label_filters=label_filters, filter_in=filter_in
            ),
            key=lambda x: x["generation"],
            reverse=True,
        ):
            gen = updater_dict["generation"]
            if max_gen is None or gen > max_gen:
                max_gen = gen
                break
        return max_gen

    def _peek_update_buffer(self, index=0):
        return self.update_buffer[index]

    def get_max_buffer_generation(self, label_filters=None, filter_in=None):
        """
        Takes a list of label filters and searches the buffered records to return the max generation found among the
        decorated functions (matching the filter criteria) associated with the buffered model object's class.

        The purpose is so that records can be updated breadth first (from leaves to root).
        """
        if label_filters is None:
            label_filters = self.default_label_filters
        if filter_in is None:
            filter_in = self.default_filter_in
        exploded_updater_dicts = []
        for buffered_item in self.update_buffer:
            exploded_updater_dicts += self.filter_updaters(
                self.get_updater_dicts_by_model_name(buffered_item.__class__.__name__),
                label_filters=label_filters,
                filter_in=filter_in,
            )
        return self.get_max_generation(
            exploded_updater_dicts, label_filters=label_filters, filter_in=filter_in
        )

    def updater_list_has_labels(self, updaters_list, label_filters=None, filter_in=None):
        """
        Returns True if any updater dict in updaters_list passes the label filtering criteria.
        """
        if label_filters is None:
            label_filters = self.default_label_filters
        if filter_in is None:
            filter_in = self.default_filter_in
        for updater_dict in updaters_list:
            label = updater_dict["update_label"]
            has_a_label = label is not None
            if filter_in:
                if has_a_label and label in label_filters:
                    return True
            elif not has_a_label or label not in label_filters:
                return True

        return False

    def buffer_update(self, mdl_obj):
        """
        This is called when MaintainedModel.save is called (if auto_updates is False), so that maintained fields can be
        updated after loading code finishes (by calling the global method: perform_buffered_updates).
        """

        # See if this class contains a field with a matching label (if a populated label_filters array was supplied)
        if mdl_obj.label_filters is not None and len(mdl_obj.label_filters) > 0:
            do_buffer = False
            for updater_dict in self.get_updater_dicts_by_model_name(mdl_obj.__class__.__name__):
                update_label = updater_dict["update_label"]

                # If there is a maintained field(s) in this model and...
                # If auto-updates are restricted to fields by their update_label and this field matches the label
                # filter criteria
                if (
                    # The update_label matches a filter-in label
                    (
                        mdl_obj.filter_in
                        and update_label is not None
                        and update_label
                        in mdl_obj.label_filters  # pylint: disable=unsupported-membership-test
                        # For the pylint disable, see: https://github.com/pylint-dev/pylint/issues/3045
                    )
                    # The update_label does not match a filter-out label
                    or (
                        not mdl_obj.filter_in
                        and (
                            update_label is None
                            or update_label
                            not in mdl_obj.label_filters  # pylint: disable=unsupported-membership-test
                            # For the pylint disable, see: https://github.com/pylint-dev/pylint/issues/3045
                        )
                    )
                ):
                    do_buffer = True
                    break

            # If there's not a field passing the filter criteria, do not buffer (return)
            if not do_buffer:
                return

        # Do not buffer if it's already buffered.  Note, this class isn't designed to support auto-updates in a
        # sepecific order.  All auto-update functions should use non-auto-update fields.
        if self.buffering and mdl_obj not in self.update_buffer:
            self.update_buffer.append(mdl_obj)
        elif self.buffering:
            # This allows the same object to be updated more than once (in the order encountered) if the fields to be
            # auto-updated in each instance, differ.  This can cause redundant updates (e.g. when a field matches the
            # filters in both cases), but given the possibility that update order may depend on the update of related
            # records, it's better to be on the safe side and do each auto-update, so...
            # If this is the same object but a different set of fields will be updated...
            # Note, Django model object equivalence (obj1 == obj2) compares primary key values
            for same_obj in [ubo for ubo in self.update_buffer if ubo == mdl_obj]:
                if (
                    same_obj.filter_in != mdl_obj.filter_in
                    or same_obj.label_filters != same_obj.label_filters
                ):
                    self.update_buffer.append(mdl_obj)

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
        use_object_label_filters = True
        if label_filters is not None:
            use_object_label_filters = False
            if self.filter_in is None:
                filter_in = self.default_filter_in

        # orig_au_mode = self.are_autoupdates_enabled()
        # if orig_au_mode:
        #     # Autoupdate mode must remain disabled during a mass update of maintained fields so that parent updates are
        #     # not triggered in a depth-first fashion.
        #     self.disable_autoupdates()
        if self.coordinator.are_autoupdates_enabled():
            raise StaleAutoupdateMode()

        if len(self.update_buffer) == 0:
            return

        # This allows our updates to be saved, but prevents propagating changes up the hierarchy in a depth-first
        # fashion
        # self.enable_mass_autoupdates()
        # Track what's been updated to prevent repeated updates triggered by multiple child updates
        updated = []
        new_buffer = []
        no_filters = label_filters is None or len(label_filters) == 0

        # For each record in the buffer
        for buffer_item in self.update_buffer:
            updater_dicts = self.get_updater_dicts_by_model_name(buffer_item.__class__.__name__)

            if use_object_label_filters:
                label_filters = buffer_item.label_filters
                filter_in = buffer_item.filter_in
                if label_filters is None:
                    label_filters = self.default_label_filters
                    filter_in = self.default_filter_in

            # Track updated records to avoid repeated updates
            key = f"{buffer_item.__class__.__name__}.{buffer_item.pk}"

            # Try to perform the update. It could fail if the affected record was deleted
            try:
                if key not in updated and (
                    no_filters
                    or self.updater_list_has_labels(
                        updater_dicts, label_filters=label_filters, filter_in=filter_in
                    )
                ):
                    # Saving the record while mass_updates is True, causes auto-updates of every field
                    # included among the model's decorated functions.  It does not only update the fields indicated in
                    # decorators that contain the labels indicated in the label_filters.  The filters are only used to
                    # decide which records should be updated.  Currently, this is not an issue because we only have 1
                    # update_label in use.  And if/when we add another label, it will only end up causing extra
                    # repeated updates of the same record.
                    buffer_item.save(propagate=False, mass_updates=True)

                    # Propagate the changes (if necessary), keeping track of what is updated and what's not.
                    # Note: all the manual changes are assumed to have been made already, so auto-updates only need to
                    # be issued once per record
                    updated = buffer_item.call_dfs_related_updaters(updated=updated, mass_updates=True)

                elif key not in updated and buffer_item not in new_buffer:
                    new_buffer.append(buffer_item)

            except Exception as e:
                # self.disable_mass_autoupdates()
                # if orig_au_mode:
                #     self.enable_autoupdates()
                raise AutoUpdateFailed(buffer_item, e, updater_dicts)

        # Eliminate the updated items from the buffer
        self.update_buffer = new_buffer

        # # We're done performing buffered updates
        # self.disable_mass_autoupdates()
        # if orig_au_mode:
        #     self.enable_autoupdates()

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
    def get_classes(
        cls,
        models_path=None,
        generation=None,
        label_filters=None,
        filter_in=None,
    ):
        """
        Retrieve a list of classes containing maintained fields that match the given criteria.
        Used by rebuild_maintained_fields and get_maintained_fields.

        models_path is optional and must be a string like "DataRepo.models".  It's only required if called before any of
        the model classes have been instantiated and after the decorators have registered.
        """
        if label_filters is None:
            label_filters = cls.default_label_filters
        if filter_in is None:
            filter_in = cls.default_filter_in
        class_list = []
        for model_class_name in MaintainedModelCoordinator.updater_list.keys():
            if (
                len(
                    cls.filter_updaters(
                        MaintainedModelCoordinator.updater_list[model_class_name],
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
        if models_path is None:
            raise ValueError(
                f"models_path is required because the class {model_class_name} hasn't been instantiated as a "
                "MaintainedModel yet."
            )
        module = importlib.import_module(models_path)
        return getattr(module, model_class_name)

    @classmethod
    def get_maintained_fields(cls, models_path=None):
        """
        Returns all of the model classes that have maintained fields and the names of those fields in a dict where the
        class name is the key and each value is a dict containing, for example:

        {"class": <model class reference>, "fields": [list of field names]}

        models_path is optional and must be a string like "DataRepo.models".  It's only required if called before any of
        the model classes have been instantiated and after the decorators have registered.
        """
        maintained_fields = defaultdict(lambda: defaultdict(list))
        for mdl in cls.get_classes(models_path):
            mdl_name = mdl.__name__
            mdl_updaters = cls.get_updater_dicts_by_model_name(mdl_name)
            if issubclass(mdl, MaintainedModel) and len(mdl_updaters) > 0:
                maintained_fields[mdl_name]["class"] = mdl
                maintained_fields[mdl_name]["fields"] = mdl_updaters
        return maintained_fields

    @classmethod
    def get_updater_dicts_by_model_name(cls, model_name):
        """
        Returns a list of update_fields of the current model that are marked via the MaintainedModel.setter
        decorators in the model.  Returns an empty list if there are none (e.g. if the only decorator in the model is
        the relation decorator on the class).
        """
        my_update_fields = []
        if model_name in cls.updater_list:
            for updater_dict in cls.updater_list[model_name]:
                if (
                    "update_field" in updater_dict.keys()
                    and updater_dict["update_field"]
                ):
                    my_update_fields.append(updater_dict["update_field"])
        else:
            raise NoDecorators(model_name)

        return my_update_fields

    @classmethod
    def get_all_maintained_field_values(cls, models_path=None):
        """
        This method can be used to obtain every value of a maintained field before and after a load that raises an
        exception to ensure that the failed load has no side-effects.  Results are stored in a list for each model in a
        dict keyed on model.

        models_path is optional and must be a string like "DataRepo.models".  It's only required if called before any of
        the model classes have been instantiated and after the decorators have registered.
        """
        all_values = {}
        maintained_fields = cls.get_maintained_fields(models_path)

        for key in maintained_fields.keys():
            mdl = maintained_fields[key]["class"]
            flds = maintained_fields[key]["fields"]
            all_values[mdl.__name__] = list(mdl.objects.values_list(*flds, flat=True))
        return all_values

    # def init_autoupdate_label_filters(self, label_filters=None, filter_in=None):
    #     """
    #     Changing the filtering criteria using label_filters changes what autoupdates will be buffered.  Model objects
    #     containing maintained fields with an update_label that matches the filtering criteria will be buffered for a
    #     later autoupdate when auto_updates is False.  If auto_updates is True, only fields in model objects with a
    #     matching update_label will be auto-updated.  And during a mass autoupdate (perform_buffered_updates), only the
    #     fields whose update_label matched during buffering will be updated.
    #     """
    #     if label_filters is not None:
    #         self.nondefault_filtering_exists = True
    #         if filter_in is None:
    #             filter_in = True  # Default
    #     else:
    #         self.nondefault_filtering_exists = False
    #         filter_in = True  # Default
    #         # label_filters default is None

    #     self.default_label_filters = label_filters
    #     self.default_filter_in = filter_in


class MaintainedModel(Model):
    """
    This class maintains database field values for a django.models.Model class whose values can be derived using a
    function.  If a record changes, the decorated function/class is used to update the field value.  It can also
    propagate changes of records in linked models.  Every function in the derived class decorated with the
    `@MaintainedModel.setter` decorator (defined above, outside this class) will be called and the associated field
    will be updated.  Only methods that take no arguments are supported.  This class overrides the class's save and
    delete methods and uses m2m_changed signals as triggers for the updates.
    """

    # Thread-safe mutable class attributes
    data = local()

    # This manages adding data to calls to save, delete, and m2m_propagation_handler
    data.default_coordinator = MaintainedModelCoordinator()
    data.coordinator_stack = []

    def __init__(self, *args, **kwargs):
        """
        This over-ride of the constructor is to prevent developers from explicitly setting values for automatically
        maintained fields.  It also performs a one-time validation check of the updater_dicts.
        """

        # The coordinator keeps track of the running mode, buffer and filters in use
        self.coordinator = self.get_coordinator()

        # Members added by MaintainedModel - the global values are set via init_autoupdate_label_filters.  They are
        # recorded in the object so that during perform_buffered_updates will know what field(s) to update when it
        # processes the object.  An update would not have been buffered if the model did not contain a maintained field
        # matching the label filtering.  And label filtering can change during the buffering process (e.g. different
        # loaders), which is why this is necessary.  Note, this is not thread-safe.
        self.label_filters = self.coordinator.default_label_filters
        self.filter_in = self.coordinator.default_filter_in

        class_name = self.__class__.__name__

        # Register the class with the coordinator if not already registered
        if class_name not in self.coordinator.model_classes.keys():
            self.coordinator.model_classes[class_name] = self.__class__

        for updater_dict in self.coordinator.updater_list[class_name]:
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
            if decorator_signature not in self.coordinator.maintained_model_initialized:
                if settings.DEBUG:
                    print(
                        f"Validating {self.__class__.__name__} updater: {updater_dict}"
                    )

                self.coordinator.maintained_model_initialized[decorator_signature] = True
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

        super().__init__(*args, **kwargs)

    def save(self, *args, **kwargs):
        """
        This is an override of the derived model's save method that is being used here to automatically update
        maintained fields.
        """
        # Custom argument: propagate - Whether to propagate updates to related model objects - default True
        # Used internally. Do not supply unless you know what you're doing.
        propagate = kwargs.pop("propagate", True)
        # Custom argument: mass_updates - Whether auto-updating biffered model objects - default False
        # Used internally. Do not supply unless you know what you're doing.
        mass_updates = kwargs.pop("mass_updates", False)

        # If auto-updates are turned on, a cascade of updates to linked models will occur, but if they are turned off,
        # the update will be buffered, to be manually triggered later (e.g. upon completion of loading), which
        # mitigates repeated updates to the same record
        if (
            self.coordinator.auto_updates is False
            and mass_updates is False
        ):
            # When buffering only, apply the global label filters, to be remembered during mass autoupdate
            self.label_filters = self.coordinator.default_label_filters
            self.filter_in = self.coordinator.default_filter_in

            # Set the changed value triggering this update
            super().save(*args, **kwargs)
            self.coordinator.buffer_update(self)
            return
        elif self.coordinator.auto_updates:
            # If autoupdates are happening (and it's not a mass-autoupdate (assumed because mass_updates
            # can only be true if auto_updates is False)), set the label filters based on the currently set global
            # conditions so that only fields matching the filters will be updated.
            self.label_filters = self.coordinator.default_label_filters
            self.filter_in = self.coordinator.default_filter_in
        # Otherwise, we are performing a mass auto-update and want to update the previously set filter conditions

        # Update the fields that change due to the the triggering change (if any)
        # This only executes either when auto_updates or mass_updates is true - both cannot be true
        self.update_decorated_fields()

        # If the auto-update resulted in no change or if there exists stale buffer contents for objects that were
        # previously saved, it can produce an error about unique constraints.  TransactionManagementErrors should have
        # been handled before we got here so that this can proceed to effect the original change that prompted the
        # save.
        try:
            # This either saves both explicit changes and auto-update changes (when auto_updates is true) or it only
            # saves the auto-updated values (when mass_updates is true)
            super().save(*args, **kwargs)
        except (IntegrityError, ForeignKeyViolation) as uc:
            # If this is a unique constraint error during a mass autoupdate
            if mass_updates and (
                "violates foreign key constraint" in str(uc)
                or "duplicate key value violates unique constraint" in str(uc)
            ):
                # Errors about unique constraints during mass autoupdates are often due to stale buffer contents
                raise LikelyStaleBufferError(self)
            else:
                raise uc

        # We don't need to check mass_updates, because propagating changes during buffered updates is
        # handled differently (in a breadth-first fashion) to mitigate repeated updates of the same related record
        if self.coordinator.auto_updates and propagate:
            # Percolate changes up to the related models (if any)
            self.call_dfs_related_updaters()

    def delete(self, *args, **kwargs):
        """
        This is an override of the derived model's delete method that is being used here to automatically update
        maintained fields.
        """
        # Custom argument: propagate - Whether to propagate updates to related model objects - default True
        # Used internally. Do not supply unless you know what you're doing.
        propagate = kwargs.pop("propagate", True)
        # Custom argument: mass_updates - Whether auto-updating biffered model objects - default False
        # Used internally. Do not supply unless you know what you're doing.
        mass_updates = kwargs.pop("mass_updates", False)

        # Delete the record triggering this update
        super().delete(*args, **kwargs)  # Call the "real" delete() method.

        # If auto-updates are turned on, a cascade of updates to linked models will occur, but if they are turned off,
        # the update will be buffered, to be manually triggered later (e.g. upon completion of loading), which
        # mitigates repeated updates to the same record
        # mass_updates is checked for consistency, but perform_buffered_updates does not call delete()
        if (
            self.coordinator.auto_updates is False
            and mass_updates is False
        ):
            # When buffering only, apply the global label filters, to be remembered during mass autoupdate
            self.label_filters = self.coordinator.default_label_filters
            self.filter_in = self.coordinator.default_filter_in

            # self.buffer_parent_update()
            parents = self.get_parent_instances()
            for parent_inst in parents:
                self.coordinator.buffer_update(parent_inst)
            return
        elif self.coordinator.auto_updates:
            # If autoupdates are happening (and it's not a mass-autoupdate (assumed because mass_updates
            # can only be true if auto_updates is False)), set the label filters based on the currently set global
            # conditions so that only fields matching the filters will be updated.
            self.label_filters = self.coordinator.default_label_filters
            self.filter_in = self.coordinator.default_filter_in
        # Otherwise, we are performing a mass auto-update and want to update the previously set filter conditions

        if self.coordinator.auto_updates and propagate:
            # Percolate changes up to the parents (if any)
            self.call_dfs_related_updaters()

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

            class_name = cls.__name__

            # Register the class with the coordinator if not already registered
            if class_name not in MaintainedModelCoordinator.model_classes.keys():
                MaintainedModelCoordinator.model_classes[class_name] = cls.__class__
            # Register the updater with the coordinator
            MaintainedModelCoordinator.updater_list[class_name].append(func_dict)

            # No way to ensure supplied fields exist because the models aren't actually loaded yet, so while that would
            # be nice to handle here, it will have to be handled in MaintanedModel when objects are created

            # Provide some debug feedback
            if settings.DEBUG:
                msg = f"Added relation decorator {class_name} to update"
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

        These decorated functions are identified by the MaintainedModel class, whose save and delete methods override
        the parent model and call the decorated functions to update field supplied to the factory function.  It also
        propagates the updates to the linked dependent model's save methods (if the parent and/or child field name is
        supplied), the assumption being that a change to "this" record's maintained field necessitates a change to
        another maintained field in the linked parent record.  Parent and child field names should only be supplied if a
        change to "this" record means that related foields in parent/child records will need to be recomputed.  There is
        no need to supply parent/child field names if that is not the case.

        The generation input is an integer indicating the hierarchy level.  E.g. if there is no parent, `generation`
        should be 0.  Each subsequence generation should increment generation.  It is used to populate update_buffer
        when auto_updates is False, so that mass updates can be triggered after all data is loaded.

        Note that a class can have multiple fields to update and that those updates (according to their decorators) can
        trigger subsequent updates in different "parent"/"child" records.  If multiple update fields trigger updates to
        different parents, they are triggered in a depth-first fashion.  Child records are updated first, then parents.
        If a child links back to a parent, already-updated records prevent repeated/looped updates.  However, this only
        becomes relevant when the global variable `auto_updates` is False, mass database changes are made (buffering the
        auto-updates), and then auto-updates are explicitly triggered.

        Note, if there are many decorated methods updating different fields, and all of the "parent"/"child" fields are
        the same, only 1 of those decorators needs to set a parent field.
        """

        if update_field_name is None and (
            parent_field_name is None and generation != 0
        ):
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

            # No way to ensure supplied fields exist because the models aren't actually loaded yet, so while that would
            # be nice to handle here, it will have to be handled in MaintanedModel when objects are created

            # Add this info to our global updater_list
            MaintainedModelCoordinator.updater_list[class_name].append(func_dict)
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

    @classmethod
    def m2m_propagation_handler(cls, **kwargs):
        """
        Additions to M:M related models do not require a .save() to be called afterwards, thus additions like:
            peakgroup.compounds.add(cmpd)
        do not propagate a change to MSRun as is necessary for automatic field maintenance, expressly because
        peakgroup.save() is not called.  To deal with this, and trigger the necessary automatic updates of maintained
        fields, an m2m_changed signal is attached to all M:M fields in MaintainedModel.__init__ to tell us when a
        MaintainedModel has an M:M field that has been added to.  That causes this method to be called, and from here
        we can propagate the changes.
        """
        obj = kwargs.pop("instance", None)
        act = kwargs.pop("action", None)

        if (
            act.startswith("post_")
            and isinstance(obj, MaintainedModel)
            and obj.coordinator.auto_updates
        ):
            obj.call_dfs_related_updaters()

    @classmethod
    def get_coordinator(cls):
        return cls._get_current_coordinator()

    @classmethod
    def _get_current_coordinator(cls):
        if len(cls.data.coordinator_stack) > 0:
            # Get the current coordinator
            coordinator_getter = cls.data.coordinator_stack[-1]
            # Call the last coordinator on the stack
            return coordinator_getter()
        else:
            return cls._get_coordinator()

    @classmethod
    def _get_coordinator(cls):
        return cls.data.default_coordinator

    @classmethod
    def get_parent_deferred_coordinator(cls):
        # Return the parent coordinator from the stack whose mode is "deferred" and has a lesser ID or None
        # This assumes that perform_buffered_updates is called inside a deferred_autoupdates block after the yielded
        # code has finished its buffering.

        # Create a copy of the list that contains the same objects - so that if you return a coordinator, it is one
        # that's in the coordinator_stack - so if you change it, you change the object in the stack.  This is what we
        # want, so that we can move items from the current coordinator's buffer to the parent coordinator's buffer.
        parent_coordinators = cls.data.coordinator_stack[:]
        parent_coordinators.pop()

        # Traverse the parent coordinators from immediate parent to distant parent.  Note, the stack doesn't include the
        # default_coordinator, which is assumed to be mode "immediate".
        for coordinator in reversed(parent_coordinators):
            if coordinator.get_mode() == "deferred":
                return coordinator

        # Return None when there is no deferred parent (meaning that puffered autoupdates will be performed)
        return None

    @classmethod
    @contextmanager
    def custom_coordinator(cls, coordinator):
        """
        This method allows you to set a temporary coordinator using a context manager.  Under this context (using a with
        block), the supplied coordinator will be used instead of the default whenever a MaintainedModel object is
        instantiated.  These contexts can be nested.

        Use this method like this:
            deferred_filtered = MaintainedModelCoordinator(auto_update_mode="deferred")
            deferred_filtered
            with MaintainedModel.custom_coordinator()
        """
        # This assumes that the default_coordinator is in mode "immediate"
        if len(cls.data.coordinator_stack) == 0 and coordinator.buffer_size() > 0:
            raise UncleanBufferError()

        cls.data.coordinator_stack.append(coordinator)
        try:
            # This is all the code in the context
            # Any MaintainedModel object created in this context will get the last coordinator on the stack
            yield
        except Exception as e:
            # Empty the buffer just to be on the safe side.  This shouldn't technically be necessary since we are
            # popping it off the stack...
            coordinator.clear_update_buffer()
            raise e
        finally:
            cls.data.coordinator_stack.pop()

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

                # If the arguments to the defer_autoupdates decorator included a disable_opt_names (e.g. []"dry_run"]),
                # check the value of each option and change the mode to "disabled" if any of them are True.
                if (
                    disable_opt_names
                    and len(disable_opt_names) > 0
                ):
                    for disable_opt_name in disable_opt_names:
                        if (
                            disable_opt_name in kwargs.keys()
                            and kwargs[disable_opt_name]
                        ):
                            mode = "disabled"
                            break

                coordinator = MaintainedModelCoordinator(
                    auto_update_mode=mode,
                    label_filters=label_filters,
                    filter_in=filter_in,
                )

                with cls.custom_coordinator(coordinator):
                    try:
                        fn(*args, **kwargs)
                    except Exception as e:
                        # We do not want to perform_buffered_updates if the yielded code throws an exception, so we
                        # raise in order to not get to the perform_buffered_updates operation below.
                        raise e

                    if mode == "deferred":
                        # Only perform buffered updates once we've left the last deferred context
                        parent_deferred_coordinator = cls.get_parent_deferred_coordinator()
                        if parent_deferred_coordinator is not None:
                            # Transfer the buffer to the next-to-last deferred coordinator
                            for buffered_item in cls.coordinator.update_buffer:
                                parent_deferred_coordinator.buffer_update(buffered_item)
                        else:
                            # Note, the pre/post mass update funcs are ignored if deferring updates to parents, so that
                            # must be specified in every decorator, as nested decorators will not bubble the functions
                            # up the coordinator stack.
                            if pre_mass_update_func is not None:
                                pre_mass_update_func()
                            cls.coordinator.perform_buffered_updates()
                            if post_mass_update_func is not None:
                                post_mass_update_func()

            return wrapper

        return decorator

    def update_decorated_fields(self):
        """
        Updates every field identified in each MaintainedModel.setter decorator using the decorated function that
        generates its value.

        This uses 2 data members: self.label_filters and self.filter_in in order to determine which fields should be
        updated.  They are initially set when the object is created and refreshed when the object is saved to reflect
        the current filter conditions.  One exception of the refresh, is if performing a mass auto-update, in which
        case the filters the were in effect during buffering are used.
        """
        for updater_dict in self.get_my_updaters():
            update_fld = updater_dict["update_field"]
            update_label = updater_dict["update_label"]

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
                try:
                    update_fun = getattr(self, updater_dict["update_function"])
                    try:
                        old_val = getattr(self, update_fld)
                    except Exception as e:
                        if isinstance(e, TransactionManagementError):
                            raise e
                        warnings.warn(
                            f"{e.__class__.__name__} error getting current value of field [{update_fld}]: "
                            f"[{str(e)}].  Possibly due to this being triggered by a deleted record that is linked in "
                            "a related model's maintained field."
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

    def call_dfs_related_updaters(self, updated=None, mass_updates=False):
        if not updated:
            updated = []
        # Assume I've been called after I've been updated, so add myself to the updated list
        self_sig = f"{self.__class__.__name__}.{self.id}"
        updated.append(self_sig)
        updated = self.call_child_updaters(updated=updated, mass_updates=mass_updates)
        updated = self.call_parent_updaters(updated=updated, mass_updates=mass_updates)
        return updated

    def call_parent_updaters(self, updated, mass_updates=False):
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

                # Don't let the save call propagate.  Previously, I was relying on save returning the updated list, but
                # since .save() could be overridden by another class that doesn't return anything, I was getting back
                # None (at least, that's my guess as to why I was getting back None when I tried it).  So instead, I
                # implemented the propagation outside of the .save calls using the call_dfs_related_updaters call
                # below.
                parent_inst.save(propagate=False, mass_updates=mass_updates)

                # Propagate manually
                updated = parent_inst.call_dfs_related_updaters(updated=updated, mass_updates=mass_updates)

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

    def call_child_updaters(self, updated, mass_updates=False):
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
                child_inst.save(propagate=False, mass_updates=mass_updates)

                # Instead, we will propagate manually:
                updated = child_inst.call_dfs_related_updaters(
                    updated=updated,
                    mass_updates=mass_updates,
                )

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

    def get_my_update_fields(self):
        """
        Returns a list of update_fields of the current model that are marked via the MaintainedModel.setter
        decorators in the model.  Returns an empty list if there are none (e.g. if the only decorator in the model is
        the relation decorator on the class).
        """
        my_update_fields = []
        for updater_dict in self.get_my_updaters():
            if (
                "update_field" in updater_dict.keys()
                and updater_dict["update_field"]
            ):
                my_update_fields.append(updater_dict["update_field"])

        return my_update_fields

    def get_my_updaters(self):
        """
        Retrieves all the updater information of each decorated function of the calling model from the global
        updater_list variable.
        """
        return self.coordinator.get_updater_dicts_by_model_name(self.__class__.__name__)

    # def buffer_update(self):
    #     """
    #     This is called when MaintainedModel.save is called (if auto_updates is False), so that maintained fields can be
    #     updated after loading code finishes (by calling the global method: perform_buffered_updates).
    #     """

    #     # See if this class contains a field with a matching label (if a populated label_filters array was supplied)
    #     if self.label_filters is not None and len(self.label_filters) > 0:
    #         do_buffer = False
    #         for updater_dict in self.get_my_updaters():
    #             update_label = updater_dict["update_label"]

    #             # If there is a maintained field(s) in this model and...
    #             # If auto-updates are restricted to fields by their update_label and this field matches the label
    #             # filter criteria
    #             if (
    #                 # The update_label matches a filter-in label
    #                 (
    #                     self.filter_in
    #                     and update_label is not None
    #                     and update_label
    #                     in self.label_filters  # pylint: disable=unsupported-membership-test
    #                     # For the pylint disable, see: https://github.com/pylint-dev/pylint/issues/3045
    #                 )
    #                 # The update_label does not match a filter-out label
    #                 or (
    #                     not self.filter_in
    #                     and (
    #                         update_label is None
    #                         or update_label
    #                         not in self.label_filters  # pylint: disable=unsupported-membership-test
    #                         # For the pylint disable, see: https://github.com/pylint-dev/pylint/issues/3045
    #                     )
    #                 )
    #             ):
    #                 do_buffer = True
    #                 break

    #         # If there's not a field passing the filter criteria, do not buffer (return)
    #         if not do_buffer:
    #             return

    #     # Do not buffer if it's already buffered.  Note, this class isn't designed to support auto-updates in a
    #     # sepecific order.  All auto-update functions should use non-auto-update fields.
    #     if self.coordinator.buffering and self not in self.coordinator.update_buffer:
    #         self.coordinator.update_buffer.append(self)
    #     elif self.coordinator.buffering:
    #         # This allows the same object to be updated more than once (in the order encountered) if the fields to be
    #         # auto-updated in each instance, differ.  This can cause redundant updates (e.g. when a field matches the
    #         # filters in both cases), but given the possibility that update order may depend on the update of related
    #         # records, it's better to be on the safe side and do each auto-update, so...
    #         # If this is the same object but a different set of fields will be updated...
    #         # Note, Django model object equivalence (obj1 == obj2) compares primary key values
    #         for same_obj in [ubo for ubo in self.coordinator.update_buffer if ubo == self]:
    #             if (
    #                 same_obj.filter_in != self.filter_in
    #                 or same_obj.label_filters != same_obj.label_filters
    #             ):
    #                 self.coordinator.update_buffer.append(self)

    # def buffer_parent_update(self):
    #     """
    #     This is called when MaintainedModel.delete is called (if auto_updates is False), so that maintained fields can
    #     be updated after loading code finishes (by calling the global method: perform_buffered_updates)
    #     """
    #     if self.coordinator.buffering:
    #         parents = self.get_parent_instances()
    #         for parent_inst in parents:
    #             self.coordinator.update_buffer.append(parent_inst)

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
        debugging effort in the future by encapsulating what I have learned to provide insights on how best to address
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
            "block, and after the atomic transaction block, call perform_buffered_updates() to make the updates.  If "
            "this is a warning, note that auto-updates can be fixed afterwards by running:\n\n"
            "\tpython manage.py rebuild_maintained_fields\n"
        )

        warning_str += f"\n{explanation}\nThe error that occurred: [{str(tme)}]."

        warnings.warn(warning_str)

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
        try:
            obj_str = str(model_object)
        except Exception:
            # Displaying the offending object is optional, so catch any error
            obj_str = "unknown"
        message = (
            f"Autoupdate of the {model_object.__class__.__name__} model's fields [{', '.join(updater_flds)}] in the "
            f"database failed for record {obj_str}.  Potential causes:\n"
            "\t1. The record was created and deleted before the buffered update (a catch for the exception should be "
            "added and ignored).\n"
            "\t2. The autoupdate buffer is stale and auto-updates are being attempted on non-existent records.  Find "
            "a previous call to a loader that performs mass auto-updates and ensure that clear_update_buffer() is "
            "called.\n"
            f"The triggering {err.__class__.__name__} exception: [{err}]."
        )
        super().__init__(message)


class StaleAutoupdateMode(Exception):
    def __init__(self):
        message = (
            "Autoupdate mode enabled during a mass update of maintained fields.  Automated update of the global "
            "variable mass_updates may have been interrupted during execution of "
            "perform_buffered_updates."
        )
        super().__init__(message)


class LikelyStaleBufferError(Exception):
    def __init__(self, model_object):
        message = (
            f"Autoupdates to {model_object.__class__.__name__} encountered a unique constraint violation.  Note, this "
            "often happens when the auto-update buffer contains stale records.  Be sure the buffer is empty before "
            "calling disable_autoupdates() (if it is intended to be empty)."
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


class InitFiltersAfterDisablingAutoupdates(Exception):
    def __init__(self, message=None):
        if message is None:
            message = (
                "Custom filtering conditions must be initialized (using init_autoupdate_label_filters()) after "
                "autoupdates are disabled (using disable_autoupdates()).  If custom filters are used by one loading "
                "script, those filters must be cleared at the end of that script so that they are not unintentionally "
                "applied to the next loading script."
            )
        super().__init__(message)


class ClearFiltersBeforeEnablingAutoupdates(Exception):
    def __init__(self, message=None):
        if message is None:
            message = (
                "Custom filtering conditions must be cleared (using init_autoupdate_label_filters()) before "
                "autoupdates are enabled (using enable_autoupdates()).  If custom filters are used by one loading "
                "script, those filters must be cleared at the end of that script so that they are not unintentionally "
                "applied to the next loading script."
            )
        super().__init__(message)
