from django.conf import settings


class MultiDBMixin:
    """
    This class is for temporary use and will be removed when there is a better way of handling user validation of load
    submissions.  It simply provides a single function that can be used to retrieve the ModelState object from an
    instance so that it can be applied to a new instance.  This allows model instance methods to make new database
    queries on the same database as is currently set by any preceding .using(db) call.
    """

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
