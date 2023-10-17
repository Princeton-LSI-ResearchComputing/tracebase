import queue
import threading
import time


class TestThreadRunner(threading.Thread):
    """
    This class allows you to start a child thread to run a function, and also specify a queue.Queue object into which
    exceptions should be queued and a time to wait after queuing an axception to allow the parent time to collect it.
    Note that the child function must give the parent time to run any tests while the child is running.
    """

    def __init__(self, exc_queue, child_func, exc_delay=0.2):
        threading.Thread.__init__(self)
        self.exc_queue = exc_queue
        self.child_func = child_func
        self.exc_delay = exc_delay

    def run(self):
        try:
            self.child_func()
        except Exception as e:
            self.exc_queue.put(e)
            # Sleep to allow the parent time to collect the queue contents
            time.sleep(self.exc_delay)


def run_in_child_thread(child_func, check_interval=0.1):
    exc_queue = queue.Queue()
    thread_runner = TestThreadRunner(
        exc_queue, child_func, exc_delay=2 * check_interval
    )
    thread_runner.start()

    # Collect possible exception
    check_times = 0
    exc = None
    while True:
        try:
            check_times += 1
            # Try to retrieve an exception from the child
            exc = exc_queue.get(block=False)
        except queue.Empty:
            # Skip down to check if the child is still alive
            pass

        # Attempt to collect the child thread with a 0.1 second timeout
        thread_runner.join(check_interval)
        # Now check if the child thread is still running
        if thread_runner.is_alive():
            continue
        else:
            # The child is done, so let's break out of this loop
            break

    if exc is not None:
        raise ChildException(exc)
    elif check_times == 1:
        raise PossiblePrematureChildDeath(check_interval)


def run_parent_during_child_thread(
    parent_func, child_func, parent_start_delay=0.1, check_interval=0.1
):
    """
    This method allows you to run some code in the parent thread while a child thread is running.  It reports exceptions
    from the child as well as the parent, in the parent(/current) thread.
    """
    exc_queue = queue.Queue()
    thread_runner = TestThreadRunner(
        exc_queue, child_func, exc_delay=2 * check_interval
    )
    thread_runner.start()

    if parent_start_delay and parent_start_delay > 0.0:
        time.sleep(parent_start_delay)

    # Run the parent function while the child runs
    parent_func()

    # Collect possible exception
    check_times = 0
    exc = None
    while True:
        try:
            check_times += 1
            # Try to retrieve an exception from the child
            exc = exc_queue.get(block=False)
        except queue.Empty:
            # Skip down to check if the child is still alive
            pass

        # Attempt to collect the child thread with a 0.1 second timeout
        thread_runner.join(check_interval)
        # Now check if the child thread is still running
        if thread_runner.is_alive():
            continue
        else:
            # The child is done, so let's break out of this loop
            break

    if exc is not None:
        raise ChildException(exc)
    elif check_times == 1:
        raise PossibleChildDeathBeforeParent(parent_start_delay, check_interval)


def run_child_during_parent_thread(parent_func, child_func, check_interval=0.1):
    """
    This method allows you to run some code in a child thread while the parent is running.  The child is started first,
    so it must sleep a biut so that the parent can start, and then the child resumes during the parent.  The parent must
    run long enough for the child to execute during the run of the parent.  It reports exceptions from the child as well
    as the parent, in the parent(/current) thread.
    """
    exc_queue = queue.Queue()
    thread_runner = TestThreadRunner(
        exc_queue, child_func, exc_delay=2 * check_interval
    )
    thread_runner.start()

    # Run the parent function while the child runs
    parent_func()

    # Collect possible exception
    check_times = 0
    exc = None
    while True:
        try:
            check_times += 1
            # Try to retrieve an exception from the child
            exc = exc_queue.get(block=False)
        except queue.Empty:
            # Skip down to check if the child is still alive
            pass

        # Attempt to collect the child thread with a 0.1 second timeout
        thread_runner.join(check_interval)
        # Now check if the child thread is still running
        if thread_runner.is_alive():
            continue
        else:
            # The child is done, so let's break out of this loop
            break

    if exc is not None:
        raise ChildException(exc)
    elif check_times == 1:
        raise PossiblePrematureChildDeath(check_interval)


class ChildException(Exception):
    def __init__(self, exc):
        message = f"Exception occurred in child thread: {exc}"
        super().__init__(message)


class PossibleChildDeathBeforeParent(Exception):
    def __init__(self, psd, ci):
        message = (
            "The child process may have exited with an exception before the parent had a chance to collect it.  "
            f"Please ensure that the child runs longer than the parent_start_delay ({psd}) and 2 * check_interval "
            f"({ci}) to ensure that exceptions are caught by the parent."
        )
        super().__init__(message)


class PossiblePrematureChildDeath(Exception):
    def __init__(self, ci):
        message = (
            "The child process may have exited with an exception before the parent had a chance to collect it.  "
            f"Please ensure that the child runs longer than 2 * check_interval ({ci}) to ensure that exceptions are "
            "caught by the parent."
        )
        super().__init__(message)
