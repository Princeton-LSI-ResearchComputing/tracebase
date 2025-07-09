import cProfile
import io
import pstats


def reduceuntil(function, untilfunction, sequence, initial=None):
    """Like functools.reduce, but with a condition function that stops the reduction early if a condition is met.

    Example:
        input_list = [2, 2, 2, 2, 2, 3, 4, 5, 6, 7]
        max_unique_len = 2
        reduceuntil(
            lambda ulst, val: ulst + [val] if val not in ulst else ulst,
            lambda val: len(val) >= max_unique_len,
            input_list,
            [],
        )
        # Output: [2, 3]
    Args:
        function (Callable): See functools.reduce (same)
        untilfunction (Callable): Takes the accumulating result and returns a bool that should be True if the reduction
            should stop and False if it should keep going.
        sequence (Iterable): See functools.reduce (same)
        initial (Any): See functools.reduce (same)
    Exceptions:
        TypeError - when initial is invalid and needed.
    Returns:
        value (Any): The final accumulated output of function
    """

    it = iter(sequence)

    if initial is None:
        try:
            value = next(it)
        except StopIteration:
            raise TypeError(
                "reduceuntil() of empty sequence with no initial value"
            ) from None
    else:
        value = initial

    for element in it:
        value = function(value, element)
        if untilfunction(value):
            break

    return value


def profile_method(sort_by="cumulative", n_rows=None, outfile=None):
    """A decorator to profile a method using cProfile.  Intended for use profiling view get methods, since the django
    debug toolbar is limited and slow.

    Example:
        class myClassView:
            @profile_method()
            def get(self, request, *args, **kwargs):
                pass
    Args:
        sort_by (str) ["cumulative"] {"cumulative", "time", "calls"}: The key to sort the profiling results by.
        n_rows (Optional[int]) [all]: Limit the number of rows in the output.
        outfile (Optional[str]) [printed]: Path to a file to save the profiling results.
    Exceptions:
        None
    Returns:
        decorator
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            pr = cProfile.Profile()
            pr.enable()
            result = func(*args, **kwargs)
            pr.disable()

            s = io.StringIO()
            ps = pstats.Stats(pr, stream=s).sort_stats(sort_by)
            if n_rows:
                ps.print_stats(n_rows)
            else:
                ps.print_stats()

            if outfile:
                with open(outfile, "w") as f:
                    f.write(s.getvalue())
            else:
                print(s.getvalue())

            return result

        return wrapper

    return decorator
