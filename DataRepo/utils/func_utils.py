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
