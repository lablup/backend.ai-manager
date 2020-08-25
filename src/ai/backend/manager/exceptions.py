class InvalidArgument(Exception):
    """
    An internal exception class to represent invalid arguments in internal APIs.
    This is wrapped as InvalidAPIParameters in web request handlers.
    """
    pass
