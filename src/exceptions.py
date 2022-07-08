class Error(Exception):
    """Base class for exception in this module."""


class InvalidArgumentError(Error):

    def __init__(self, message: str) -> None:
        self.message = message
