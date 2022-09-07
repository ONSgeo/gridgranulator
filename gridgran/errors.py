"""Custom exceptions for gridgran"""


class DataFrameCouldNotBeSeparatedException(Exception):
    pass


class DataFrameNotOverDisclosureLimitException(Exception):
    pass


class ClassificationMismatchException(Exception):
    pass
