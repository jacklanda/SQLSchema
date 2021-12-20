# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com

"""
sql_parse.exceptions
~~~~~~~~~~~~~~~~~~~~

This module contains the set of sql_parse's exceptions.
"""


class SQLParseError(Exception):
    """An ambiguous exception that occurred while parsing SQL statements."""


class RegexMatchError(SQLParseError):
    """An exception that occurred while matching failed with regex."""


class DefColumnError(SQLParseError):
    """A column definition error occurred."""


class RefUnfoundError(SQLParseError):
    """A references unfound error occurred."""


class UnknownVariantError(SQLParseError):
    """An unknown or unhandled variant error occurred."""


if __name__ == "__main__":
    pass
