# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com

"""
sql_parse.exceptions
~~~~~~~~~~~~~~~~~~~~

This module contains the set of sql_parse's exceptions.
"""

# Self-defined Errors / Exceptions


class SQLParseError(Exception):
    """An ambiguous exception that occurred while parsing SQL statements."""


class ParseStageFailedError(SQLParseError):
    """A parse stage failed error occurred."""


class RegexMatchError(SQLParseError):
    """A regex match failed error occurred."""


class DefColumnError(SQLParseError):
    """A column definition error occurred."""


class RefUnfoundError(SQLParseError):
    """A references unfound error occurred."""


class UnknownVariantError(SQLParseError):
    """An unknown or unhandled variant error occurred."""


# Self-defined Warnings
# TODO


if __name__ == "__main__":
    pass
