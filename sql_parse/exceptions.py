# -*- coding: utf-8 -*-
# @author: Yang Liu
# @email: v-yangliu4@microsoft.com

"""
sql_parse.exceptions
~~~~~~~~~~~~~~~~~~~~

This module contains the set of sql_parse's exceptions.
"""

from inspect import getframeinfo, stack


# Self-defined Errors / Exceptions


class SQLParseError(Exception):
    """An ambiguous exception that occurred while parsing SQL statements."""


class ParseStageFailedError(SQLParseError):
    """A parse stage failed error occurred."""

    def __init__(self, msg="Parse stage failed error!"):
        last_frame_info = getframeinfo(stack()[1][0])
        self.msg = f"{last_frame_info.filename}:{last_frame_info.lineno}, {msg}"
        super().__init__(self.msg)


class RegexMatchError(SQLParseError):
    """A regex match failed error occurred."""

    def __init__(self, msg="Regex match error!"):
        last_frame_info = getframeinfo(stack()[1][0])
        self.msg = f"{last_frame_info.filename}:{last_frame_info.lineno}, {msg}"
        super().__init__(self.msg)


class DefColumnError(SQLParseError):
    """A column definition error occurred."""

    def __init__(self, msg="Column definition error!"):
        last_frame_info = getframeinfo(stack()[1][0])
        self.msg = f"{last_frame_info.filename}:{last_frame_info.lineno}, {msg}"
        super().__init__(self.msg)


class RefUnfoundError(SQLParseError):
    """A references unfound error occurred."""

    def __init__(self, msg="Unfound references error!"):
        last_frame_info = getframeinfo(stack()[1][0])
        self.msg = f"{last_frame_info.filename}:{last_frame_info.lineno}, {msg}"
        super().__init__(self.msg)


class UnknownVariantError(SQLParseError):
    """An unknown or unhandled variant error occurred."""

    def __init__(self, msg="Unknown / Unhandled variant error!"):
        last_frame_info = getframeinfo(stack()[1][0])
        self.msg = f"{last_frame_info.filename}:{last_frame_info.lineno}, {msg}"
        super().__init__(self.msg)


# Self-defined Warnings
# TODO


if __name__ == "__main__":
    def test_sql_parse_error(): raise SQLParseError()

    def test_parse_stage_failed_error(): raise ParseStageFailedError()

    def test_regex_match_error(): raise RegexMatchError()

    def test_def_column_error(): raise DefColumnError()

    def test_ref_unfound_error(): raise RefUnfoundError()

    def test_unknown_variant_error(): raise UnknownVariantError()

    try:
        test_sql_parse_error()
    except Exception as e:
        print(e)

    try:
        test_parse_stage_failed_error()
    except Exception as e:
        print(e)

    try:
        test_regex_match_error()
    except Exception as e:
        print(e)

    try:
        test_def_column_error()
    except Exception as e:
        print(e)

    try:
        test_ref_unfound_error()
    except Exception as e:
        print(e)

    try:
        test_unknown_variant_error()
    except Exception as e:
        print(e)
