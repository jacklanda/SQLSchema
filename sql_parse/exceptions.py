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

    def __init__(self, msg="SQL parsing error!"):
        last_frame_info = getframeinfo(stack()[1][0])
        self.msg = f"{last_frame_info.filename}:{last_frame_info.lineno}, {msg}"
        super().__init__(self.msg)


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
    pass
