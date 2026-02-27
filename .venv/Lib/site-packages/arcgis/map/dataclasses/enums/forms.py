from enum import Enum


class GroupInitialState(Enum):
    """
    Defines if the group should be expanded or collapsed when the form is initially displayed. If not provided, the default value is `expanded`
    """

    collapsed = "collapsed"
    expanded = "expanded"


class TimeResolution(Enum):
    """
    The resolution identifier. If not specified default is 'minutes'.
    """

    milliseconds = "milliseconds"
    minutes = "minutes"
    seconds = "seconds"


class TextFormat(Enum):
    """
    Defines language of `text` property. Default is `plain-text`.
    """

    markdown = "markdown"
    plain_text = "plain-text"


class AttachmentAssociationType(Enum):
    """
    Indicates if existing attachments should be associated with this element. `any` will associate all existing attachments to this form element; this can be the only `formAttachmentElement` within the form. `exactOrNone` will associate any attachments with the associated `keyword` and any attachments with no keyword defined; only one form element can have this value defined. `exact` will associate only attachments that include the specific keyword.
    """

    any = "any"
    exact = "exact"
    exact_or_none = "exactOrNone"


class InputMethodType(Enum):
    """
    The input method used to attach an audio.
    """

    any = "any"
    capture = "capture"
    upload = "upload"
