from enum import Enum


class ArcadeReturnType(Enum):
    """
    Return type of the Arcade expression. This can be determined by the authoring client by executing the expression using a sample feature(s), although it can be corrected by the user.
    """

    boolean = "boolean"
    date = "date"
    date_only = "dateOnly"
    number = "number"
    string = "string"
    time = "time"
    dictionary = "dictionary"


class StringFieldOption(Enum):
    """
    A string determining what type of input box editors see when editing the
    field. Applies only to string fields. Not applicable to Arcade expressions.
    """

    richtext = "richtext"
    textarea = "textarea"
    textbox = "textbox"


class MediaType(Enum):
    """
    A string defining the type of media.
    """

    barchart = "barchart"
    columnchart = "columnchart"
    image = "image"
    linechart = "linechart"
    piechart = "piechart"


class AttachmentDisplayType(Enum):
    """
    This property applies to elements of type `attachments`. A string value
    indicating how to display the attachment. If `list` is specified, attachments
    show as links. If `preview` is specified, attachments expand to the width of
    the pop-up. The default `auto` setting allows applications to choose the most
    suitable default experience.
    """

    auto = "auto"
    list = "list"
    preview = "preview"


class Order(Enum):
    """
    Indicates whether features are sorted in ascending or descending order of
    the field values.
    """

    asc = "asc"
    desc = "desc"


class AssociationTypes(Enum):
    attachment = "attachment"
    connectivity = "connectivity"
    containment = "container"
    content = "content"
    structural = "structure"
