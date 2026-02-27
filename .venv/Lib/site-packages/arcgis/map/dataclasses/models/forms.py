"""
A module for managing forms in the ArcGIS platform
"""

from __future__ import annotations  # Enables postponed evaluation of type hints

from .base_model import BaseModel, common_config
from pydantic import Field
from typing import Literal
from ..enums.popups import ArcadeReturnType
from ..enums.forms import (
    GroupInitialState,
    TimeResolution,
    TextFormat,
    AttachmentAssociationType,
    InputMethodType,
)
from .popups import AssociationType, OrderByField


class InheritedDomain(BaseModel):
    """
    This domain applies to domains on subtypes. It implies that the domain for a field at the subtype level is the same as the domain for the field at the layer level.
    """

    type: Literal["inherited"] = Field(
        "inherited", description="String value representing the domain type."
    )


class RangeDomain(BaseModel):
    """
    Range domain specifies a range of valid values for a field.
    """

    model_config = common_config

    name: str | None = Field(None, description="The domain name.")
    range: list[float] | list[int] = Field(
        ...,
        description="The first element is the minValue and the second element is the maxValue.",
        max_length=2,
        min_length=2,
    )
    type: Literal["range"] = Field(
        "range", description="String value representing the domain type."
    )


class UniqueCodedValue(BaseModel):
    """
    A set of valid coded values with unique names.
    """

    code: float | str = Field(
        ..., description="The value stored in the feature attribute."
    )
    name: str = Field(..., description="User-friendly name for what the code means.")


class CodedValue(BaseModel):
    """
    The coded value domain includes both the actual value that is stored in a database and a description of what the code value means.
    """

    model_config = common_config

    coded_values: list[UniqueCodedValue] = Field(
        ..., alias="codedValues", description="A set of valid values with unique names."
    )
    name: str | None = Field(None, description="The domain name.")
    type: Literal["codedValue"] = Field(
        "codedValue", description="String value representing the domain type."
    )


class FormExpressionInfo(BaseModel):
    """
    Arcade expression used in the form.
    """

    model_config = common_config

    expression: str | None = Field(None, description="The Arcade expression.")
    name: str | None = Field(None, description="Unique identifier for the expression.")
    return_type: ArcadeReturnType | None = Field(
        None,
        alias="returnType",
        description="Return type of the Arcade expression. This can be determined by the authoring client by executing the expression using a sample feature(s), although it can be corrected by the user.",
    )
    title: str | None = Field(None, description="Title of the expression.")


class FormAudioInput(BaseModel):
    """
    Defines that an audio file can be attached.
    """

    model_config = common_config

    input_method: InputMethodType | None = Field(
        InputMethodType.any,
        alias="inputMethod",
        description="The input method used to attach an audio.",
    )
    max_duration: int | None = Field(
        None,
        alias="maxDuration",
        description="The maximum duration of the audio file in seconds. If not provided, there is no maximum length.",
    )
    type: Literal["audio"] = Field("audio", description="The input type identifier.")


class FormDocumentInput(BaseModel):
    """
    Defines that a document file can be attached.
    """

    model_config = common_config

    max_file_size: int | None = Field(
        None,
        alias="maxFileSize",
        description="The maximum file size in megabytes. If not provided, there is no maximum size.",
    )
    type: Literal["document"] = Field(
        "document", description="The input type identifier."
    )


class FormImageInput(BaseModel):
    """
    Defines that an image file should be attached.
    """

    model_config = common_config

    input_method: InputMethodType | None = Field(
        InputMethodType.any,
        alias="inputMethod",
        description="The input method used to attach an image.",
    )
    max_image_size: int | None = Field(
        None,
        alias="maxImageSize",
        description="Number of pixels on the longest edge depending on orientation. Larger images will be resized and aspect ratio is maintained. If `maxImageSize` is not specified, images will not be resized.",
    )
    type: Literal["image"] = Field("image", description="The input type identifier.")


class FormSignatureInput(BaseModel):
    """
    Defines that a signature should be captured and attached.
    """

    model_config = common_config

    input_method: InputMethodType | None = Field(
        InputMethodType.any,
        alias="inputMethod",
        description="The input method used to attach a signature.",
    )
    type: Literal["signature"] = Field(
        "signature", description="The input type identifier."
    )


class FormVideoInput(BaseModel):
    """
    Defines that a video file should be attached.
    """

    model_config = common_config

    input_method: InputMethodType | None = Field(
        InputMethodType.any,
        alias="inputMethod",
        description="The input method used to attach a video.",
    )
    max_duration: int | None = Field(
        None,
        alias="maxDuration",
        description="The maximum duration of the video file in seconds. If not provided, there is no maximum length.",
    )
    type: Literal["video"] = Field("video", description="The input type identifier.")


class FormAttachmentInput(BaseModel):
    """
    Defines that any supported file can be attached.
    """

    model_config = common_config

    attachment_association_type: AttachmentAssociationType | None = Field(
        AttachmentAssociationType.exact,
        alias="attachmentAssociationType",
        description="Indicates if existing attachments should be associated with this element. `any` will associate all existing attachments to this form element; this can be the only `formAttachmentElement` within the form. `exactOrNone` will associate any attachments with the associated `keyword` and any attachments with no keyword defined; only one form element can have this value defined. `exact` will associate only attachments that include the specific keyword.",
    )
    input_types: list[
        FormAudioInput
        | FormDocumentInput
        | FormImageInput
        | FormSignatureInput
        | FormVideoInput
    ] = Field(
        ...,
        alias="inputTypes",
        description="An array of input types that are allowed for the attachment.",
    )
    type: Literal["attachment"] = Field(
        "attachment", description="The input type identifier."
    )


class FormBarcodeScannerInput(BaseModel):
    """
    Defines the desired user interface is a barcode or QR code scanner. If the client does not support barcode scanning, a single-line text box should be used.
    """

    model_config = common_config

    max_length: int | None = Field(
        None,
        alias="maxLength",
        description="This represents the maximum number of characters allowed. This only applies for string fields. If not supplied, the value is derived from the length property of the referenced field in the service.",
    )
    min_length: int | None = Field(
        None,
        alias="minLength",
        description="This represents the minimum number of characters allowed. This only applies for string fields. If not supplied, the value is 0, meaning there is no minimum constraint.",
    )
    type: Literal["barcode-scanner"] = Field(
        "barcode-scanner", description="The input type identifier."
    )


class FormComboBoxInput(BaseModel):
    """
    Defines the desired user interface is a list of values in a drop-down that supports typing to filter. Only one value can be selected at a time.
    """

    model_config = common_config

    no_value_option_label: str | None = Field(
        None,
        alias="noValueOptionLabel",
        description="The text used to represent a null value.",
    )
    show_no_value_option: bool | None = Field(
        None,
        alias="showNoValueOption",
        description="This property only applies to fields that support null values. It indicates whether to display a null value option. If not provided, the default value is `true`.",
    )
    type: Literal["combo-box"] = Field(
        "combo-box", description="The input type identifier."
    )


class FormDatePickerInput(BaseModel):
    """
    Defines the desired user interface is a date picker.
    """

    model_config = common_config

    max: str | None = Field(
        None,
        description="The maximum date to allow. The number represents an ISO-8601 date.",
    )
    min: str | None = Field(
        None,
        description="The minimum date to allow. The number represents an ISO-8601 date.",
    )
    type: Literal["date-picker"] = Field(
        "date-picker", description="The input type identifier."
    )


class FormDatetimePickerInput(BaseModel):
    """
    Defines the desired user interface is a calendar date picker.
    """

    model_config = common_config

    include_time: bool | None = Field(
        None,
        alias="includeTime",
        description="Indicates if the datetime picker should provide an option to select the time. If not provided, the default value is `false`.",
    )
    max: int | None = Field(
        None,
        description="The maximum date to allow. The number represents the number of milliseconds since epoch (January 1, 1970) in UTC.",
    )
    min: int | None = Field(
        None,
        description="The minimum date to allow. The number represents the number of milliseconds since epoch (January 1, 1970) in UTC.",
    )
    type: Literal["datetime-picker"] = Field(
        "datetime-picker", description="The input type identifier."
    )


class FormRadioButtonsInput(BaseModel):
    """
    Defines the desired user interface is a radio button group.
    """

    model_config = common_config

    no_value_option_label: str | None = Field(
        None,
        alias="noValueOptionLabel",
        description="The text used to represent a null value.",
    )
    show_no_value_option: bool | None = Field(
        None,
        alias="showNoValueOption",
        description="This property only applies to fields that support null values. It indicates whether to display a null value option. If not provided, the default value is `true`.",
    )
    type: Literal["radio-buttons"] = Field(
        "radio-buttons", description="The input type identifier."
    )


class FormSwitchInput(BaseModel):
    """
    Defines a desired user interface to present a binary switch, or toggle. This should be used when selecting between two options.
    """

    model_config = common_config

    off_value: int | str = Field(
        ..., alias="offValue", description="The coded value when switch state is `off`."
    )
    on_value: int | str = Field(
        ..., alias="onValue", description="The coded value when switch state is `on`."
    )
    type: Literal["switch"] = Field("switch", description="The input type identifier.")


class FormTextAreaInput(BaseModel):
    """
    Defines the desired user interface is a multi-line text area.
    """

    model_config = common_config

    max_length: int | None = Field(
        None,
        alias="maxLength",
        description="This represents the maximum number of characters allowed. If not supplied, the value is derived from the length property of the referenced field in the service.",
    )
    min_length: int | None = Field(
        None,
        alias="minLength",
        description="This represents the minimum number of characters allowed. If not supplied, the value is 0, meaning there is no minimum constraint.",
    )
    type: Literal["text-area"] = Field(
        "text-area", description="The input type identifier."
    )


class FormTextBoxInput(BaseModel):
    """
    Defines the desired user interface is a single-line text box.
    """

    model_config = common_config

    max_length: int | None = Field(
        None,
        alias="maxLength",
        description="This represents the maximum number of characters allowed. This only applies for string fields. If not supplied, the value is derived from the length property of the referenced field in the service.",
    )
    min_length: int | None = Field(
        None,
        alias="minLength",
        description="This represents the minimum number of characters allowed. This only applies for string fields. If not supplied, the value is 0, meaning there is no minimum constraint.",
    )
    type: Literal["text-box"] = Field(
        "text-box", description="The input type identifier."
    )


class FormTimeInput(BaseModel):
    """
    Defines the desired user interface is a time picker.
    """

    model_config = common_config

    max: str | None = Field(
        None,
        description="The maximum time to allow. The number represents an extended ISO-8601 time.",
    )
    min: str | None = Field(
        None,
        description="The minimum time to allow. The number represents an extended ISO-8601 time.",
    )
    time_resolution: TimeResolution | None = Field(
        TimeResolution.minutes.value,
        alias="timeResolution",
        description="The resolution identifier. If not specified default is 'minutes'.",
    )
    type: Literal["time-picker"] = Field(
        "time-picker", description="The input type identifier."
    )


class FormTimestampOffsetPickerInput(BaseModel):
    """
    Defines the desired user interface is a calendar date and time picker with a time offset.
    """

    model_config = common_config
    include_time_offset: bool | None = Field(
        True,
        alias="includeTimeOffset",
        description="Indicates if the timestampoffset picker should provide an option to select the timeoffset. If not provided, the default value is `true`.",
    )
    max: str | None = Field(
        None,
        description="The maximum timestampoffset to allow. The number represents an ISO-8601 date with a time offset.",
    )
    min: str | None = Field(
        None,
        description="The minimum timestampoffset to allow. The number represents an ISO-8601 date with a time offset.",
    )
    time_resolution: TimeResolution | None = Field(
        TimeResolution.minutes.value,
        alias="timeResolution",
        description="The resolution identifier. If not specified default is 'minutes'.",
    )
    type: Literal["timestampoffset-picker"] = Field(
        "timestampoffset-picker", description="The input type identifier."
    )


class FormFieldElement(BaseModel):
    """
    Defines how a field in the dataset participates in the form.
    """

    model_config = common_config
    description: str | None = Field(
        None, description="A string that describes the element in detail."
    )
    domain: CodedValue | InheritedDomain | RangeDomain | None = Field(
        None,
        description="The domain to apply to this field. If defined, it takes precedence over domains defined in field, type, or subtype.",
    )
    editable_expression: str | None = Field(
        None,
        alias="editableExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true`, the element is editable. When the expression evaluates to `false` the element is not editable. If the referenced field is not editable, the editable expression is ignored and the element is not editable.",
    )
    field_name: str | None = Field(
        None,
        alias="fieldName",
        description="A string containing the field name as defined by the feature layer.",
    )
    hint: str | None = Field(
        None,
        description="A string representing placeholder text. This only applies for input types that support text or numeric entry.",
    )
    input_type: (
        FormBarcodeScannerInput
        | FormComboBoxInput
        | FormDatePickerInput
        | FormDatetimePickerInput
        | FormRadioButtonsInput
        | FormSwitchInput
        | FormTextAreaInput
        | FormTextBoxInput
        | FormTimeInput
        | FormTimestampOffsetPickerInput
        | None
    ) = Field(
        ...,
        alias="inputType",
        description="The input user interface to use for the element.",
    )
    label: str | None = Field(
        None,
        description="A string indicating what the element represents. If not supplied, the label is derived from the alias property in the referenced field in the service.",
    )
    required_expression: str | None = Field(
        None,
        alias="requiredExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true` and the element is visible, the element must have a valid value in order for the feature to be created or edited. When the expression evaluates to `false` the element is not required. If no expression is provided, the default behavior is that the element is not required. If the referenced field is non-nullable, the required expression is ignored and the element is always required.",
    )
    type: Literal["field"] = Field(
        "field", description="A string indicating which type of element to use."
    )
    value_expression: str | None = Field(
        None,
        alias="valueExpression",
        description="A reference to an Arcade expression that returns a date, number, or string value.  When this expression evaluates the value of the field will be updated to the result.  This expression is only evaluated when `editableExpression` (if defined) is false but the field itself allows edits.",
    )
    visibility_expression: str | None = Field(
        None,
        alias="visibilityExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true`, the element is displayed. When the expression evaluates to `false` the element is not displayed. If no expression is provided, the default behavior is that the element is displayed. Care must be taken when defining a visibility expression for a non-nullable field i.e. to make sure that such fields either have default values or are made visible to users so that they can provide a value before submitting the form.",
    )


class FormUtilityNetworkAssociationsElement(BaseModel):
    """
    Defines how a utility network association field in the dataset participates in the form.
    When present in the form, the user may have the option to view related utility network associations.
    """

    model_config = common_config

    association_types: list[AssociationType] = Field(
        ...,
        alias="associationTypes",
        description="An array of AssociationTypes objects that represent the types of associations to display in the pop-up.",
    )
    description: str | None = Field(
        None, description="A string that describes the element in detail."
    )
    display_count: int | None = Field(
        None,
        alias="displayCount",
        description="An integer that indicates the maximum number of records to display per layer.",
    )
    editable_expression: str | None = Field(
        None,
        alias="editableExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true`, the element is editable. When the expression evaluates to `false` the element is not editable.",
    )
    label: str | None = Field(
        None, description="A string value indicating what the element represents."
    )
    type: Literal["utilityNetworkAssociation"] = Field(
        "utilityNetworkAssociation",
        description="String value indicating which type of element to use.",
    )
    visibility_expression: str | None = Field(
        None,
        alias="visibilityExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true`, the element is displayed. When the expression evaluates to `false` the element is not displayed. If no expression is provided, the default behavior is that the element is displayed.",
    )


class FormAttachmentElement(BaseModel):
    """
    Defines how one or more attachments can participate in the form. When present in the form, the user has the ability to upload an attachment specific to the form element.
    """

    model_config = common_config

    allow_user_rename: bool | None = Field(
        True,
        alias="allowUserRename",
        description="A boolean value indicating whether the user can rename the attachment. If not provided, the default value is `true`.",
    )
    attachment_keyword: str = Field(
        ...,
        alias="attachmentKeyword",
        description="A string to identify the attachment(s). When a file is attached using the form, this property is used to set the value of the keywords field for the attachment. When a form is displaying existing attachments, this property is used to query attachments using an exact match on the keywords field.",
    )
    description: str | None = Field(
        None, description="A string that describes the element in detail."
    )
    display_file_name: bool | None = Field(
        False,
        alias="displayFileName",
        description="A boolean value indicating whether the file name is displayed. If not provided, the default value is `false`.",
    )
    editable_expression: str | None = Field(
        None,
        alias="editableExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true`, the element is editable. When the expression evaluates to `false` the element is not editable.",
    )
    file_name_expression: str | None = Field(
        None,
        alias="fileNameExpression",
        description="Determines the name of a new attachment. If not specified, a unique name will be generated using the `attachmentKeyword` and the current timestamp when attachment is added.",
    )
    input_type: (
        FormAttachmentInput
        | FormAudioInput
        | FormDocumentInput
        | FormImageInput
        | FormSignatureInput
        | FormVideoInput
        | None
    ) = Field(
        ...,
        alias="inputType",
        description="The input user interface to use for the attachment.",
    )
    label: str | None = Field(
        ..., description="A string value indicating what the element represents."
    )
    max_attachment_count: int | None = Field(
        None,
        alias="maxAttachmentCount",
        description="An integer that indicates the maximum number of attachments that can be added. If not provided, there is no maximum number required.",
    )
    min_attachment_count: int | None = Field(
        None,
        alias="minAttachmentCount",
        description="An integer that indicates the minimum number of attachments that must be added. If not provided, there is no minimum number required.",
    )
    type: Literal["attachment"] = Field(
        "attachment",
        description="String value indicating which type of element to use.",
    )
    use_original_file_name: bool | None = Field(
        True,
        alias="useOriginalFileName",
        description="Determines whether the uploaded attachment's file name is preserved. If `false`, the name is updated based on `filenameFormatExpression`. Default is `true`.",
    )
    visibility_expression: str | None = Field(
        None,
        alias="visibilityExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true`, the element is displayed. When the expression evaluates to `false` the element is not displayed. If no expression is provided, the default behavior is that the element is displayed.",
    )


class FormRelationshipElement(BaseModel):
    """
    Defines how a relationship between feature layers and tables can participate in the form. When present in the form, the user may have the option to add or edit related records.
    """

    model_config = common_config

    description: str | None = Field(
        None, description="A string that describes the element in detail."
    )
    display_count: int | None = Field(
        None,
        alias="displayCount",
        description="An integer that indicates the maximum number of records to display.",
    )
    display_type: Literal["list"] = Field(
        "list",
        alias="displayType",
        description="A string that defines how the related records should be displayed.",
    )
    editable_expression: str | None = Field(
        None,
        alias="editableExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true`, the element is editable. When the expression evaluates to `false` the element is not editable. If the referenced related table is not editable, the editable expression is ignored and the element is not editable.",
    )
    label: str = Field(
        ..., description="A string value indicating what the element represents."
    )
    order_by_fields: list[OrderByField] | None = Field(
        None,
        alias="orderByFields",
        description="Array of orderByField objects indicating the display order for the related records, and whether they should be sorted in ascending <code>'asc'</code> or descending <code>'desc'</code> order.",
    )
    relationship_id: int = Field(
        ...,
        alias="relationshipId",
        description="The id of the relationship as defined in the feature layer definition",
    )
    type: Literal["relationship"] = Field(
        "relationship",
        description="String value indicating which type of element to use.",
    )
    visibility_expression: str | None = Field(
        None,
        alias="visibilityExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true`, the element is displayed. When the expression evaluates to `false` the element is not displayed. If no expression is provided, the default behavior is that the element is displayed.",
    )


class FormTextElement(BaseModel):
    """
    Configures read-only text in form elements.
    """

    model_config = common_config
    text: str = Field(
        ...,
        description="String value indicating the text to be displayed within the formTextElement.",
    )
    text_format: TextFormat | None = Field(
        TextFormat.plain_text,
        validate_default=True,
        alias="textFormat",
        description="Defines language of `text` property. Default is `plain-text`.",
    )
    type: Literal["text"] = Field(
        "text",
        description="String value indicating which type of element to use. Valid value of this property is `text`.",
    )
    visibility_expression: str | None = Field(
        None,
        alias="visibilityExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true`, the element is displayed. When the expression evaluates to `false` the element is not displayed. If no expression is provided, the default behavior is that the element is displayed.",
    )


class FormGroupElement(BaseModel):
    """
    Defines a container that holds a set of form elements that can be expanded, collapsed, or displayed together.
    """

    model_config = common_config

    description: str | None = Field(
        None, description="A string that describes the element in detail."
    )
    form_elements: list[
        FormAttachmentElement
        | FormFieldElement
        | FormRelationshipElement
        | FormTextElement
    ] = Field(
        ...,
        alias="formElements",
        description="An array of Form Element objects that represent an ordered list of form elements. Nested group elements are not supported.",
    )
    initial_state: GroupInitialState | None = Field(
        None,
        alias="initialState",
        description="Defines if the group should be expanded or collapsed when the form is initially displayed. If not provided, the default value is `expanded`",
    )
    label: str | None = Field(
        None, description="A string value indicating what the element represents."
    )
    type: Literal["group"] = Field(
        "group", description="String value indicating which type of element to use."
    )
    visibility_expression: str | None = Field(
        None,
        alias="visibilityExpression",
        description="A reference to an Arcade expression that returns a boolean value. When this expression evaluates to `true`, the element is displayed. When the expression evaluates to `false` the element is not displayed. If no expression is provided, the default behavior is that the element is displayed.",
    )


class FormInfo(BaseModel):
    """
    Defines the form configuration when a user edits a feature.
    """

    model_config = common_config

    description: str | None = Field(
        None,
        description="A string that appears in the body of the form as a description.",
    )
    expression_infos: list[FormExpressionInfo] | None = Field(
        None,
        alias="expressionInfos",
        description="List of Arcade expressions used in the form.",
    )
    form_elements: list[
        FormAttachmentElement
        | FormFieldElement
        | FormRelationshipElement
        | FormTextElement
        | FormGroupElement
        | FormUtilityNetworkAssociationsElement
    ] = Field(
        ...,
        alias="formElements",
        description="An array of formElement objects that represent an ordered list of form elements.",
    )
    preserve_field_values_when_hidden: bool | None = Field(
        None,
        alias="preserveFieldValuesWhenHidden",
        description="Determines whether a previously visible `formFieldElement` value is retained or cleared when a `visibilityExpression` applied on the `formFieldElement` or its parent `formGroupElement` evaluates to `false`. Default is `false`.",
    )
    title: str | None = Field(
        None, description="A string that appears at the top of the form as a title."
    )
