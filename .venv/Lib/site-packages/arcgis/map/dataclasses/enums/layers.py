from enum import Enum


class BlendMode(Enum):
    """
    Blend modes are used to create various effects by blending colors of top and background layers. `normal` blend mode is the default.
    """

    average = "average"
    color = "color"
    color_burn = "color-burn"
    color_dodge = "color-dodge"
    darken = "darken"
    destination_atop = "destination-atop"
    destination_in = "destination-in"
    destination_out = "destination-out"
    destination_over = "destination-over"
    difference = "difference"
    exclusion = "exclusion"
    hard_light = "hard-light"
    hue = "hue"
    invert = "invert"
    lighten = "lighten"
    lighter = "lighter"
    luminosity = "luminosity"
    minus = "minus"
    multiply = "multiply"
    normal = "normal"
    overlay = "overlay"
    plus = "plus"
    reflect = "reflect"
    saturation = "saturation"
    screen = "screen"
    soft_light = "soft-light"
    source_atop = "source-atop"
    source_in = "source-in"
    source_out = "source-out"
    vivid_light = "vivid-light"
    xor = "xor"


class ListMode(Enum):
    """
    To show or hide the sublayer in the layer list. If the layer has sublayers, selecting `hide-children` will hide them in the layer list.
    """

    hide = "hide"
    hide_children = "hide-children"
    show = "show"


class FeatureCollectionType(Enum):
    """
    Indicates the type of features in the feature collection. If `featureCollectionType` is missing, it means the feature collection is a regular single-layer or multi-layer feature collection.
    """

    markup = "markup"
    notes = "notes"
    route = "route"


class ImageFormat(Enum):
    """
    Image format of the cached tiles.
    """

    bip = "bip"
    bmp = "bmp"
    bsq = "bsq"
    emf = "emf"
    gif = "gif"
    jpg = "jpg"
    jpgpng = "jpgpng"
    lerc = "lerc"
    mixed = "mixed"
    pdf = "pdf"
    png = "png"
    png24 = "png24"
    png32 = "png32"
    png8 = "png8"
    ps = "ps"
    svg = "svg"
    svgz = "svgz"
    tiff = "tiff"


class ColumnDelimiter(Enum):
    """
    A string defining the character used to separate columns in a CSV file.
    """

    field_ = "\t"
    field__1 = " "
    field__2 = ","
    field__3 = ";"
    field__4 = "|"


class NoDataInterpretation(Enum):
    """
    A string value of interpretation of noData setting. Default is 'esriNoDataMatchAny' when noData is a number, and 'esriNoDataMatchAll' when noData is an array.
    """

    esri_no_data_match_all = "esriNoDataMatchAll"
    esri_no_data_match_any = "esriNoDataMatchAny"


class BingLayerType(Enum):
    """
    String indicating the layer type.
    """

    bing_maps_aerial = "BingMapsAerial"
    bing_maps_hybrid = "BingMapsHybrid"
    bing_maps_road = "BingMapsRoad"


class Download(Enum):
    """
    When editing layers, the edits are always sent to the server. This string value indicates which data is retrieved. For example, `none` indicates that only the schema is written since neither the features nor attachments are retrieved. For a full sync without downloading attachments, indicate `features`. Lastly, the default behavior is to have a full sync using `featuresAndAttachments` where both features and attachments are retrieved.
    """

    features = "features"
    features_and_attachments = "featuresAndAttachments"
    none = "none"


class Sync(Enum):
    """
    This string value indicates how the data is synced.
    """

    sync_features_and_attachments = "syncFeaturesAndAttachments"
    sync_features_upload_attachments = "syncFeaturesUploadAttachments"
    upload_features_and_attachments = "uploadFeaturesAndAttachments"


class Interpolation(Enum):
    """
    The algorithm used for interpolation.
    """

    rsp_bilinear_interpolation = "RSP_BilinearInterpolation"
    rsp_cubic_convolution = "RSP_CubicConvolution"
    rsp_majority = "RSP_Majority"
    rsp_nearest_neighbor = "RSP_NearestNeighbor"


class PixelType(Enum):
    """
    Pertains to the type of values stored in the raster, such as signed integer, unsigned integer, or floating point.
    """

    c128 = "C128"
    c64 = "C64"
    f32 = "F32"
    f64 = "F64"
    s16 = "S16"
    s32 = "S32"
    s8 = "S8"
    u1 = "U1"
    u16 = "U16"
    u2 = "U2"
    u32 = "U32"
    u4 = "U4"
    u8 = "U8"
    unknown = "UNKNOWN"


class GraphType(Enum):
    """
    Indicates the type of graph object.
    """

    entity = "entity"
    relationship = "relationship"


class VisibilityMode(Enum):
    """
    Defines how visibility of sub layers is affected. If set to 'exclusive', clients should ensure only one sublayer is visible at a time. If set to 'independent', clients should allow visibility to be set independently for each sublayer. 'independent' is default.'
    """

    exclusive = "exclusive"
    independent = "independent"
    inherited = "inherited"
