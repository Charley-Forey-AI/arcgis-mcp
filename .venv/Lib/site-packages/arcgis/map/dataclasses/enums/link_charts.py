from enum import Enum


class LinkChartLayoutType(Enum):
    basic_grid = "basic-grid"
    chronological_mono_timeline = "chronological-mono-timeline"
    chronological_multi_timeline = "chronological-multi-timeline"
    geographic_organic_standard = "geographic-organic-standard"
    hierarchical_bottom_to_top = "hierarchical-bottom-to-top"
    hierarchical_top_to_bottom = "hierarchical-top-to-bottom"
    organic_community = "organic-community"
    organic_fusiform = "organic-fusiform"
    organic_leaf_circle = "organic-leaf-circle"
    organic_standard = "organic-standard"
    radial_node_centric = "radial-node-centric"
    radial_root_centric = "radial-root-centric"
    tree_bottom_to_top = "tree-bottom-to-top"
    tree_top_to_bottom = "tree-top-to-bottom"
    tree_left_to_right = "tree-left-to-right"
    tree_right_to_left = "tree-right-to-left"


class IdealEdgeLengthType(Enum):
    absolute_value = "absoluteValue"
    multiplier = "multiplier"


class EventsTicksVisualizationType(Enum):
    none = "none"
    start_and_end = "startAndEnd"
    start_only = "startOnly"


class TimeDirectionType(Enum):
    bottom = "bottom"
    left = "left"
    right = "right"
    top = "top"
