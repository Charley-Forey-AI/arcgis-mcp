## The goal of this script is to create methods for smart mapping and the Map widget
### The SmartMappingManager class will be used to access smart mapping methods. These methods will talk to the JS API and return the results to the Python API.
from __future__ import annotations


class SmartMappingManager(object):
    """
    A class to manage smart mapping methods. Smart Mapping **ONLY** works with a rendered map.

        .. warning::
            There are a few points to note with these methods:
            - You need to make sure that the layer has been loaded before calling a method on it.
            - The map can take a few seconds to reflect the changes from the renderer creation.
            - Calling any other methods or properties need to be done in a separate cell from this method call due to asynchronous conflicts.
            - If you do not see the layer update, check the console log for any JavaScript errors.
    """

    def __init__(self, source, layer) -> None:
        # Must have a view to do smart mapping
        from arcgis.map import Map, Scene

        if isinstance(source, Scene):
            raise Exception("Smart Mapping is not supported for Scene.")

        # Set the view and gis
        self._source: Map = source
        self._layer_id = layer.id
        self._layer = layer

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"SmartMappingManager(layer={self._layer.title})"

    def relationship_renderer(
        self,
        field1: dict,
        field2: dict,
        classification_method: str | None = None,
        focus: str | None = None,
        num_classes: int | None = None,
        outline_optimization_enabled: bool = False,
        size_optimization_enabled: bool = False,
        legend_options: dict | None = None,
        relationship_scheme: dict | None = None,
        default_symbol_enabled: bool = True,
        for_binning: bool | None = None,
    ) -> None:
        """
        This method utilizes the JS API create_renderer method to create a relationship renderer. This can only be accomplished with a rendered map.
        A relationship renderer helps explore the relationship between two numeric fields. The relationship is visualized using a bivariate choropleth visualization.
        This renderer classifies each variable in either 2, 3, or 4 classes along separate color ramps.

        Executing this method will automatically update your layer renderer on the map.

        .. warning::
            There are a few points to note with this method:
            - You need to make sure that the layer has been loaded before calling this method.
            - The map can take a few seconds to reflect the changes from the renderer.
            - Calling the `save` or `update` method must be done in a separate cell from this method call due to asynchronous conflicts.
            - If you do not see the layer update, check the console log for any JavaScript errors.

        ==============================      ====================================================================
        **Argument**                        **Description**
        ------------------------------      --------------------------------------------------------------------
        field1                              Required dictionary. A numeric field that will be used to explore its
                                            relationship with field2. In the default visualization, the values of
                                            this field are rendered along the vertical axis of the Legend.

                                            .. code-block:: python

                                                # Example dictionary
                                                field1 = {
                                                    "field": "POP2010", #required
                                                    "normalizationField": "SQMI",
                                                    "maxValue": 1000000,
                                                    "minValue": 1000,
                                                    "label": "Population 2010",
                                                }
        ------------------------------      --------------------------------------------------------------------
        field2                              Required dictionary. A numeric field that will be used to explore its
                                            relationship with field1. In the default visualization, the values of
                                            this field are rendered along the horizontal axis of the Legend.
        ------------------------------      --------------------------------------------------------------------
        classification_method               Optional string. The method for classifying each field's data values.
                                            Values: "quantile", "equal-interval", "natural-breaks"
        ------------------------------      --------------------------------------------------------------------
        focus                               Optional string. Determines the orientation of the Legend. This value
                                            does not change the renderer or symbology of any features in the layer.
                                            This affects the legend only. See the table at this
                                            :ref:`link <https://developers.arcgis.com/javascript/latest/api-reference/esri-smartMapping-renderers-relationship.html#createRenderer:~:text=%22natural%2Dbreaks%22-,focus,-String>`
                                            for more information.

                                            Values: None, "HH", "HL", "LH", "LL"
                                            If None, the legend renders as a square.
        ------------------------------      --------------------------------------------------------------------
        num_classes                         Optional integer. Indicates the number of classes by which to break
                                            up the values of each field. More classes give you more detail,
                                            but more colors, making the visualization more difficult to understand.
                                            There are only three possible values: 2, 3, or 4.
        ------------------------------      --------------------------------------------------------------------
        outline_optimization_enabled        Optional boolean. For polygon layers only. Indicates whether the polygon
                                            outline width should vary based on view scale.
        ------------------------------      --------------------------------------------------------------------
        size_optimization_enabled           Optional boolean. For point and polyline layers only. Indicates whether
                                            symbol sizes should vary based on view scale.
        ------------------------------      --------------------------------------------------------------------
        legend_options                      Optional dictionary. Provides options for modifying Legend properties
                                            describing the visualization.

                                            .. code-block:: python

                                                # Example dictionary
                                                legend_options = {
                                                    "title": "Population 2010",
                                                    "showLegend": True,
                                                }
        ------------------------------      --------------------------------------------------------------------
        relationship_scheme                 Optional dictionary. In authoring apps, the user may select a pre-defined
                                            relationship scheme. Pass the scheme object to this property to avoid
                                            getting one based on the background of the Map.
        ------------------------------      --------------------------------------------------------------------
        default_symbol_enabled              Optional boolean. Enables the defaultSymbol on the renderer and assigns
                                            it to features with no value or that fall outside of the prescribed class breaks.
        ------------------------------      --------------------------------------------------------------------
        for_binning                         Optional boolean. Indicates whether the generated renderer is for a
                                            binning visualization. If true, then the input field(s) in this method
                                            should refer to aggregate fields defined in the featureReduction property of the layer.
        ==============================      ====================================================================

        """
        if "field" not in field1.keys() and "field" not in field2.keys():
            raise ValueError("field1 and field2 must have a 'field' key.")

        params = {
            "type": "relationship",
            "field1": field1,
            "field2": field2,
            "classificationMethod": (
                classification_method if classification_method else "quantile"
            ),
            "numClasses": num_classes if num_classes else 3,
            "outlineOptimizationEnabled": outline_optimization_enabled,
            "sizeOptimizationEnabled": size_optimization_enabled,
            "symbolType": "2d",  # 3d not yet supported
            "defaultSymbolEnabled": default_symbol_enabled,
            "layerId": self._layer_id,
        }
        if focus:
            params["focus"] = focus
        if legend_options:
            params["legendOptions"] = legend_options
        if relationship_scheme:
            params["relationshipScheme"] = relationship_scheme
        if for_binning in [True, False]:
            params["forBinning"] = for_binning

        self._create_renderer(params)

    def univariate_color_size_renderer(
        self,
        field: str | None = None,
        normalization_field: str | None = None,
        value_expression: str | None = None,
        value_expression_title: str | None = None,
        theme: str | None = None,
        sql_expression: str | None = None,
        sql_where: str | None = None,
        statistics: dict | None = None,
        min_value: int | None = None,
        max_value: int | None = None,
        default_symbol_enabled: bool = None,
        color_options: dict | None = None,
        size_options: dict | None = None,
        symbol_options: dict | None = None,
        legend_options: dict | None = None,
        for_binning: bool | None = None,
    ) -> None:
        """
        This method utilizes the JS API create_renderer method to create a univariate color and size renderer. This can only be accomplished with a rendered map.
        A univariate color and size renderer visualizes quantitative data by adjusting the color and size of each feature proportionally to a data value.

        Executing this method will automatically update your layer renderer on the map.

        .. warning::
            There are a few points to note with this method:
            - You need to make sure that the layer has been loaded before calling this method.
            - The map can take a few seconds to reflect the changes from the renderer.
            - Calling the `save` or `update` method must be done in a separate cell from this method call due to asynchronous conflicts.
            - If you do not see the layer update, check the console log for any JavaScript errors.

        ==============================      ====================================================================
        **Argument**                        **Description**
        ------------------------------      --------------------------------------------------------------------
        field                               Optional string. The name of the field whose data will be queried for
                                            statistics and used for the basis of the data-driven visualization.
                                            If a value_expression is set, this field is ignored.
        ------------------------------      --------------------------------------------------------------------
        normalization_field                 Optional string. The name of the field to normalize the values of
                                            the given field. Providing a normalization field helps minimize some
                                            visualization errors and standardizes the data so all features are
                                            visualized with minimal bias due to area differences or count variation.
                                            This option is commonly used when visualizing densities.
        ------------------------------      --------------------------------------------------------------------
        value_expression                    Optional string. An Arcade expression following the specification
                                            defined by the Arcade Visualization Profile. Expressions may reference
                                            field values using the $feature profile variable and must return a number.
                                            This property overrides the field property and therefore is used instead
                                            of an input field value.
        ------------------------------      --------------------------------------------------------------------
        value_expression_title              Optional string. The title used to describe the value_expression in the Legend.
        ------------------------------      --------------------------------------------------------------------
        theme                               Optional string. Sets the size stops and colors based on meaningful data values.
                                            For more information on each value, see the table at this :ref:`link <https://developers.arcgis.com/javascript/latest/api-reference/esri-smartMapping-renderers-univariateColorSize.html#createContinuousRenderer:~:text=Since%20version%204.18.-,Value,-Description>`

                                            Values: "high-to-low", "above", "below", "above-and-below"
        ------------------------------      --------------------------------------------------------------------
        sql_expression                      Optional string. A SQL expression evaluating to a number.
        ------------------------------      --------------------------------------------------------------------
        sql_where                           Optional string. A where clause for the query. Used to filter features for
                                            the statistics query.
        ------------------------------      --------------------------------------------------------------------
        statistics                          Optional dictionary.  If statistics for the field have already been generated,
                                            then pass the dictionary here to avoid making a second statistics query to the server.

                                            .. code-block:: python

                                                # Example dictionary
                                                statistics = {
                                                    "average": <int>,
                                                    "count": <int>,
                                                    "max": <int>,
                                                    "min": <int>,
                                                    "median": <int>,
                                                    "stddev": <int>,
                                                    "sum": <int>,
                                                    "variance": <int>,
                                                    "nullCount": <int>, #optional
                                                }
        ------------------------------      --------------------------------------------------------------------
        min_value                           Optional integer. A custom minimum value set by the user. Use this
                                            in conjunction with max_value to generate statistics between lower and
                                            upper bounds. This will be the lowest stop in the returned visual variables.
        ------------------------------      --------------------------------------------------------------------
        max_value                           Optional integer. A custom maximum value set by the user. Use this
                                            in conjunction with min_value to generate statistics between lower and
                                            upper bounds. This will be the highest stop in the returned visual variables.
        ------------------------------      --------------------------------------------------------------------
        default_symbol_enabled              Optional boolean. Enables the defaultSymbol on the renderer and assigns
                                            it to features with no value or that fall outside of the prescribed class breaks.
        ------------------------------      --------------------------------------------------------------------
        color_options                       Optional dictionary. Options for configuring the color portion of the visualization.

                                            .. code-block:: python

                                                # Example dictionary
                                                color_options = {
                                                    "colorScheme": {}, #depends on layer type
                                                    "isContinuous": <boolean>,
                                                }
        ------------------------------      --------------------------------------------------------------------
        size_options                        Optional dictionary. Options for configuring the size portion of the visualization.

                                            .. code-block:: python

                                                # Example dictionary
                                                size_options = {
                                                    "sizeScheme": {}, #depends on geometry type of layer
                                                    "sizeOptimizationEnabled": <boolean>,
                                                }
        ------------------------------      --------------------------------------------------------------------
        symbol_options                      Optional dictionary. Options for configuring the symbol with "above-and-below" theme.

                                            * 'symbolStyle' Values: "caret", "circle-caret", "arrow", "circle-arrow", "plus-minus", "circle-plus-minus", "square", "circle", "triangle", "happy-sad", "thumb"

                                            .. code-block:: python

                                                # Example dictionary
                                                symbol_options = {
                                                    "symbolStyle": <string>, #see values above
                                                    "symbols": {
                                                        "above": <dict>, #symbol dictionary
                                                        "below": <dict>, #symbol dictionary
                                                    }
                                                }
        ------------------------------      --------------------------------------------------------------------
        legend_options                      Optional dictionary. Options for configuring the legend.

                                            .. code-block:: python

                                                # Example dictionary
                                                legend_options = {
                                                    "title": <string>,
                                                    "showLegend": <boolean>,
                                                }
        ------------------------------      --------------------------------------------------------------------
        for_binning                         Optional boolean. Indicates whether the generated renderer is for a
                                            binning visualization. If true, then the input field(s) in this method
                                            should refer to aggregate fields defined in the featureReduction property
                                            of the layer.
        ==============================      ====================================================================

        """
        params = {
            "type": "univariateColorSize",
            "field": field,
            "normalizationField": normalization_field,
            "valueExpression": value_expression,
            "valueExpressionTitle": value_expression_title,
            "theme": theme if theme else "high-to-low",
            "sqlExpression": sql_expression,
            "sqlWhere": sql_where,
            "statistics": statistics,
            "minValue": min_value,
            "maxValue": max_value,
            "symbolType": "2d",
            "layerId": self._layer_id,
        }
        if default_symbol_enabled:
            params["defaultSymbolEnabled"] = default_symbol_enabled
        if color_options:
            params["colorOptions"] = color_options
        if size_options:
            params["sizeOptions"] = size_options
        if symbol_options:
            params["symbolOptions"] = symbol_options
        if legend_options:
            params["legendOptions"] = legend_options
        if for_binning:
            params["forBinning"] = for_binning

        self._create_renderer(params)

    def heatmap_renderer(
        self,
        field: str | None = None,
        scheme: dict | None = None,
        statistics: dict | None = None,
        fade_ratio: float | None = None,
        fade_to_transparent: bool = True,
        radius: int | None = None,
        min_ratio: float | None = None,
        max_ratio: float | None = None,
    ) -> None:
        """
        The HeatmapRenderer uses kernel density to render point features in FeatureLayers, CSVLayers,
        GeoJSONLayers and OGCFeatureLayers as a raster surface.

        To create this visual, the HeatmapRenderer fits a smoothly curved surface over each point.
        The surface value is highest at the location of the point and decreases proportionally to the
        distance from the point, reaching zero at the distance from the point specified in radius.
        The value of the surface equals the field value for the point, or 1 if no field is provided.
        The density at each pixel is calculated by adding the values of all the kernel surfaces where they overlay.

        Executing this method will automatically update your layer renderer on the map.

        .. warning::
            There are a few points to note with this method:
            - You need to make sure that the layer has been loaded before calling this method.
            - The map can take a few seconds to reflect the changes from the renderer.
            - Calling the `save` or `update` method must be done in a separate cell from this method call due to asynchronous conflicts.
            - If you do not see the layer update, check the console log for any JavaScript errors.

        ==============================      ====================================================================
        **Argument**                        **Description**
        ------------------------------      --------------------------------------------------------------------
        field                               Optional string. The name of the field whose data will be queried for
                                            statistics and used for the basis of the data-driven visualization.
                                            The value of the field is used as a multiplier in the heatmap,
                                            making areas with high field values hotter than areas where the features
                                            have low field values.
        ------------------------------      --------------------------------------------------------------------
        scheme                              Optional dictionary. A pre-defined heatmap scheme. Use this parameter to
                                            avoid generating a new scheme based on the background of the Map.

                                            .. code-block:: python

                                                # Example dictionary
                                                scheme = {
                                                    "name": "Heatmap Blue 2",
                                                    "tags": ["Heatmap", "Blue"],
                                                    "id": <themeName>/<basemapName>/<schemeName>,
                                                    "colors": [
                                                        [0, 0, 255, 0.2],
                                                        [0, 0, 255, 0.4],
                                                        [0, 0, 255, 0.6],
                                                        [0, 0, 255, 0.8],
                                                        [0, 0, 255, 1]
                                                    ],
                                                    opacity: 0.75
                                                }
        ------------------------------      --------------------------------------------------------------------
        statistics                          Optional dictionary. If statistics for the field have already been generated,
                                            then pass the object here to avoid making a second statistics query to the server.
        ------------------------------      --------------------------------------------------------------------
        fade_ratio                          Optional float. Indicates how much to fade the lower color stops with
                                            transparency to create a fuzzy boundary on the edge of the heatmap.
                                            A value of 0 makes a discrete boundary on the lower color stop.
        ------------------------------      --------------------------------------------------------------------
        fade_to_transparent                 Optional boolean. Indicates whether to fade the lower color stops with
                                            transparency to create a fuzzy boundary on the edge of the heatmap.
                                            If False, the fade_ratio is ignored.
        ------------------------------      --------------------------------------------------------------------
        radius                              Optional integer. The radius in points that determines the area of
                                            influence of each point. A higher radius indicates points have more
                                            influence on surrounding points.
        ------------------------------      --------------------------------------------------------------------
        min_ratio                           Optional float. The minimum ratio used to normalize the heatmap intensity values.
        ------------------------------      --------------------------------------------------------------------
        max_ratio                           Optional float. The maximum ratio used to normalize the heatmap intensity values.
        ==============================      ====================================================================

        """
        params = {
            "type": "heatmap",
            "field": field,
            "statistics": statistics,
            "fadeRatio": fade_ratio,
            "fadeToTransparent": fade_to_transparent,
            "radius": radius,
            "minRatio": min_ratio,
            "maxRatio": max_ratio,
            "layerId": self._layer_id,
        }
        if scheme:
            params["heatmapScheme"] = scheme

        self._create_renderer(params)

    def dot_density_renderer(
        self,
        attributes: dict,
        dot_value_optimization_enabled: bool = True,
        dot_blending_enabled: bool = True,
        outline_optimization_enabled: bool = False,
        scheme: dict | None = None,
        for_binning: bool | None = None,
    ) -> None:
        """
        DotDensityRenderer allows you to create dot density visualizations for polygon layers.
        Dot density visualizations randomly draw dots within each polygon to visualize the
        density of a population or some other variable. Each dot represents a fixed numeric
        value of an attribute or a subset of attributes. Unlike choropleth maps, field values
        used in dot density visualizations don't need to be normalized because the size of
        the polygon, together with the number of dots rendered within its boundaries,
        indicate the spatial density of that value.

        Executing this method will automatically update your layer renderer on the map.

        .. warning::
            There are a few points to note with this method:
            - You need to make sure that the layer has been loaded before calling this method.
            - The map can take a few seconds to reflect the changes from the renderer.
            - Calling the `save` or `update` method must be done in a separate cell from this method call due to asynchronous conflicts.
            - If you do not see the layer update, check the console log for any JavaScript errors.

        ==============================      ====================================================================
        **Argument**                        **Description**
        ------------------------------      --------------------------------------------------------------------
        attributes                          Required list of dictionaries. A set of complementary numeric fields/expressions
                                            used as the basis of the dot density visualization. For example,
                                            if creating an election map, you would indicate the names of each field
                                            representing the candidate or political party where total votes are stored.

                                            Keys:
                                            - "field": The name of a numeric field.
                                            - "label" : The label used to describe the field in the Legend.
                                            - "valueExpression": An Arcade expression following the specification
                                                defined by the Arcade Visualization Profile. Expressions may reference
                                                field values using the $feature profile variable and must return a number.
                                                This property overrides the field property and therefore is used instead
                                                of an input field value.
                                            - "valueExpressionTitle": Text describing the value returned from the 'valueExpression'.
        ------------------------------      --------------------------------------------------------------------
        dot_value_optimization_enabled      Optional boolean. Indicates whether to enable dot value optimization.
                                            When enabled, the renderer attempts to find the best dot value for each
                                            polygon based on the polygon's size and the number of dots rendered within it.
        ------------------------------      --------------------------------------------------------------------
        dot_blending_enabled                Optional boolean. Indicates whether to enable dot blending.
                                            When enabled, the renderer blends dots together where they overlap.
        ------------------------------      --------------------------------------------------------------------
        outline_optimization_enabled        Optional boolean. Indicates whether to enable outline optimization.
                                            When enabled, the renderer attempts to find the best outline color for
                                            each polygon based on the polygon's size and the number of dots rendered within it.
        ------------------------------      --------------------------------------------------------------------
        scheme                              Optional dictionary. A pre-defined dot density scheme. Use this parameter to
                                            avoid generating a new scheme based on the background of the Map.

                                            .. code-block:: python

                                                # Example dictionary
                                                scheme = {
                                                    "name": "Reds 5",
                                                    "tags": ["Single Color", "Red"],
                                                    "id": <themeName>/<basemapName>/<schemeName>,
                                                    "colors": [
                                                        [255, 245, 240],
                                                        [254, 224, 210],
                                                        [252, 187, 161],
                                                        [252, 146, 114],
                                                        [222, 45, 38]
                                                    ],
                                                    "outline": {
                                                        "color": [0, 0, 0, 0.5],
                                                        "width": 0.75
                                                    },
                                                    "opacity": 0.75
                                                }
        ------------------------------      --------------------------------------------------------------------
        for_binning                         Optional boolean. Indicates whether the generated renderer is for a
                                            binning visualization. If true, then the input field(s) in this method
                                            should refer to aggregate fields defined in the featureReduction property
                                            of the layer.
        ==============================      ====================================================================


        """
        if not isinstance(attributes, list):
            if isinstance(attributes, dict):
                attributes = [attributes]
            else:
                raise ValueError(
                    f"attributes must be a list of dictionaries. {type(attributes)} was passed."
                )

        params = {
            "type": "dotDensity",
            "attributes": attributes,
            "dotValueOptimizationEnabled": dot_value_optimization_enabled,
            "dotBlendingEnabled": dot_blending_enabled,
            "outlineOptimizationEnabled": outline_optimization_enabled,
            "layerId": self._layer_id,
        }
        if scheme:
            params["dotDensityScheme"] = scheme
        if for_binning:
            params["forBinning"] = for_binning

        self._create_renderer(params)

    def pie_chart_renderer(
        self,
        attributes: dict,
        shape: str | None = None,
        include_size_variable: bool | None = None,
        outline_optimization_enabled: bool = False,
        size_optimization_enabled: bool = False,
        scheme: dict | None = None,
        for_binning: bool | None = None,
    ) -> None:
        """
        PieChartRenderer allows you to create a pie chart for each feature in the layer.
        The value and color of each pie slice is specified in the attributes property.
        You can vary the size of each pie based on data with any other field value or
        Arcade expression using visualVariables.

        Executing this method will automatically update your layer renderer on the map.

        .. warning::
            There are a few points to note with this method:
            - You need to make sure that the layer has been loaded before calling this method.
            - The map can take a few seconds to reflect the changes from the renderer.
            - Calling the `save` or `update` method must be done in a separate cell from this method call due to asynchronous conflicts.
            - If you do not see the layer update, check the console log for any JavaScript errors.

        ==============================      ====================================================================
        **Argument**                        **Description**
        ------------------------------      --------------------------------------------------------------------
        attributes                          Required list of dictionaries. A set of complementary numeric fields/expressions
                                            used to create the charts. For example, if creating an election map,
                                            you would indicate the name of each field representing the candidate
                                            or political party where their total counts are stored.

                                            Keys:
                                            - "field": The name of a numeric field.
                                            - "label" : The label used to describe the field in the Legend.
                                            - "valueExpression": An Arcade expression following the specification
                                                defined by the Arcade Visualization Profile. Expressions may reference
                                                field values using the $feature profile variable and must return a number.
                                                This property overrides the field property and therefore is used instead
                                                of an input field value.
                                            - "valueExpressionTitle": Text describing the value returned from the 'valueExpression'.
        ------------------------------      --------------------------------------------------------------------
        shape                               Optional string. The shape used for the pie chart.

                                            Values: "pie" | "donut"
        ------------------------------      --------------------------------------------------------------------
        include_size_variable               Optional boolean. Indicates whether to include data-driven size in
                                            the final renderer. If true, features will be assigned a sized based
                                            on the sum of all values in the attributes param. Features with
                                            small total counts will be sized with small charts and features with
                                            large total counts will be sized with large charts. Enabling this
                                            option is good for visualizing how influential a particular feature
                                            is compared to the dataset as a whole. It removes bias introduced
                                            by features with large geographic areas, but relatively small data values.
        ------------------------------      --------------------------------------------------------------------
        outline_optimization_enabled        Optional boolean. Only for polygon layers. Indicates whether the
                                            polygon's background fill symbol outline width should vary based on
                                            view scale.
        ------------------------------      --------------------------------------------------------------------
        size_optimization_enabled           Optional boolean. Indicates whether symbol sizes should vary based on view scale.
        ------------------------------      --------------------------------------------------------------------
        scheme                              Optional dictionary. A pre-defined pie chart scheme. Use this parameter to
                                            avoid generating a new scheme based on the background of the Map.

                                            .. code-block:: python

                                                # Example dictionary
                                                scheme = {
                                                    "name": "Reds 5",
                                                    "tags": ["Single Color", "Red"],
                                                    "colors": [
                                                        [255, 245, 240],
                                                        [254, 224, 210],
                                                        [252, 187, 161],
                                                        [252, 146, 114],
                                                        [222, 45, 38]
                                                    ],
                                                    "colorForOthersCategory": [200, 200, 200, 255],
                                                    "outline": {
                                                        "color": [0, 0, 0, 0.5],
                                                        "width": 0.75
                                                    },
                                                    "size": 75,
                                                }
        ------------------------------      --------------------------------------------------------------------
        for_binning                         Optional boolean. Indicates whether the generated renderer is for a
                                            binning visualization. If true, then the input field(s) in this method
                                            should refer to aggregate fields defined in the featureReduction property
                                            of the layer.
        ==============================      ====================================================================

        """
        if not isinstance(attributes, list):
            if isinstance(attributes, dict):
                attributes = [attributes]
            else:
                raise ValueError(
                    f"attributes must be a list of dictionaries. {type(attributes)} was passed."
                )

        params = {
            "type": "pieChart",
            "attributes": attributes,
            "shape": shape if shape else "pie",
            "outlineOptimizationEnabled": outline_optimization_enabled,
            "sizeOptimizationEnabled": size_optimization_enabled,
            "layerId": self._layer_id,
        }
        if include_size_variable:
            params["includeSizeVariable"] = include_size_variable
        if scheme:
            params["pieChartScheme"] = scheme
        if for_binning:
            params["forBinning"] = for_binning

        self._create_renderer(params)

    def predominance_renderer(
        self,
        fields: list,
        include_opacity_variable: bool | None = None,
        include_size_variable: bool | None = None,
        outline_optimization_enabled: bool = False,
        size_optimization_enabled: bool = False,
        statistics: dict | None = None,
        sort_by: str | None = None,
        scheme: dict | None = None,
        default_symbol_enabled: bool | None = True,
        for_binning: bool | None = None,
    ) -> None:
        """
        This object contains a helper method for generating a predominance visualization.
        Visualizing predominance involves coloring a layer's features based on which attribute
        among a set of competing numeric attributes wins or beats the others in total count.
        Common applications of this include visualizing election results, survey results,
        and demographic majorities.

        Executing this method will automatically update your layer renderer on the map.

        .. warning::
            There are a few points to note with this method:
            - You need to make sure that the layer has been loaded before calling this method.
            - The map can take a few seconds to reflect the changes from the renderer.
            - Calling the `save` or `update` method must be done in a separate cell from this method call due to asynchronous conflicts.
            - If you do not see the layer update, check the console log for any JavaScript errors.

        ==============================      ====================================================================
        **Argument**                        **Description**
        ------------------------------      --------------------------------------------------------------------
        fields                              Required list of dictionaries. A set of competing numeric fields used as the basis
                                            of the predominance visualization. A minimum of 2 fields are required.

                                            .. code-block:: python

                                                # Example list
                                                fields = [
                                                    {
                                                        "name": "POP2010",
                                                        "label": "Population 2010", (optional)
                                                    },
                                                    {
                                                        "name": "POP2011",
                                                        "label": "Population 2011", (optional)
                                                    },
                                                ]
        ------------------------------      --------------------------------------------------------------------
        include_opacity_variable            Optional boolean. Indicates whether to include data-driven opacity
                                            in the final renderer. If true, features where the predominant value
                                            beats all others by a large margin are given a high opacity.
                                            Features where the predominant value beats others by a small margin
                                            will be assigned a low opacity, indicating that while the feature has
                                            a winning value, it doesn't win by much.
        ------------------------------      --------------------------------------------------------------------
        include_size_variable               Optional boolean. Indicates whether to include data-driven size in
                                            the final renderer. If true, features will be assigned a sized based
                                            on the sum of all competing values in the fields param. Features with
                                            small total counts will be sized with small icons or lines depending
                                            on the geometry type of the layer, and features with large total counts
                                            will be sized with large icons or lines. Enabling this option is good
                                            for visualizing how influential a particular feature is compared to the
                                            dataset as a whole. It removes bias introduced by features with large
                                            geographic areas, but relatively small data values.
        ------------------------------      --------------------------------------------------------------------
        outline_optimization_enabled        Optional boolean. Indicates whether the polygon's background fill symbol
                                            outline width should vary based on view scale. Only for polygon layers.
        ------------------------------      --------------------------------------------------------------------
        size_optimization_enabled           Optional boolean. Indicates whether symbol sizes should vary based on view scale.
        ------------------------------      --------------------------------------------------------------------
        statistics                          Optional dictionary. If statistics for the field have already been generated,
                                            then pass the object here to avoid making a second statistics query to the server.
        ------------------------------      --------------------------------------------------------------------
        sort_by                             Optional string. Indicates how to sort the fields in the legend.
                                            If count, unique values/types will be sorted from highest to lowest
                                            based on the count of features that fall in each category. If value,
                                            unique values/types will be sorted in the order they were specified in
                                            the fields parameter.

                                            Values: "count" | "value"
        ------------------------------      --------------------------------------------------------------------
        scheme                              Optional dictionary. A pre-defined predominance scheme. Use this parameter to
                                            avoid generating a new scheme based on the background of the Map.
                                            For more information on what to include in the scheme see this url:
                                            https://developers.arcgis.com/javascript/latest/api-reference/esri-smartMapping-symbology-predominance.html#PredominanceScheme
        ------------------------------      --------------------------------------------------------------------
        default_symbol_enabled              Optional boolean. Enables the defaultSymbol on the renderer and assigns
                                            it to features with no value.
        ------------------------------      --------------------------------------------------------------------
        for_binning                         Optional boolean. Indicates whether the generated renderer is for a
                                            binning visualization. If true, then the input field(s) in this method
                                            should refer to aggregate fields defined in the featureReduction property
                                            of the layer.
        ==============================      ====================================================================


        """
        params = {
            "type": "predominance",
            "fields": fields,
            "outlineOptimizationEnabled": outline_optimization_enabled,
            "sizeOptimizationEnabled": size_optimization_enabled,
            "defaultSymbolEnabled": default_symbol_enabled,
            "symbolType": "2d",
            "layerId": self._layer_id,
        }
        if include_opacity_variable:
            params["includeOpacityVariable"] = include_opacity_variable
        if include_size_variable:
            params["includeSizeVariable"] = include_size_variable
        if statistics:
            params["statistics"] = statistics
        if sort_by:
            params["sortBy"] = sort_by
        if scheme:
            params["predominanceScheme"] = scheme
        if for_binning:
            params["forBinning"] = for_binning

        self._create_renderer(params)

    def class_breaks_renderer(
        self,
        break_type: str,
        field: str | None = None,
        normalization_field: str | None = None,
        normalization_type: str | None = None,
        normalization_total: int | None = None,
        classification_method: str | None = None,
        standard_deviation_interval: float | None = None,
        num_classes: int | None = None,
        value_expression: str | None = None,
        value_expression_title: str | None = None,
        sql_expression: str | None = None,
        sql_where: str | None = None,
        outline_optimization_enabled: bool = False,
        min_value: int | None = None,
        max_value: int | None = None,
        default_symbol_enabled: bool = True,
        for_binning: bool | None = None,
    ) -> None:
        """
        ClassBreaksRenderer defines the symbol of each feature in a Layer based on the value of a numeric attribute.
        Symbols are assigned based on classes or ranges of data. Each feature is assigned a symbol based on the
        class break in which the value of the attribute falls.

        The resulting renderer defines the symbol size of each feature based on the value of the given field value.
        A default size scheme is determined based on the background of the view. Depending on the classificationMethod,
        class breaks (or data ranges) are generated based on the statistics of the data. Each feature is assigned a size
        based on the class break in which the value of the field falls.

        Executing this method will automatically update your layer renderer on the map.

        .. warning::
            There are a few points to note with this method:
            - You need to make sure that the layer has been loaded before calling this method.
            - The map can take a few seconds to reflect the changes from the renderer.
            - Calling the `save` or `update` method must be done in a separate cell from this method call due to asynchronous conflicts.
            - If you do not see the layer update, check the console log for any JavaScript errors.

        ==============================      ====================================================================
        **Argument**                        **Description**
        ------------------------------      --------------------------------------------------------------------
        break_type                          Required string. The type of class breaks to generate. This value
                                            determines how class breaks are generated. Values are: "size" or "color".
        ------------------------------      --------------------------------------------------------------------
        field                               Optional string. The name of the field whose data will be queried for
                                            statistics and used for the basis of the data-driven visualization.
                                            If a value_expression is set, this field is ignored.
                                            The field should represent numeric types.
        ------------------------------      --------------------------------------------------------------------
        normalization_field                 Optional string. The name of the field to normalize the values of
                                            the given field. Providing a normalization field helps minimize some
                                            visualization errors and standardizes the data so all features are
                                            visualized with minimal bias due to area differences or count variation.
                                            This option is commonly used when visualizing densities.
        ------------------------------      --------------------------------------------------------------------
        normalization_type                  Optional string. Indicates how the data is normalized. The data value
                                            obtained from the field is normalized in one of the following ways
                                            before it is compared with the class breaks.

                                            Values:
                                            - "log" : Computes the base 10 logarithm of each data value. This can
                                                be useful because it reduces the influence of very large data values.
                                            - "percent-of-total" : Divides each data value by the sum of all data
                                                values then multiplies by 100.
                                            - "field" : Divides each data value by the value of the normalization_field.
        ------------------------------      --------------------------------------------------------------------
        normalization_total                 Optional integer. The total of all data values. This is used when
                                            normalization_type is "percent-of-total".
        ------------------------------      --------------------------------------------------------------------
        classification_method               Optional string. The method for classifying the data. This value
                                            determines how class breaks are generated. When the value is "equal-interval",
                                            class breaks are generated such that the difference between any two
                                            breaks is the same. When the value is "natural-breaks", class breaks
                                            are generated based on natural groupings of the data. When the value
                                            is "quantile", class breaks are generated such that the total number
                                            of data values in each class is the same. When the value is "standard-deviation",
                                            class breaks are generated based on the standard deviation of the data.
        ------------------------------      --------------------------------------------------------------------
        standard_deviation_interval         Optional float. The standard deviation interval. This value is used
                                            when classification_method is "standard-deviation".
        ------------------------------      --------------------------------------------------------------------
        num_classes                         Optional integer. The number of classes. This is ignored when
                                            when "standard-deviation" is specified.
        ------------------------------      --------------------------------------------------------------------
        value_expression                    Optional string. An Arcade expression following the specification
                                            defined by the Arcade Visualization Profile. Expressions may reference
                                            field values using the $feature profile variable and must return a number.
                                            This property overrides the field property and therefore is used instead
                                            of an input field value.
        ------------------------------      --------------------------------------------------------------------
        value_expression_title              Optional string. The title used to describe the value_expression in the Legend.
        ------------------------------      --------------------------------------------------------------------
        sql_expression                      Optional string. A SQL expression evaluating to a number.
        ------------------------------      --------------------------------------------------------------------
        sql_where                           Optional string. A where clause for the query. Used to filter features for
                                            the statistics query.
        ------------------------------      --------------------------------------------------------------------
        outline_optimization_enabled        Optional boolean. Indicates whether the polygon's background fill symbol
                                            outline width should vary based on view scale. Only for polygon layers.
        ------------------------------      --------------------------------------------------------------------
        min_value                           Optional integer. A custom minimum value set by the user. Use this
                                            in conjunction with max_value to generate statistics between lower and
                                            upper bounds. This will be the lowest stop in the returned visual variables.
        ------------------------------      --------------------------------------------------------------------
        max_value                           Optional integer. A custom maximum value set by the user. Use this
                                            in conjunction with min_value to generate statistics between lower and
                                            upper bounds. This will be the highest stop in the returned visual variables.
        ------------------------------      --------------------------------------------------------------------
        default_symbol_enabled              Optional boolean. Enables the defaultSymbol on the renderer and assigns
                                            it to features with no value or that fall outside of the prescribed class breaks.
        ------------------------------      --------------------------------------------------------------------
        for_binning                         Optional boolean. Indicates whether the generated renderer is for a
                                            binning visualization. If true, then the input field(s) in this method
                                            should refer to aggregate fields defined in the featureReduction property
                                            of the layer.
        ==============================      ====================================================================


        """
        params = {
            "field": field,
            "normalizationField": normalization_field,
            "normalizationType": normalization_type,
            "normalizationTotal": normalization_total,
            "classificationMethod": classification_method,
            "numClasses": num_classes,
            "valueExpression": value_expression,
            "valueExpressionTitle": value_expression_title,
            "sqlExpression": sql_expression,
            "sqlWhere": sql_where,
            "outlineOptimizationEnabled": outline_optimization_enabled,
            "minValue": min_value,
            "maxValue": max_value,
            "defaultSymbolEnabled": default_symbol_enabled,
            "symbolType": "2d",
            "layerId": self._layer_id,
        }

        if classification_method == "standard-deviation":
            params["standardDeviationInterval"] = (
                standard_deviation_interval
                if standard_deviation_interval
                and standard_deviation_interval in [1, 0.5, 0.33, 0.25]
                else 1
            )
        if for_binning:
            params["forBinning"] = for_binning

        if break_type == "color":
            params["type"] = "classBreaksColor"
        elif break_type == "size":
            params["type"] = "classBreaksSize"
        else:
            raise Exception("break_type must be 'color' or 'size'")
        self._create_renderer(params)

    def unique_values_renderer(
        self,
        field: str | None = None,
        field2: str | None = None,
        field3: str | None = None,
        num_types: int | None = None,
        sort_by: str | None = None,
        value_expression: str | None = None,
        value_expression_title: str | None = None,
        outline_optimization_enabled: bool = False,
        size_optimization_enabled: bool = False,
        legend_options: dict | None = None,
        default_symbol_enabled: bool = True,
        statistics: dict | None = None,
        for_binning: bool | None = None,
    ) -> None:
        """
        Generates data-driven visualizations with unique types (or categories) based on a field value from features in a Layer.
        This renderer works with Feature Layers, CSV Layers, GeoJSON Layers, WFS Layers, Oriented Imagery Layer, and OGC Feature Layers.

        Executing this method will automatically update your layer renderer on the map.

        .. warning::
            There are a few points to note with this method:
            - You need to make sure that the layer has been loaded before calling this method.
            - The map can take a few seconds to reflect the changes from the renderer.
            - Calling the `save` or `update` method must be done in a separate cell from this method call due to asynchronous conflicts.
            - If you do not see the layer update, check the console log for any JavaScript errors.

        ==============================      ====================================================================
        **Argument**                        **Description**
        ------------------------------      --------------------------------------------------------------------
        field                               Optional string. The name of the field from which to extract unique
                                            values that will be used for the basis of the data-driven visualization.
                                            This property is ignored if a valueExpression is used.
        ------------------------------      --------------------------------------------------------------------
        field2                              Optional string. Specifies the name of a second attribute field used
                                            to categorize features. All combinations of field, field2, and field3
                                            values are unique categories and may have their own symbol.
                                            This property is ignored if a valueExpression is used.
        ------------------------------      --------------------------------------------------------------------
        field3                              Optional string. Specifies the name of a third attribute field used
                                            to categorize features. All combinations of field, field2, and field3
                                            values are unique categories and may have their own symbol.
                                            This property is ignored if a valueExpression is used.
        ------------------------------      --------------------------------------------------------------------
        num_types                           Optional integer. The number of unique types (or categories) displayed
                                            by teh renderer. Use -1 to display all unique types. The default is 10.
        ------------------------------      --------------------------------------------------------------------
        sort_by                             Optional string. Indicates how to sort the fields in the legend.
                                            If count, unique values/types will be sorted from highest to lowest
                                            based on the count of features that fall in each category. If value,
                                            unique values/types will be sorted in the order they were specified in
                                            the fields parameter. If none, unique values/types will be returned in the
                                            same order they are defined in the statistics parameter or returned from the
                                            uniqueValues statistics query (if the statistics parameter is not defined).

                                            Values: "count" | "value" | "none"
        ------------------------------      --------------------------------------------------------------------
        value_expression                    Optional string. An Arcade expression following the specification
                                            defined by the Arcade Visualization Profile. Expressions may reference
                                            field values using the $feature profile variable and must return a number.
                                            This property overrides the field property and therefore is used instead
                                            of an input field value.
        ------------------------------      --------------------------------------------------------------------
        value_expression_title              Optional string. The title used to describe the value_expression in the Legend.
        ------------------------------      --------------------------------------------------------------------
        outline_optimization_enabled        Optional boolean. Indicates whether the polygon's background fill symbol
                                            outline width should vary based on view scale. Only for polygon layers.
        ------------------------------      --------------------------------------------------------------------
        size_optimization_enabled           Optional boolean. Indicates whether symbol sizes should vary based on view scale.
        ------------------------------      --------------------------------------------------------------------
        legend_options                      Optional dictionary. Options for configuring the legend.

                                            .. code-block:: python

                                                # Example dictionary
                                                legend_options = {
                                                    "title": <string>,
                                                }
        ------------------------------      --------------------------------------------------------------------
        default_symbol_enabled              Optional boolean. Enables the defaultSymbol on the renderer and assigns
                                            it to features with no value.
        ------------------------------      --------------------------------------------------------------------
        statistics                          Optional dictionary. If statistics for the field have already been generated,
                                            then pass the object here to avoid making a second statistics query to the server.
        ------------------------------      --------------------------------------------------------------------
        for_binning                         Optional boolean. Indicates whether the generated renderer is for a
                                            binning visualization. If true, then the input field(s) in this method
                                            should refer to aggregate fields defined in the featureReduction property
                                            of the layer.
        ==============================      ====================================================================



        """
        params = {
            "type": "uniqueValues",
            "field": field,
            "field2": field2,
            "field3": field3,
            "numTypes": num_types,
            "sortBy": sort_by,
            "valueExpression": value_expression,
            "valueExpressionTitle": value_expression_title,
            "outlineOptimizationEnabled": outline_optimization_enabled,
            "sizeOptimizationEnabled": size_optimization_enabled,
            "defaultSymbolEnabled": default_symbol_enabled,
            "symbolType": "2d",
            "layerId": self._layer_id,
        }
        if statistics:
            params["statistics"] = statistics
        if legend_options:
            params["legendOptions"] = legend_options
        if for_binning:
            params["forBinning"] = for_binning

        self._create_renderer(params)

    def _create_renderer(self, params):
        self._source._smart_mapping_params = {}
        # set the new renderer parameters
        self._source._smart_mapping_params = params
        # the renderer will be updated in the observer in the main Map class
