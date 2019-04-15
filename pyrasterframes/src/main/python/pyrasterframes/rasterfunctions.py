"""
This module creates explicit Python functions that map back to the existing Scala
implementations. Most functions are standard Column functions, but those with unique
signatures are handled here as well.
"""


from __future__ import absolute_import
from pyspark.sql.types import *
from pyspark.sql.column import Column, _to_java_column
from .context import RFContext


THIS_MODULE = 'pyrasterframes'


def _context_call(name, *args):
    f = RFContext.active().lookup(name)
    return f(*args)


def _celltype(cellTypeStr):
    """ Convert the string cell type to the expected CellType object."""
    return _context_call('cell_type', cellTypeStr)


def _create_assembleTile():
    """ Create a function mapping to the Scala implementation."""
    def _(colIndex, rowIndex, cellData, numCols, numRows, cellType):
        jfcn = RFContext.active().lookup('assemble_tile')
        return Column(jfcn(_to_java_column(colIndex), _to_java_column(rowIndex), _to_java_column(cellData), numCols, numRows, _celltype(cellType)))
    _.__name__ = 'assemble_tile'
    _.__doc__ = "Create a Tile from  a column of cell data with location indices"
    _.__module__ = THIS_MODULE
    return _


def _create_arrayToTile():
    """ Create a function mapping to the Scala implementation."""
    def _(arrayCol, numCols, numRows):
        jfcn = RFContext.active().lookup('array_to_tile')
        return Column(jfcn(_to_java_column(arrayCol), numCols, numRows))
    _.__name__ = 'array_to_tile'
    _.__doc__ = "Convert array in `arrayCol` into a Tile of dimensions `numCols` and `numRows'"
    _.__module__ = THIS_MODULE
    return _


def _create_convertCellType():
    """ Create a function mapping to the Scala implementation."""
    def _(tileCol, cellType):
        jfcn = RFContext.active().lookup('convert_cell_type')
        return Column(jfcn(_to_java_column(tileCol), _celltype(cellType)))
    _.__name__ = 'convert_cell_type'
    _.__doc__ = "Convert the numeric type of the Tiles in `tileCol`"
    _.__module__ = THIS_MODULE
    return _


def _create_makeConstantTile():
    """ Create a function mapping to the Scala implementation."""
    def _(value, cols, rows, cellType):
        jfcn = RFContext.active().lookup('make_constant_tile')
        return Column(jfcn(value, cols, rows, cellType))
    _.__name__ = 'make_constant_tile'
    _.__doc__ = "Constructor for constant tile column"
    _.__module__ = THIS_MODULE
    return _


def _create_tileZeros():
    """ Create a function mapping to the Scala implementation."""
    def _(cols, rows, cellType = 'float64'):
        jfcn = RFContext.active().lookup('tile_zeros')
        return Column(jfcn(cols, rows, cellType))
    _.__name__ = 'tile_zeros'
    _.__doc__ = "Create column of constant tiles of zero"
    _.__module__ = THIS_MODULE
    return _


def _create_tileOnes():
    """ Create a function mapping to the Scala implementation."""
    def _(cols, rows, cellType = 'float64'):
        jfcn = RFContext.active().lookup('tile_ones')
        return Column(jfcn(cols, rows, cellType))
    _.__name__ = 'tile_ones'
    _.__doc__ = "Create column of constant tiles of one"
    _.__module__ = THIS_MODULE
    return _


def _create_rasterize():
    """ Create a function mapping to the Scala rasterize function. """
    def _(geometryCol, boundsCol, valueCol, numCols, numRows):
        jfcn = RFContext.active().lookup('rasterize')
        return Column(jfcn(_to_java_column(geometryCol), _to_java_column(boundsCol), _to_java_column(valueCol), numCols, numRows))
    _.__name__ = 'rasterize'
    _.__doc__ = 'Create a tile where cells in the grid defined by cols, rows, and bounds are filled with the given value.'
    _.__module__ = THIS_MODULE
    return _


def _create_reproject_geometry():
    """ Create a function mapping to the Scala reproject_geometry function. """
    def _(geometryCol, srcCRSName, dstCRSName):
        jfcn = RFContext.active().lookup('reproject_geometry')
        return Column(jfcn(_to_java_column(geometryCol), srcCRSName, dstCRSName))
    _.__name__ = 'reproject_geometry'
    _.__doc__ = """Reproject a column of geometry given the CRS names of the source and destination.
Currently supported registries are EPSG, ESRI, WORLD, NAD83, & NAD27.
An example of a valid CRS name is EPSG:3005.
"""
    _.__module__ = THIS_MODULE
    return _


def _create_explode_tiles():
    """ Create a function mapping to Scala explode_tiles function """
    def _(*args):
        jfcn = RFContext.active().lookup('explode_tiles')
        jcols = [_to_java_column(arg) for arg in args]
        return Column(jfcn(RFContext.active().list_to_seq(jcols)))
    _.__name__ = 'explode_tiles'
    _.__doc__ = 'Create a row for each cell in Tile.'
    _.__module__ = THIS_MODULE
    return _


def _create_explode_tiles_sample():
    """ Create a function mapping to Scala explode_tiles_sample function"""
    def _(sample_frac, seed, *tile_cols):
        jfcn = RFContext.active().lookup('explode_tiles_sample')
        jcols = [_to_java_column(arg) for arg in tile_cols]
        return Column(jfcn(sample_frac, seed, RFContext.active().list_to_seq(jcols)))

    _.__name__ = 'explode_tiles_sample'
    _.__doc__ = 'Create a row for a sample of cells in Tile columns.'
    _.__module__ = THIS_MODULE
    return _


def _create_maskByValue():
    """ Create a function mapping to Scala mask_by_value function """
    def _(data_tile, mask_tile, mask_value):
        jfcn = RFContext.active().lookup('mask_by_value')
        return Column(jfcn(_to_java_column(data_tile), _to_java_column(mask_tile), _to_java_column(mask_value)))
    _.__name__ = 'mask_by_value'
    _.__doc__ = 'Generate a tile with the values from the data tile, but where cells in the masking tile contain the masking value, replace the data value with NODATA.'
    _.__module__ = THIS_MODULE
    return _


_rf_unique_functions = {
    'array_to_tile': _create_arrayToTile(),
    'assemble_tile': _create_assembleTile(),
    'cell_types': lambda: _context_call('cell_types'),
    'convert_cell_type': _create_convertCellType(),
    'explode_tiles': _create_explode_tiles(),
    'explode_tiles_sample': _create_explode_tiles_sample(),
    'make_constant_tile': _create_makeConstantTile(),
    'mask_by_value': _create_maskByValue(),
    'rasterize': _create_rasterize(),
    'reproject_geometry': _create_reproject_geometry(),
    'tile_ones': _create_tileOnes(),
    'tile_zeros': _create_tileZeros(),
}


_rf_column_scalar_functions = {
    'with_no_data': 'Assign a `NoData` value to the Tiles in the given Column.',
    'local_add_scalar': 'Add a scalar to a Tile',
    'local_add_scalar_int': 'Add a scalar to a Tile',
    'local_subtract_scalar': 'Subtract a scalar from a Tile',
    'local_subtract_scalar_int': 'Subtract a scalar from a Tile',
    'local_multiply_scalar': 'Multiply a Tile by a scalar',
    'local_multiply_scalar_int': 'Multiply a Tile by a scalar',
    'local_divide_scalar': 'Divide a Tile by a scalar',
    'local_divide_scalar_int': 'Divide a Tile by a scalar',
    'local_less_scalar': 'Return a Tile with values equal 1 if the cell is less than a scalar, otherwise 0',
    'local_less_scalar_int': 'Return a Tile with values equal 1 if the cell is less than a scalar, otherwise 0',
    'local_less_equal_scalar': 'Return a Tile with values equal 1 if the cell is less than or equal to a scalar, otherwise 0',
    'local_less_equal_scalar_int': 'Return a Tile with values equal 1 if the cell is less than or equal to a scalar, otherwise 0',
    'local_greater_scalar': 'Return a Tile with values equal 1 if the cell is greater than a scalar, otherwise 0',
    'local_greater_scalar_int': 'Return a Tile with values equal 1 if the cell is greater than a scalar, otherwise 0',
    'local_greater_equal_scalar': 'Return a Tile with values equal 1 if the cell is greater than or equal to a scalar, otherwise 0',
    'local_greater_equal_scalar_int': 'Return a Tile with values equal 1 if the cell is greater than or equal to a scalar, otherwise 0',
    'local_equal_scalar': 'Return a Tile with values equal 1 if the cell is equal to a scalar, otherwise 0',
    'local_equal_scalar_int': 'Return a Tile with values equal 1 if the cell is equal to a scalar, otherwise 0',
    'local_unequal_scalar': 'Return a Tile with values equal 1 if the cell is not equal to a scalar, otherwise 0',
    'local_unequal_scalar_int': 'Return a Tile with values equal 1 if the cell is not equal to a scalar, otherwise 0',
}


_rf_column_functions = {
    # ------- RasterFrames functions -------
    'tile_dimensions': 'Query the number of (cols, rows) in a Tile.',
    'envelope': 'Extracts the bounding box (envelope) of the geometry.',
    'tile_to_int_array': 'Flattens Tile into an array of integers. Deprecated in favor of `tile_to_array_int`.',
    'tile_to_double_array': 'Flattens Tile into an array of doubles. Deprecated in favor of `tile_to_array_double`',
    'tile_to_array_int': 'Flattens Tile into an array of integers.',
    'tile_to_array_double': 'Flattens Tile into an array of doubles.',
    'cell_type': 'Extract the Tile\'s cell type',
    'is_no_data_tile': 'Report if the Tile is entirely NODDATA cells',
    'agg_approx_histogram': 'Compute the full column aggregate floating point histogram',
    'agg_stats': 'Compute the full column aggregate floating point statistics',
    'agg_mean': 'Computes the column aggregate mean',
    'agg_data_cells': 'Computes the number of non-NoData cells in a column',
    'agg_no_data_cells': 'Computes the number of NoData cells in a column',
    'tile_histogram': 'Compute the Tile-wise histogram',
    'tile_mean': 'Compute the Tile-wise mean',
    'tile_sum': 'Compute the Tile-wise sum',
    'tile_min': 'Compute the Tile-wise minimum',
    'tile_max': 'Compute the Tile-wise maximum',
    'tile_stats': 'Compute the Tile-wise floating point statistics',
    'render_ascii': 'Render ASCII art of tile',
    'no_data_cells': 'Count of NODATA cells',
    'data_cells': 'Count of cells with valid data',
    'local_add': 'Add two Tiles',
    'local_subtract': 'Subtract two Tiles',
    'local_multiply': 'Multiply two Tiles',
    'local_divide': 'Divide two Tiles',
    'normalized_difference': 'Compute the normalized difference of two tiles',
    'local_agg_stats': 'Compute cell-local aggregate descriptive statistics for a column of Tiles.',
    'agg_local_max': 'Compute the cell-wise/local max operation between Tiles in a column.',
    'agg_local_min': 'Compute the cellwise/local min operation between Tiles in a column.',
    'agg_local_mean': 'Compute the cellwise/local mean operation between Tiles in a column.',
    'agg_local_data_cells': 'Compute the cellwise/local count of non-NoData cells for all Tiles in a column.',
    'agg_local_no_data_cells': 'Compute the cellwise/local count of NoData cells for all Tiles in a column.',
    'mask': 'Where the mask (second) tile contains NODATA, replace values in the source (first) tile with NODATA.',
    'inverse_mask': 'Where the mask (second) tile DOES NOT contain NODATA, replace values in the source (first) tile with NODATA.',
    'local_less': 'Cellwise less than comparison between two tiles',
    'local_less_equal': 'Cellwise less than or equal to comparison between two tiles',
    'local_greater': 'Cellwise greater than comparison between two tiles',
    'local_greater_equal': 'Cellwise greater than or equal to comparison between two tiles',
    'local_equal': 'Cellwise equality comparison between two tiles',
    'local_unequal': 'Cellwise inequality comparison between two tiles',
    'round': 'Round cell values to the nearest integer without changing the cell type',
    'log': 'Performs cell-wise natural logarithm',
    'log10': 'Performs cell-wise logartithm with base 10',
    'log2': 'Performs cell-wise logartithm with base 2',
    'log1p': 'Performs natural logarithm of cell values plus one',
    'exp': 'Performs cell-wise exponential',
    'exp2': 'Compute 2 to the power of cell values',
    'exp10': 'Compute 10 to the power of cell values',
    'expm1': 'Performs cell-wise exponential, then subtract one',
    'resample': 'Resample tile to different size based on scalar factor or tile whose dimension to match',
    'st_extent': 'Compute the extent/bbox of a Geometry',
    'extent_geometry': 'Convert the given extent/bbox to a polygon',

    # ------- JTS functions -------
    # spatial constructors
    'st_geomFromGeoHash': '',
    'st_geomFromWKT': '',
    'st_geomFromWKB': '',
    'st_lineFromText': '',
    'st_makeBox2D': '',
    'st_makeBBox': '',
    'st_makePolygon': '',
    'st_makePoint': '',
    'st_makeLine': '',
    'st_makePointM': '',
    'st_mLineFromText': '',
    'st_mPointFromText': '',
    'st_mPolyFromText': '',
    'st_point': '',
    'st_pointFromGeoHash': '',
    'st_pointFromText': '',
    'st_pointFromWKB': '',
    'st_polygon': '',
    'st_polygonFromText': '',
    # spatial converters
    'st_castToPoint': '',
    'st_castToPolygon': '',
    'st_castToLineString': '',
    'st_byteArray': '',
    # spatial accessors
    'st_boundary': '',
    'st_coordDim': '',
    'st_dimension': '',
    'st_envelope': '',
    'st_exteriorRing': '',
    'st_geometryN': '',
    'st_geometryType': '',
    'st_interiorRingN': '',
    'st_isClosed': '',
    'st_isCollection': '',
    'st_isEmpty': '',
    'st_isRing': '',
    'st_isSimple': '',
    'st_isValid': '',
    'st_numGeometries': '',
    'st_numPoints': '',
    'st_pointN': '',
    'st_x': '',
    'st_y': '',
    # spatial outputs
    'st_asBinary': '',
    'st_asGeoJSON': '',
    'st_asLatLonText': '',
    'st_asText': '',
    'st_geoHash': '',
    # spatial processors
    'st_bufferPoint': '',
    'st_antimeridianSafeGeom': '',
    # spatial relations
    'st_translate': '',
    'st_contains': '',
    'st_covers': '',
    'st_crosses': '',
    'st_disjoint': '',
    'st_equals': '',
    'st_intersects': '',
    'st_overlaps': '',
    'st_touches': '',
    'st_within': '',
    'st_relate': '',
    'st_relateBool': '',
    'st_area': '',
    'st_closestPoint': '',
    'st_centroid': '',
    'st_distance': '',
    'st_distanceSphere': '',
    'st_length': '',
    'st_aggregateDistanceSphere': '',
    'st_lengthSphere': '',
}


__all__ = list(_rf_column_functions.keys()) + \
          list(_rf_column_scalar_functions.keys()) + \
          list(_rf_unique_functions.keys())


def _create_column_function(name, doc=""):
    """ Create a mapping to Scala UDF for a column function by name"""
    def _(*args):
        jfcn = RFContext.active().lookup(name)
        jcols = [_to_java_column(arg) for arg in args]
        return Column(jfcn(*jcols))
    _.__name__ = name
    _.__doc__ = doc
    _.__module__ = THIS_MODULE
    return _


def _create_columnScalarFunction(name, doc=""):
    """ Create a mapping to Scala UDF for a (column, scalar) -> column function by name"""
    def _(col, scalar):
        jfcn = RFContext.active().lookup(name)
        return Column(jfcn(_to_java_column(col), scalar))
    _.__name__ = name
    _.__doc__ = doc
    _.__module__ = THIS_MODULE
    return _


def _register_functions():
    """ Register each function in the scope"""
    for name, doc in _rf_column_functions.items():
        globals()[name] = _create_column_function(name, doc)

    for name, doc in _rf_column_scalar_functions.items():
        globals()[name] = _create_columnScalarFunction(name, doc)

    for name, func in _rf_unique_functions.items():
        globals()[name] = func


_register_functions()