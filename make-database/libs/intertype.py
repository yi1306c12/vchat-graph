import dataclasses
import numpy
import pandas

@dataclasses.dataclass
class InterType:
    pandas: numpy.dtype
    sql: str


TEXT = InterType(
    pandas=numpy.dtype(numpy.object_),
    sql = 'TEXT'
)


INT = InterType(
    pandas=numpy.dtype(numpy.int64),
    sql = 'INT'
)


_TYPE_CONVERT_DICT = {
    TEXT.pandas : TEXT,
    INT.pandas : INT,
}


def convert_from_dtype(dtypes: pandas.Series):
    return {
        name : _TYPE_CONVERT_DICT[dtype] for name, dtype in dtypes.items()
    }