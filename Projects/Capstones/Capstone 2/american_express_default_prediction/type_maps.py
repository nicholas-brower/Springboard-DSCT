import numpy as np
py_to_np = {
    'int': np.float_, 'float': np.float_, 'str': np.str_, 'bool': np.bool_, 
    'datetime': np.datetime64
}
TYPE_MAP_KEYS = {
    'type': {
        'float': 0, 'int': 10, 'str': 20, 'bool': 10, 'object': 40
    },
    'environment': {
        'py': {'float': 0, 'int': 10, 'str': 20, 'bool': 30, 'object': 40},
        'np': {
            'np.double': 0, 'np.half': 1, 'np.float16': 1, 'np.int_': 10, 'np.str_': 21,
            'np.bool_': 30, 'np.object_': 40
        },
        'sql': {'FLOAT': 0, 'INT': 10, 'BIT': 30, 'VARCHAR': 20, 'NVARCHAR': 21}
    }
}
TYPE_MAP = {
    0: [float, np.double, 'FLOAT'], 1: [float, np.half, 'FLOAT'], 
    10: [int, np.int_, 'INT'], 20: [str, np.object_, 'VARCHAR'], 
    21: [str, np.str_, 'NVARCHAR'], 30: [bool, np.bool_, 'BIT'],
    40: [object, np.object_, '']
}