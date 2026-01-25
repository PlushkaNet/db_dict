"""
This is the file contains code of the DBDict
DBDict is alternative user-friendly method for using dict's,
but without saving values and keys to memory!
Now it's supports only list, dict, int, float and bool types in values,
and only strings in keys, but in future it's gonna change.

Currently only supports SQLite
"""

from sqlite3 import connect
import json

"""
There are two types of errors.
First (UnserializableError) is for objects that cannot be serialized
(like dicts/lists with classes/functions).
And second (UnsupportedType) is for directly passed objects/functions (unzerializble objects)
"""

class UnserializableError(Exception):
    def __init__(self, text):
        super().__init__(text)

class UnsupportedType(Exception):
    def __init__(self, text):
        super().__init__(text)

"""
Types codes:
0 - str (original)
1 - json
2 - int
3 - float
4 - bool
"""

class DBDict:
    def __setup__(self):
        with connect(self.path) as db:
            cursor = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='dicts_info'")
            data = cursor.fetchone()
            if not data:
                cursor.execute("CREATE TABLE dicts_info(NAME TEXT PRIMARY KEY)")
            cursor.execute("SELECT 1 FROM dicts_info WHERE NAME = ?", (self.name,))
            data = cursor.fetchone()
            if not data:
                cursor.execute("INSERT INTO dicts_info VALUES (?)", (self.name,))
                cursor.execute(f"CREATE TABLE {self.name} (D_KEY TEXT PRIMARY KEY, D_VALUE TEXT, VALUE_TYPE INT)")
            cursor.close()
            db.commit()

    # name MUST be a valid input in latin without spaces, etc.
    # it can only contain chars (a, b, c) and _
    def __init__(self, path, name:str):
        self.name = "dict_" + name
        self.path = path
        self.__setup__()

    def __convert_(self, obj) -> tuple[str, int]:
        obj_type = type(obj)
        if obj_type == str:
            return obj, 0
        if obj_type == dict or obj_type == list:
            try:
                serialized = json.dumps(obj)
            except:
                raise UnserializableError("Object cannot be serialized, because it probably contains unusual data (like classes/functions)")
            else:
                return serialized, 1
        if obj_type == int:
            return str(obj), 2
        if obj_type == float:
            return str(obj), 3
        if obj_type == bool:
            return str(int(obj)), 4
        raise UnsupportedType("Object is not supported type")
        
    def __deconvert_(self, obj:str, otype):
        if otype == 0: return obj
        if otype == 1: return json.loads(obj)
        if otype == 2: return int(obj)
        if otype == 3: return float(obj)
        if otype == 4: return bool(int(obj))

    def __setitem__(self, key, value):
        with connect(self.path) as db:
            # types of key and value MUST be str, otherwise it will throw an error
            value, vtype = self.__convert_(value)
            db.execute(f"INSERT INTO {self.name} VALUES (?, ?, ?) ON CONFLICT (D_KEY) DO UPDATE SET D_VALUE = EXCLUDED.D_VALUE, VALUE_TYPE = EXCLUDED.VALUE_TYPE", (key, value, vtype,))
            db.commit()
    
    def __getitem__(self, key):
        with connect(self.path) as db:
            # type of key MUST be str, otherwise it will throw an error
            cursor = db.execute(f"SELECT D_VALUE, VALUE_TYPE FROM {self.name} WHERE D_KEY = ?", (key,))
            value = cursor.fetchone()
            if value:
                value = self.__deconvert_(value[0], value[1])
            else:
                value = None
            cursor.close()
        
        return value
    
    def __contains__(self, key):
        with connect(self.path) as db:
            cursor = db.execute(f"SELECT 1 FROM {self.name} WHERE D_KEY = ?", (key,))
            data = cursor.fetchone()
            cursor.close()

        return bool(data)
    
    def pop(self, key):
        with connect(self.path) as db:
            db.execute(f"DELETE FROM {self.name} WHERE D_KEY = ?", (key,))
            db.commit()

    def __fetch_values_(self, values:str):
        with connect(self.path) as db:
            cursor = db.execute(f"SELECT {values}, VALUE_TYPE FROM {self.name}")
            data = cursor.fetchall()
            cursor.close()
        return data
    
    def __generator_(self, values:str):
        data = self.__fetch_values_(values)
        yield from [self.__deconvert_(i[0], i[1]) for i in data]
    
    def keys(self): return self.__generator_("D_KEY")
    def values(self): return self.__generator_("D_VALUE")
    def items(self):
        data = self.__fetch_values_("D_KEY, D_VALUE")
        yield from [(i[0], self.__deconvert_(i[1], i[2])) for i in data]