"""
This is the file contains code of the DBDict
DBDict is alternative user-friendly method for using dict's,
but without saving values and keys to memory!
In future there will be json serializer and deserializer.
Now it's supports only strings in keys and values but in future it's gonna change.

Currently only supports SQLite
"""

from sqlite3 import connect

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
                cursor.execute(f"CREATE TABLE {self.name} (D_KEY TEXT PRIMARY KEY, D_VALUE TEXT)")
            cursor.close()
            db.commit()

    # name MUST be a valid input in latin without spaces, etc.
    # it can only contain chars (a, b, c) and _
    def __init__(self, path, name:str):
        self.name = "dict_" + name
        self.path = path
        self.__setup__()

    def __setitem__(self, key, value):
        with connect(self.path) as db:
            # types of key and value MUST be str, otherwise it will throw an error
            db.execute(f"INSERT INTO {self.name} VALUES (?, ?) ON CONFLICT (D_KEY) DO UPDATE SET D_VALUE = EXCLUDED.D_VALUE", (key, value,))
            db.commit()
    
    def __getitem__(self, key):
        with connect(self.path) as db:
            # type of key MUST be str, otherwise it will throw an error
            cursor = db.execute(f"SELECT D_VALUE FROM {self.name} WHERE D_KEY = ?", (key,))
            value = cursor.fetchone()
            if value:
                value = value[0]
            else:
                value = None
            cursor.close()
        
        return value
    
    def pop(self, key):
        with connect(self.path) as db:
            db.execute(f"DELETE FROM {self.name} WHERE D_KEY = ?", (key,))
            db.commit()
    
    def __generator_(self, value:str, unpack):
        with connect(self.path) as db:
            cursor = db.execute(f"SELECT {value} FROM {self.name}")
            data = cursor.fetchall()
            cursor.close()
        
        if unpack:
            yield from [i[0] for i in data]
        else:
            yield from data
    
    def keys(self): return self.__generator_("D_KEY", True)
    def values(self): return self.__generator_("D_VALUE", True)
    def items(self): return self.__generator_("D_KEY, D_VALUE", False)