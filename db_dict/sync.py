"""
This is the file contains code of the DBDict
DBDict is alternative user-friendly method for using dict's,
but without saving values and keys to memory!

Currently only supports SQLite

It uses pickle module to serialize and deserialize data
"""

from sqlite3 import connect
import pickle
from .exceptions import *

class DBDict:
    def setup(self):
        """Sets up DBDict\n
        It creates necessary tables to work properly"""
        self.__validate_name()
        with connect(self.path) as db:
            db.execute(f"CREATE TABLE IF NOT EXISTS {self.name}(D_KEY BLOB PRIMARY KEY, D_VALUE BLOB)")
            db.commit()

    # replaces non-ascii letters to "a"
    def __validate_name(self):
        ascii = '_abcdefghijklmnopqrstuvwxyz'+'abcdefghijklmnopqrstuvwxyz'.upper()
        v = ""
        for i in self.name:
            if i not in ascii:
                v += "a"
            else:
                v += i
        self.name = v

    # name MUST be a valid input in latin without spaces, etc.
    # it can only contain chars (a, b, c) and _
    def __init__(self, path, name:str, autosetup=True):
        """Creates DBDict\n
        Arguments are: `path` and `name`\n
        `path` - path in the system where database file is located\n
        `name` - name of the table in database"""
        self.name = "dict_" + name
        self.path = path
        if autosetup:
            self.setup()

    def __serialize(self, obj) -> bytes:
        return pickle.dumps(obj)
        
    def __unserialize(self, obj:bytes):
        return pickle.loads(obj)

    def __setitem__(self, key, value):
        with connect(self.path) as db:
            key = self.__serialize(key)
            value = self.__serialize(value)
            db.execute(f"INSERT INTO {self.name} VALUES (?, ?) ON CONFLICT (D_KEY) DO UPDATE SET D_VALUE = EXCLUDED.D_VALUE", (key, value,))
            db.commit()
    
    def get(self, key, default=None):
        """Gets item from dict\n
        Returns default if key not found\n
        Usage:
        ```
        db = DBDict("db.db", "dict")
        value = db.get("key", default="not found")
        ```"""
        with connect(self.path) as db:
            key = self.__serialize(key)
            cursor = db.execute(f"SELECT D_VALUE FROM {self.name} WHERE D_KEY = ?", (key,))
            value = cursor.fetchone()
            if value:
                value = self.__unserialize(value[0])
            else:
                value = default
            cursor.close()
        
        return value

    # gets item from dict by key
    # raises KeyError if key not found
    def __getitem__(self, key):
        data = self.get(key)
        if data != None: return data
        else: raise KeyError(f"Key {key} does not exists in the database")
    
    def __contains__(self, key):
        with connect(self.path) as db:
            key = self.__serialize(key)
            cursor = db.execute(f"SELECT 1 FROM {self.name} WHERE D_KEY = ?", (key,))
            data = cursor.fetchone()
            cursor.close()

        if data: return True
        return False
    
    def pop(self, key):
        """Deletes value by key in the database"""
        with connect(self.path) as db:
            key = self.__serialize(key)
            db.execute(f"DELETE FROM {self.name} WHERE D_KEY = ?", (key,))
            db.commit()
    
    def __generator(self, value:str):
        with connect(self.path) as db:
            cursor = db.execute(f"SELECT {value} FROM {self.name}")
            for i in cursor:
                yield self.__unserialize(i[0])
            cursor.close()
    
    def keys(self):
        """Return generator of dict's keys\n
        Usage:
        ```
        db = DBDict("db.db", "dict")
        for i in db.keys():
            print(i)
        ```"""
        return self.__generator("D_KEY")
    def values(self):
        """Return generator of dict's values\n
        Usage:
        ```
        db = DBDict("db.db", "dict")
        for i in db.values():
            print(i)
        ```"""
        return self.__generator("D_VALUE")
    def items(self):
        """Return generator of dict's items\n
        Usage:
        ```
        db = DBDict("db.db", "dict")
        for k, v in db.values():
            print(f"key: {k}, value: {v}")
        ```"""
        with connect(self.path) as db:
            cursor = db.execute(f"SELECT D_KEY, D_VALUE FROM {self.name}")
            for i in cursor:
                yield (
                self.__unserialize(i[0]),
                self.__unserialize(i[1])
            )
            cursor.close()
    
    def __len__(self):
        with connect(self.path) as db:
            cursor = db.execute(f"SELECT COUNT(*) FROM {self.name}")
            data = cursor.fetchone()[0]
            cursor.close()
        return data