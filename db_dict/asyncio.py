"""
This is the file contains code of the asyncio DBDict
DBDict is alternative user-friendly method for using dict's,
but without saving values and keys to memory!
Now it's supports only list, dict, int, float and bool types in values,
and only strings in keys, but in future it's gonna change.

Currently only supports SQLite with aiosqlite

If you don't like standart json module, you can install orjson
And DBDict will switch to using orjson instead of json
"""

from aiosqlite import connect
import sqlite3
from asyncio import get_running_loop, new_event_loop, set_event_loop
from .exceptions import *

# tryes to import faster orjson module
# if its not found, uses standart json module
try:
    from orjson import dumps, loads
except ModuleNotFoundError:
    from json import dumps, loads

class DBDict:
    def __init__(self, path, name, autosetup=True):
        self.path = path
        self.name = "dict_" + name

        if autosetup:
            self.setup()

    # This function will setup DBDict for correct work
    def setup(self):
        try:
            loop = get_running_loop()
        except RuntimeError:
            loop = new_event_loop()
            set_event_loop(loop)
        self._asyncio_loop = loop
        self.__sync_setup()
        # loop.create_task(self._setup())
    # async setup (currently unused)
    # All setup work are in this function
    async def _setup(self):
        async with connect(self.path) as db:
            async with db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='dicts_info'") as cursor:
                data = await cursor.fetchone()
                if not data:
                    await cursor.execute("CREATE TABLE dicts_info(NAME TEXT PRIMARY KEY)")
                await cursor.execute("SELECT 1 FROM dicts_info WHERE NAME = ?", (self.name,))
                data = await cursor.fetchone()
                if not data:
                    await cursor.execute("INSERT INTO dicts_info VALUES (?)", (self.name,))
                    await cursor.execute(f"CREATE TABLE {self.name} (D_KEY TEXT PRIMARY KEY, D_VALUE TEXT, VALUE_TYPE INT)")
            await db.commit()

    # currently sync setup is used, because thread need to wait until setup complete
    def __sync_setup(self):
        with sqlite3.connect(self.path) as db:
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

    # currently sync, works fine if json is not so big
    # otherwise it will blocking
    def __serialize_object(self, obj) -> tuple[str, int]:
        obj_type = type(obj)
        if obj_type == str:
            return obj, 0
        if obj_type == dict or obj_type == list:
            try:
                serialized = dumps(obj)
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
    
    # the same as for __serialize_object
    def __deserialize_object(self, obj:str, otype):
        if otype == 0: return obj
        if otype == 1: return loads(obj)
        if otype == 2: return int(obj)
        if otype == 3: return float(obj)
        if otype == 4: return bool(int(obj))

    async def set(self, key, value):
        async with connect(self.path) as db:
            value, vtype = self.__serialize_object(value)
            await db.execute(f"INSERT INTO {self.name} VALUES (?, ?, ?) ON CONFLICT (D_KEY) DO UPDATE SET D_VALUE = EXCLUDED.D_VALUE, VALUE_TYPE = EXCLUDED.VALUE_TYPE", (key, value, vtype,))
            await db.commit()

    def __setitem__(self, key, value):
        self._asyncio_loop.create_task(self.set(key, value))

    async def get(self, key):
        async with connect(self.path) as db:
            async with db.execute(f"SELECT D_VALUE, VALUE_TYPE FROM {self.name} WHERE D_KEY = ?", (key,)) as cursor:
                value = await cursor.fetchone()
                if value:
                    value = self.__deserialize_object(value[0], value[1])
                else:
                    value = None
        
        return value

    def __getitem__(self, key):
        self._asyncio_loop.create_task(self.get(key))

    async def contains(self, key):
        async with connect(self.path) as db:
            async with db.execute(f"SELECT 1 FROM {self.name} WHERE D_KEY = ?", (key,)) as cursor:
                data = await cursor.fetchone()

        if data and data[0] == 1: return True
        return False

    def __contains__(self, key):
        self._asyncio_loop.create_task(self.contains(key))

    async def pop(self, key):
        async with connect(self.path) as db:
            await db.execute(f"DELETE FROM {self.name} WHERE D_KEY = ?", (key,))
            await db.commit()

    async def __fetch_values(self, values:str):
        async with connect(self.path) as db:
            async with db.execute(f"SELECT {values}, VALUE_TYPE FROM {self.name}") as cursor:
                async for value in cursor:
                    yield value
    
    async def __create_generator(self, values:str):
        async for i in self.__fetch_values(values):
            yield self.__deserialize_object(i[0], i[1])

    async def keys(self): return self.__create_generator("D_KEY")
    async def values(self): return self.__create_generator("D_VALUE")
    async def items(self):
        async for i in self.__fetch_values("D_KEY, D_VALUE"):
            yield i[0], self.__deserialize_object(i[1], i[2])

    # here will be context manager in the future
    async def __aenter__(self):
        pass
    async def __aexit__(self):
        pass