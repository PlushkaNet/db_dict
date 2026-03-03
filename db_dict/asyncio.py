"""
This is the file contains code of the asyncio DBDict
DBDict is alternative user-friendly method for using dict's,
but without saving values and keys to memory!
Supports a lot of different types, including complex

Currently only supports SQLite with aiosqlite

It uses pickle module to serialize and deserialize data
"""

from aiosqlite import connect
import sqlite3
from asyncio import get_running_loop, new_event_loop, set_event_loop

import pickle

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
                    await cursor.execute(f"CREATE TABLE {self.name} (D_KEY BLOB PRIMARY KEY, D_VALUE BLOB)")
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
                cursor.execute(f"CREATE TABLE {self.name} (D_KEY BLOB PRIMARY KEY, D_VALUE BLOB)")
            cursor.close()
            db.commit()

    # uses pickle to encode
    def __serialize_object(self, obj) -> bytes:
        return pickle.dumps(obj)
    
    # the same as for __serialize_object
    def __deserialize_object(self, obj:bytes):
        return pickle.loads(obj)

    """
    Sets key with value
    Use this, if you need to await your set operations
    """
    async def set(self, key, value):
        async with connect(self.path) as db:
            key = self.__serialize_object(key)
            value = self.__serialize_object(value)
            await db.execute(f"INSERT INTO {self.name} VALUES (?, ?) ON CONFLICT (D_KEY) DO UPDATE SET D_VALUE = EXCLUDED.D_VALUE", (key, value,))
            await db.commit()

    # creates a task with self.set(key, value) method
    def __setitem__(self, key, value):
        self._asyncio_loop.create_task(self.set(key, value))

    """
    Similar to dict.get() method
    default - value will be returned if key not found
    """
    async def get(self, key, default=None):
        async with connect(self.path) as db:
            async with db.execute(f"SELECT D_VALUE FROM {self.name} WHERE D_KEY = ?", (key,)) as cursor:
                value = await cursor.fetchone()
                if value:
                    value = self.__deserialize_object(value[0])
                else:
                    value = default
        
        return value

    # returns a coroutine, so its can be awaited
    # like:
    # value = await dbdict["value"]
    def __getitem__(self, key):
        # self._asyncio_loop.create_task(self.get(key))
        return self.get(key)

    async def contains(self, key):
        async with connect(self.path) as db:
            async with db.execute(f"SELECT 1 FROM {self.name} WHERE D_KEY = ?", (key,)) as cursor:
                data = await cursor.fetchone()

        if data and data[0] == 1: return True
        return False

    # __contains__ currently not implemented
    # # !
    # def __contains__(self, key):
    #     self._asyncio_loop.create_task(self.contains(key))

    async def pop(self, key):
        async with connect(self.path) as db:
            await db.execute(f"DELETE FROM {self.name} WHERE D_KEY = ?", (key,))
            await db.commit()

    # method that fetchs values from DB and returns them as a generator
    async def __fetch_values(self, values:str):
        async with connect(self.path) as db:
            async with db.execute(f"SELECT {values} FROM {self.name}") as cursor:
                async for value in cursor:
                    yield value
    
    # method that deserializes values from __fetch_values
    async def __create_generator(self, value:str):
        async for i in self.__fetch_values(value):
            yield self.__deserialize_object(i[0])

    async def keys(self): return self.__create_generator("D_KEY")
    async def values(self): return self.__create_generator("D_VALUE")
    async def items(self):
        async for i in self.__fetch_values("D_KEY, D_VALUE"):
            yield (
                self.__deserialize_object(i[0]),
                self.__deserialize_object(i[1])
            )

    # here will be context manager in the future
    async def __aenter__(self):
        pass
    async def __aexit__(self):
        pass