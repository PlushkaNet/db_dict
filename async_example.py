from db_dict.asyncio import DBDict
from asyncio import run, sleep
from dataclasses import dataclass
from datetime import datetime


class Data:
    def __init__(self):
        self.data = "data"
        self.list = [1, 5, 9]


@dataclass
class Info:
    title: str
    description: str


@dataclass
class Session:
    expires: datetime
    info: Info

async def main():
    db_dict = DBDict("test.db", "test")
    db_dict["value1"] = 1
    db_dict["value2"] = 2
    session = Session(
        datetime.now(),
        Info("title", "desc")
    )
    db_dict["session"] = session
    print(await db_dict["value1"])
    # sleep here are used to suspend function a little
    # for db_dict managed to save all the values
    await sleep(0.2)

    async for k, v in db_dict.items():
        print(f"k: {k}\tv: {v}")

run(main())
