from db_dict.asyncio import DBDict
from asyncio import run, sleep

async def main():
    db_dict = DBDict("test.db", "test")
    db_dict["value1"] = 1
    db_dict["value2"] = 2
    print(db_dict["value1"])
    # sleep here are used to suspend function a little
    # for db_dict managed to save all the values
    await sleep(0.2)
    async for k, v in db_dict.items():
        print(f"k: {k}\tv: {v}")

run(main())