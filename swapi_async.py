import asyncio
import datetime

import aiohttp
from more_itertools import chunked

from models import Base, Session, SwapiPeople, engine

CHUNK_SIZE = 5


async def get_people(client, people_id):
    response = await client.get(f"https://swapi.py4e.com/api/people/{people_id}")
    json_data = await response.json()
    return json_data


async def insert_to_db(results):
    async with Session() as session:
        swapi_people_list = [SwapiPeople(json=item) for item in results]
        session.add_all(swapi_people_list)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.drop_all)
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)
    async with aiohttp.ClientSession() as client:
        for ids_chunk in chunked(range(1, 100), CHUNK_SIZE):
            coros = [get_people(client, i) for i in ids_chunk]
            results = await asyncio.gather(*coros)
            insert_to_db_coro = insert_to_db(results)
            asyncio.create_task(insert_to_db_coro)
    current_task = asyncio.current_task()
    tasks_to_await = asyncio.all_tasks() - {
        current_task,
    }
    for task in tasks_to_await:
        await task


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
