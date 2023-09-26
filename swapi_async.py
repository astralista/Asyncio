import asyncio
import datetime

import aiohttp
from more_itertools import chunked

from models import Base, Session, SwapiPeople, engine

CHUNK_SIZE = 5


async def fetch(session, url):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            json_data = await response.json()
            return json_data
    except aiohttp.ClientResponseError:
        pass
    except Exception:
        pass
    return None


async def insert_to_db(results):
    async with Session() as session:
        people_list = []
        for person_json in results:
            if person_json and person_json.get("url"):
                films = await get_info(session, person_json.get("films"))
                homeworld = await get_info(session, [person_json.get("homeworld")])
                species = await get_info(session, person_json.get("species"))
                starships = await get_info(session, person_json.get("starships"))
                vehicles = await get_info(session, person_json.get("vehicles"))

                if None not in (films, homeworld, species, starships, vehicles):
                    films_titles = [film["title"] for film in films if film is not None]
                    species_names = [
                        specie["name"] for specie in species if specie is not None
                    ]
                    starships_names = [
                        starship["name"]
                        for starship in starships
                        if starship is not None
                    ]
                    vehicles_names = [
                        vehicle["name"] for vehicle in vehicles if vehicle is not None
                    ]

                    person = SwapiPeople(
                        id=int(person_json["url"].split("/")[-2]),
                        birth_year=person_json["birth_year"],
                        eye_color=person_json["eye_color"],
                        films=", ".join(films_titles),
                        gender=person_json["gender"],
                        hair_color=person_json["hair_color"],
                        height=person_json["height"],
                        homeworld=homeworld[0]["name"]
                        if homeworld[0] is not None
                        else None,
                        mass=person_json["mass"],
                        name=person_json["name"],
                        skin_color=person_json["skin_color"],
                        species=", ".join(species_names),
                        starships=", ".join(starships_names),
                        vehicles=", ".join(vehicles_names),
                    )
                    people_list.append(person)

        session.add_all(people_list)
        await session.commit()


async def get_info(session, urls):
    tasks = [fetch(session, url) for url in urls]
    return await asyncio.gather(*tasks)


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.drop_all)
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    async with aiohttp.ClientSession() as client:
        for ids_chunk in chunked(range(100), CHUNK_SIZE):
            coros = [
                fetch(client, f"https://swapi.py4e.com/api/people/{i}/")
                for i in ids_chunk
            ]
            results = await asyncio.gather(*coros)
            await insert_to_db(results)

    current_task = asyncio.current_task()
    tasks_to_await = asyncio.all_tasks() - {current_task}
    for task in tasks_to_await:
        await task


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
