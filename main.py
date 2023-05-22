from fastapi import FastAPI

app = FastAPI()


@app.get("/lookup/{vin}")
async def lookup_vehicle(vin):
    """
    Looks up a vechicle by VIN.

    TODO:
        - add pydantic model for vin (It should contain exactly 17
            alphanumeric characters.)
        - add pydantic model for vehicle (the return obj)
    """
    # check cache
    # if not in cache
        # call vPIC API
        # store result in cache

    return {
        "vin": vin,
        "make": "",
        "model": "",
        "model_year": "",
        "body_class": "",
        "from_cache": False,
    }


@app.delete("/remove/{vin}")
async def remove_vehicle(vin):
    """
    Removes a vehicle by VIN.
    """
    # remove from cache
    return {
        "vin": vin,
        "removal_success": True,
    }


@app.post("/export")
async def export_cache():
    """
    Exports the SQLite database cache and return a binary file (parquet
    format) containing the data in the cache.
    """
    # put cache into parquet file
    return
