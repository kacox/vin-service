from fastapi import FastAPI
from db import get_connection, VehicleTable

import requests

NHSTA_BASE_URL = "https://vpic.nhtsa.dot.gov/api/"


app = FastAPI()


def extract_from_response(response):
    vin, make, model, model_year, body_class = None, None, None, None, None

    vin = response["SearchCriteria"].lstrip("VIN:")
    for result in response["Results"]:
        if result["Variable"] == "Make":
            make = result["Value"]
        if result["Variable"] == "Model":
            model = result["Value"]
        if result["Variable"] == "Model Year":
            model_year = result["Value"]
        if result["Variable"] == "Body Class":
            body_class = result["Value"]

    return {
        "vin": vin,
        "make": make,
        "model": model,
        "model_year": model_year,
        "body_class": body_class,
    }


@app.get("/lookup/{vin}")
async def lookup_vehicle(vin):
    """
    Looks up a vechicle by VIN.

    TODO:
        - add pydantic model for vin (It should contain exactly 17
            alphanumeric characters.)
        - add pydantic model for vehicle (the return obj)
    """
    conn = get_connection()

    with conn:
        vehicle = VehicleTable.get_by_vin(conn, vin)
        if vehicle:
            vehicle["from_cache"] = True
        else:
            response = requests.get(
                NHSTA_BASE_URL + f"/vehicles/DecodeVin/{vin}", params={"format": "json"},
            ).json()
            vehicle_data = extract_from_response(response)
            vehicle = VehicleTable.create(conn, vehicle_data)
            print(f"lookup_vehicle:vehicle post insert is {vehicle}")
            vehicle["from_cache"] = False

    return vehicle


@app.delete("/remove/{vin}")
async def remove_vehicle(vin):
    """
    Removes a vehicle by VIN.
    """
    conn = get_connection()

    with conn:
        pass

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
