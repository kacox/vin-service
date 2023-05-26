import logging
import logging.config
from dataclasses import asdict
from typing import Annotated

import requests
from fastapi import FastAPI, Path, Response

from db import Vehicle, VehicleTable, VIN_REGEX


NHSTA_BASE_URL = "https://vpic.nhtsa.dot.gov/api/"

logging.config.fileConfig("logging.conf")
LOGGER = logging.getLogger("vinService")

app = FastAPI()


def extract_from_response(response):
    vin, make, model, model_year, body_class = (
        None, None, None, None, None,
    )

    vin = response["SearchCriteria"].lstrip("VIN:")
    for result in response["Results"]:
        if result["Variable"] == "Make":
            make = result["Value"]
        elif result["Variable"] == "Model":
            model = result["Value"]
        elif result["Variable"] == "Model Year":
            model_year = result["Value"]
        elif result["Variable"] == "Body Class":
            body_class = result["Value"]
        else:
            continue

    return Vehicle(
        vin = vin,
        make = make,
        model = model,
        model_year = model_year,
        body_class = body_class,
    )


@app.get("/lookup/{vin}")
async def lookup_vehicle(
    vin: Annotated[str, Path(regex=VIN_REGEX)],
):
    """
    Looks up a vechicle by VIN.
    """
    cached = False
    vehicle = VehicleTable.get_by_vin(vin)
    if vehicle:
        cached = True
    else:
        response = requests.get(
            NHSTA_BASE_URL + f"/vehicles/DecodeVin/{vin}",
            params={"format": "json"},
        ).json()
        vehicle_data = extract_from_response(response)
        LOGGER.info(f"Vehicle {vin} fetched from NHSTA API")
        vehicle = VehicleTable.create(vehicle_data)
    response = asdict(vehicle)
    response["from_cache"] = cached
    return response


@app.delete("/remove/{vin}")
async def remove_vehicle(
    vin: Annotated[str, Path(regex=VIN_REGEX)],
):
    """
    Removes a vehicle by VIN.
    """
    vehicle = VehicleTable.delete_by_vin(vin)
    if vehicle:
        return {
            "vin": vin,
            "removal_success": False,
        }
    else:
        return {
            "vin": vin,
            "removal_success": True,
        }


@app.post("/export")
async def export_cache():
    """
    Exports the SQLite database cache and return a binary file (parquet
    format) containing the data in the cache.

    Note: ideally in production you would return a link (e.g. an S3
    link) that clients could use to download the file instead of
    allowing direct server downloads.
    """
    parquet = VehicleTable.get_db_as_parquet()

    return Response(parquet, status_code=200)
