# VIN Service

A simple FastAPI webservice that can lookup vehicles by their VIN
(vehicle identification number).

## Setup

Create a virtual environment and install the packages specified in the
`requirements.txt` file.

Create the SQLite3 database "cache" by running the `setup_db.py` script.

Run the webserver using:
```
uvicorn main:app --reload
```

## API

`GET /lookup/{vin}`

Retrieves vehicle information using the provided VIN.

`DELETE /remove/{vin}`

Removes the specified vehicle's information from the cache.

`POST /export`

Returns a parquet file containing all vehicles currently in the cache.

## Additional info

This webservice uses the vPIC API provided by the NHSTA to lookup
vehicle information.
