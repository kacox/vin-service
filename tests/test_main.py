from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from requests import Response

from db import Vehicle
from main import app, extract_from_response

client = TestClient(app)

TEST_VEHICLE = Vehicle(
    body_class="Truck-Tractor",
    make="PETERBILT",
    model="388",
    model_year="2014",
    vin="1XPWD40X1ED215307",
)

TEST_LOOKUP_RESPONSE = {
    "Count": 4,
    "Message": "Results returned successfully.",
    "SearchCriteria": "VIN:1XPWD40X1ED215307",
    "Results": [
        {
          "Value": "PETERBILT",
          "ValueId": "495",
          "Variable": "Make",
          "VariableId": 26
        },
        {
          "Value": "388",
          "ValueId": "2793",
          "Variable": "Model",
          "VariableId": 28
        },
        {
          "Value": "2014",
          "ValueId": "",
          "Variable": "Model Year",
          "VariableId": 29
        },
        {
          "Value": "Truck-Tractor",
          "ValueId": "66",
          "Variable": "Body Class",
          "VariableId": 5
        },
    ]
}


@patch("main.VehicleTable.get_by_vin")
@patch("main.VehicleTable.create")
def test_lookup_vehicle_in_cache(create, get_by_vin):
    get_by_vin.return_value = TEST_VEHICLE
    response = client.get(f"/lookup/{TEST_VEHICLE.vin}")
    assert response.status_code == 200
    assert response.json()["vin"] == TEST_VEHICLE.vin
    assert response.json()["from_cache"]
    assert not create.called


@patch("main.VehicleTable.create")
@patch("main.requests.get")
@patch("main.VehicleTable.get_by_vin")
def test_lookup_vehicle_not_in_cache(get_by_vin, nhsta_call, create):
    get_by_vin.return_value = None
    response_with_json = Response()
    response_with_json.json = lambda: TEST_LOOKUP_RESPONSE
    nhsta_call.return_value = response_with_json
    create.return_value = TEST_VEHICLE

    response = client.get(f"/lookup/{TEST_VEHICLE.vin}")
    assert response.status_code == 200
    assert response.json()["vin"] == TEST_VEHICLE.vin
    assert not response.json()["from_cache"]
    assert create.called


def test_lookup_vehicle_invalid_vin():
    response = client.get(f"/lookup/123456")
    assert response.status_code == 422


@patch("main.VehicleTable.delete_by_vin")
def test_remove_vehicle_success(delete_by_vin):
    delete_by_vin.return_value = None

    response = client.delete(f"/remove/{TEST_VEHICLE.vin}")
    assert response.status_code == 200
    assert response.json()["vin"] == TEST_VEHICLE.vin
    assert response.json()["success"]


@patch("main.VehicleTable.delete_by_vin")
def test_remove_vehicle_fail(delete_by_vin):
    delete_by_vin.return_value = TEST_VEHICLE

    response = client.delete(f"/remove/{TEST_VEHICLE.vin}")
    assert response.status_code == 200
    assert response.json()["vin"] == TEST_VEHICLE.vin
    assert not response.json()["success"]


def test_remove_vehicle_invalid_vin():
    response = client.delete(f"/remove/123456")
    assert response.status_code == 422


@patch("main.VehicleTable.get_db_as_parquet")
def test_export_success(get_db_as_parquet):
    get_db_as_parquet.return_value = None

    response = client.post(f"/export")
    assert response.status_code == 200
    assert not response.content


def test_extract_from_response():
    result = extract_from_response(TEST_LOOKUP_RESPONSE)
    assert isinstance(result, Vehicle)
    assert result.body_class == "Truck-Tractor"
    assert result.make == "PETERBILT"
    assert result.model == "388"
    assert result.model_year == "2014"
    assert result.vin == "1XPWD40X1ED215307"


def test_extract_from_response_json_empty():
    with pytest.raises(ValueError) as excinfo:
        extract_from_response({})
    assert "Response is empty or missing" in str(excinfo.value)


def test_extract_from_response_json_missing():
    with pytest.raises(ValueError) as excinfo:
        extract_from_response(None)
    assert "Response is empty or missing" in str(excinfo.value)


def test_extract_from_response_json_wrong_structure():
    json_wrong_structure = {
        "Count": 4,
        "Message": "Results returned successfully.",
        "Results": [
            {
              "Value": "1XPWD40X1ED215307",
              "ValueId": None,
              "Variable": "VIN",
              "VariableId": 1
            },
            {
              "Value": "PETERBILT",
              "ValueId": "495",
              "Variable": "Make",
              "VariableId": 26
            },
            {
              "Value": "388",
              "ValueId": "2793",
              "Variable": "Model",
              "VariableId": 28
            },
            {
              "Value": "2014",
              "ValueId": "",
              "Variable": "Model Year",
              "VariableId": 29
            },
            {
              "Value": "Truck-Tractor",
              "ValueId": "66",
              "Variable": "Body Class",
              "VariableId": 5
            },
        ]
    }

    with pytest.raises(KeyError) as excinfo:
        extract_from_response(json_wrong_structure)
    assert "SearchCriteria" in str(excinfo.value)
