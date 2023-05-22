import sqlite3

from pandas import read_sql_query


conn = None


def get_connection():
    # in production would use SQLAlchemy to manage connection pool
    global conn
    if not conn:
        conn = sqlite3.connect("vehicle.db")
    return conn


class VehicleTable:

    def create(conn, data):
        params = (
            data["body_class"],
            data["make"],
            data["model"],
            data["model_year"],
            data["vin"],
        )
        print(f"create: params are {params}")
        cur = conn.execute(
            """
            INSERT INTO vehicle VALUES (?, ?, ?, ?, ?)
            """,
            params,
        )
        vehicle = VehicleTable.get_by_vin(conn, data["vin"])
        print(f"create: vehicle data post insert is {vehicle}")
        return vehicle

    def delete_by_vin(conn, vin):
        conn.execute(
            """
            DELETE from vehicle WHERE vin = ?
            """,
            (vin,)
        )
        return VehicleTable.get_by_vin(conn, vin)

    def get_by_vin(conn, vin):
        cur = conn.execute(
            """
            SELECT * FROM vehicle WHERE vin = ?
            """,
            (vin,)
        )

        vehicle_data = cur.fetchone()
        print(f"get_by_vin: vehicle data from cache is {vehicle_data}")
        if not vehicle_data:
            return None
        else:
            return {
                "body_class": vehicle_data[0],
                "make": vehicle_data[1],
                "model": vehicle_data[2],
                "model_year": vehicle_data[3],
                "vin": vehicle_data[4],
            }

    def get_db_as_parquet(conn):
        db_dataframe = read_sql_query("SELECT * FROM vehicle", conn)
        return db_dataframe.to_parquet('vehicle.parq', index=False)
