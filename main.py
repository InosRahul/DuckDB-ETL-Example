import duckdb
from duckdb import DuckDBPyConnection
import os

create_table_vehicles = """
        DROP TABLE IF EXISTS vehicles CASCADE;
        CREATE TABLE vehicles (
            VIN VARCHAR(10),
            County VARCHAR,
            City VARCHAR,
            State VARCHAR(2),
            Postal_Code VARCHAR(10),
            Model_Year INTEGER,
            Make VARCHAR,
            Model VARCHAR,
            Electric_Vehicle_Type VARCHAR,
            CAFV_Eligibility VARCHAR,
            Electric_Range INTEGER,
            Base_MSRP DECIMAL,
            Legislative_District INTEGER,
            DOL_Vehicle_ID VARCHAR(15),
            Vehicle_Location VARCHAR,
            Electric_Utility VARCHAR,
            Census_Tract VARCHAR(11)
    );
"""

select_all_vehicles = """
    SELECT * from vehicles
"""

describle_vehicles = """
    DESCRIBE vehicles
"""


def insertData(cursor: DuckDBPyConnection):
    cursor.sql(
        "INSERT INTO vehicles SELECT * FROM read_csv_auto('data/Electric_Vehicle_Population_Data.csv');"
    )

    cursor.commit()


def countCarsPerCity(cursor: DuckDBPyConnection):
    cars_per_city_query = """
    SELECT city, COUNT(VIN) AS cars_in_city 
    FROM vehicles 
    GROUP BY city
    """

    cursor.execute(cars_per_city_query)
    results = cursor.fetchall()
    return results


def topThreePopluarVehicles(cursor: DuckDBPyConnection):
    top_three_popular_vehicles = """
    SELECT CONCAT(make, ' ', model), 
    COUNT(VIN) as vehicle_count 
    FROM vehicles 
    GROUP BY make, model 
    ORDER BY vehicle_count DESC
    LIMIT 3
    """

    cursor.execute(top_three_popular_vehicles)
    results = cursor.fetchall()
    return results


def mostPopularVehicleByPostalCode(cursor: DuckDBPyConnection):
    most_popular_vehicle_by_postal_code = """
    SELECT CONCAT(t1.make, ' ', t1.model),
    t1.postal_code
    FROM (
        SELECT model, make, postal_code, COUNT(vin) as vehicle_count,
        ROW_NUMBER() OVER(PARTITION BY postal_code ORDER BY COUNT(vin) DESC) as rank
        FROM vehicles
        GROUP BY postal_code, model, make
    ) t1
    WHERE t1.rank = 1
    """

    cursor.execute(most_popular_vehicle_by_postal_code)
    results = cursor.fetchall()
    return results


def numCarsByModelYear(cursor: DuckDBPyConnection):
    num_cars_by_model_year = """
    SELECT model_year, COUNT(vin) as num_cars_year
    FROM vehicles
    GROUP BY model_year
    """

    cursor.execute(num_cars_by_model_year)
    rows = cursor.fetchall()

    return rows


def main():
    conn = duckdb.connect(database="electric_vehicles_data")

    cursor = conn.cursor()

    cursor.execute(create_table_vehicles)

    insertData(cursor)

    cars_per_city = countCarsPerCity(cursor)

    print(cars_per_city)

    top_three_popular_vehicles = topThreePopluarVehicles(cursor)

    print(top_three_popular_vehicles)

    most_popular_vehicle_by_postal_code = mostPopularVehicleByPostalCode(cursor)

    print(most_popular_vehicle_by_postal_code)

    rows = numCarsByModelYear(cursor)

    output_dir = "data/results"
    os.makedirs(output_dir, exist_ok=True)

    for row in rows:
        year = row[0]
        count = row[1]

        year_dir = os.path.join(output_dir, str(year))
        os.makedirs(year_dir, exist_ok=True)

        file_path = os.path.join(year_dir, f"{year}.parquet")

        with open(file_path, "w") as file:
            file.write(f"Count: {count}")

    conn.close()


if __name__ == "__main__":
    main()
