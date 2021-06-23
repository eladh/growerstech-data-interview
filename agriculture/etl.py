import pandas as pd
from typing import Tuple
import datetime
import re

import pymysql.cursors
import os
from sqlalchemy import create_engine, types

# get here path

dirname = os.path.dirname(__file__)

user = os.getenv("MYSQL_USER") if os.getenv("MYSQL_USER") else "root"
password = os.getenv("MYSQL_PASSWORD") if os.getenv("MYSQL_PASSWORD") else "1234"
host = os.getenv("MYSQL_HOST") if os.getenv("MYSQL_HOST") else "localhost"
port = os.getenv("MYSQL_PORT") if os.getenv("MYSQL_PORT") else "6603"

# Connect to the database
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}", echo=False)

with engine.connect() as connection:
    connection.execute("DROP DATABASE IF EXISTS agmatix;")
    connection.execute("CREATE DATABASE agmatix;")
    connection.execute("USE agmatix;")

    # with open("agriculture/create_db.sql" ,"r") as f:
    #     create_tables = f.read()

    # connection.execute(create_tables)


def load_data_files() -> Tuple[pd.DataFrame]:
    """Load Files function

    Returns:
        Tuple[pd.DataFrame]:
    """
    samples_meta = pd.read_excel(
        os.path.join(dirname,"samples.xlsx"), sheet_name="metadata- treatments"
    )
    samples = pd.read_excel(os.path.join(dirname,"samples.xlsx"), sheet_name="sampels")

    sensors_25 = pd.read_excel(os.path.join(dirname,"sensors.xlsx"), sheet_name="25 cm")
    sensors_50 = pd.read_excel(os.path.join(dirname,"sensors.xlsx"), sheet_name="50 cm")
    sensors_75 = pd.read_excel(os.path.join(dirname,"sensors.xlsx"), sheet_name="75 cm")

    sensors_25["level"] = 25
    sensors_50["level"] = 50
    sensors_75["level"] = 75
    sensors = pd.concat([sensors_25, sensors_50, sensors_75])

    climate = pd.read_excel(os.path.join(dirname,"climate.xlsx"), dtype=str, header=None)

    return climate, samples_meta, samples, sensors


def process_climate_data(climate: pd.DataFrame) -> pd.DataFrame:
    """
    Process Climate data file

    Args:
        climate (pd.DataFrame): raw excel file as df

    Returns:
        pd.DataFrame: processed file
    """

    climate = climate.drop([0, 1, 2])
    climate.columns = [
        "latitude",
        "longitude",
        "et",
        "rain",
        "tmin",
        "tmax",
        "time",
        "date",
    ]

    # types
    climate[["et", "rain", "tmin", "tmax"]] = climate[
        ["et", "rain", "tmin", "tmax"]
    ].astype(float)

    # date
    climate.date = pd.to_datetime(climate.date).dt.date

    # hour
    climate.time = climate.time.str.split(" ").apply(lambda x: x[1])

    # ET fillna values
    climate.et = climate.et.fillna((climate.et.shift() + climate.et.shift(-1)) / 2)

    # rain
    climate.rain.fillna(0, inplace=True)

    # tmin & tmax
    climate.tmin = climate.tmin.fillna(
        (climate.tmin.shift() + climate.tmin.shift(-1)) / 2
    )
    climate.tmax = climate.tmax.fillna(
        (climate.tmax.shift() + climate.tmax.shift(-1)) / 2
    )

    return climate


def process_sensors_data(sensors: pd.DataFrame) -> pd.DataFrame:
    """
    Process sensor data file

    Args:
        sensors (pd.DataFrame): raw excel file as df

    Returns:
        pd.DataFrame: [description]
    """
    # fill long and lat
    sensors.Latitude.fillna(method="ffill", inplace=True)
    sensors.Longitude.fillna(method="ffill", inplace=True)

    # column names
    sensors.rename(
        columns={
            "Latitude": "latitude",
            "Longitude": "longitude",
            "Date.1": "date",
            "Time": "time",
            "Value (Kpa)": "value",
        },
        inplace=True,
    )

    sensors.drop("Date", 1, inplace=True)

    return sensors


def process_samples_metadata(samples_meta: pd.DataFrame) -> pd.DataFrame:
    """
    Process samples metadata file

    Args:
        samples_meta (pd.DataFrame): raw excel file as df

    Returns:
        pd.DataFrame: processed smaples metadata
    """

    samples_meta.dropna(how="all", inplace=True)
    samples_meta.dropna(axis=1, how="all", inplace=True)

    # column names
    samples_meta.rename(
        columns={
            "Research_ID": "research_id",
            "trial_ID": "trial_id",
            "treatment ID": "treatment_id",
            "tratment timing": "treatment_timing",
            "treatment date": "treatment_date",
        },
        inplace=True,
    )

    # date format

    def find_dates(x):
        if isinstance(x, datetime.datetime):
            return [x.date()]

        matches = re.findall("(\d{1,}/\d{1,}/\d{4})", x)
        if matches:
            return [pd.to_datetime(d).date() for d in matches]
        matches = re.findall("(\w{3}[ ]?\,[ ]?\d{1,}[ ]?\,[ ]?\d{4})", x)
        if matches:
            return [pd.to_datetime(d).date() for d in matches]
        return x

    samples_meta.treatment_date = samples_meta.treatment_date.apply(
        lambda x: find_dates(x)
    )

    return samples_meta


def process_samples_data(samples: pd.DataFrame) -> pd.DataFrame:
    """
    Process trial samples data

    Args:
        samples (pd.DataFrame): raw excel file as df

    Returns:
        pd.DataFrame: processed sample data
    """

    # drop nan values
    samples.dropna(how="all", inplace=True)

    # rename columns
    samples.columns = [
        re.sub("\s+", "_", col.strip().lower()).replace("(", "").replace(")", "")
        for col in samples.columns
    ]

    # process dates
    samples["second_n_date_application"] = samples.second_n_date_application.apply(
        lambda x: pd.to_datetime(x).date()
        if isinstance(x, (datetime.datetime, str))
        else x
    )

    # treatment id, join with meta data
    id_to_timing = (
        samples.groupby(["treatment_id", "n_timing"])
        .count()
        .reset_index()[["treatment_id", "n_timing"]]
    )
    id_to_timing = id_to_timing[
        id_to_timing.treatment_id.astype(str).apply(str.isnumeric)
    ]
    meta = pd.merge(samples_meta, id_to_timing, on="treatment_id")

    samples = pd.merge(
        samples,
        meta[["treatment_id", "n_timing"]],
        on="n_timing",
        suffixes=("", "_meta"),
        how="left",
    )
    samples["treatment_id"] = samples.treatment_id_meta.astype(int)
    samples.drop("treatment_id_meta", 1, inplace=True)

    return samples


if __name__ == "__main__":
    climate, samples_meta, samples, sensors = load_data_files()

    climate = process_climate_data(climate)
    sensors = process_sensors_data(sensors)
    samples_meta = process_samples_metadata(samples_meta)
    samples = process_samples_data(samples)

    field_df = climate.iloc[:1, :][["longitude", "latitude"]].reset_index(drop=True)

    field_df.to_sql(
        con=engine,
        name="fields",
        if_exists="append",
        dtype=types.Float(),
        index_label="id",
    )

    climate.drop(["longitude", "latitude"], 1, inplace=True)
    climate["field_id"] = 0

    climate.to_sql(
        con=engine,
        name="climate",
        if_exists="append",
        index=False,
        dtype={
            "field_id": types.Integer(),
            "et": types.Float(),
            "rain": types.Float(),
            "tmin": types.Float(),
            "tmax": types.Float(),
            "date": types.Date(),
        },
    )

    sensors.drop(["longitude", "latitude"], 1, inplace=True)
    sensors["field_id"] = 0

    sensors.to_sql(
        con=engine,
        name="sensors",
        if_exists="append",
        index=False, 
        dtype={
            "field_id": types.Integer(),
            "date": types.Date(),
            "time": types.Time(),
            "value": types.Float(),
            "level": types.Integer(),
        },
    )

    samples_metadata = samples_meta.drop("treatment_date", 1)

    samples_metadata.to_sql(
        con=engine,
        name="samples_metadata",
        if_exists="append",
        index=False,
        dtype={
            "research_id": types.Integer(),
            "trial_id": types.Integer(),
            "treatmeny_id": types.Integer(),
            "treatment_timing": types.VARCHAR(120),
        },
    )

    samples_dates = (
        samples_meta.explode("treatment_date").reset_index(drop=True).drop("treatment_timing", 1)
    )
    samples_dates.to_sql(
        con=engine,
        name="samples_dates",
        if_exists="append",
        index=False,
        dtype={
            "research_id": types.Integer(),
            "trial_id": types.Integer(),
            "treatmeny_id": types.Integer(),
            "treatment_date": types.Date(),
        },
    )

    samples.drop(["longitude", "latitude"], 1, inplace=True)
    samples["field_id"] = 0

    samples.to_sql(
        con=engine,
        name="samples",
        if_exists="append",
        index=False,
        dtype={
            "field_id": types.Integer(),
            "trial": types.Integer(),
            "repetition": types.Integer(),
            "treatment_id": types.Integer(),
            "n_timing": types.VARCHAR(120),
            "grain_yield": types.VARCHAR(120),
            "total_biomass": types.Float(),
            "total_n_content": types.VARCHAR(120),
            "total_ndff": types.Float(),
            "root_ndff": types.Float(),
            "shoot_ndff": types.Float(),
            "grain_ndff": types.Float(),
            "total_soil_ndff": types.Float(),
            "clay": types.Float(),
            "silt": types.Float(),
            "sand": types.Float(),
            "precipitation": types.Float(),
            "oat": types.VARCHAR(120),
            "oat_planting_date": types.Date(),
            "corn_hybrids": types.VARCHAR(120),
            "corn_planting_date": types.DATE(),
            "plant_population": types.Float(),
            "fertilizer_n_rate": types.Float(),
            "product_name": types.VARCHAR(120),
            "n_date_application": types.DATE(),
            "second_n_date_application": types.DATE(),
            "ph": types.Float(),
            "soil_organic_matter_som": types.Float(),
            "p_resin_as_extractor": types.Float(),
            "k": types.Float(),
        },
    )
