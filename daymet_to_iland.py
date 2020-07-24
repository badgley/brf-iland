import os
import sqlite3

import daymetpy
import geohash2 as gh
import luigi
import numpy as np
import pandas as pd


PA2KPA = 0.001


class Config(luigi.Config):
    daymet_dir = luigi.Parameter(default=os.path.expanduser("~/mnt/daymet"))


def get_esat(tair):
    """
    Sonntag 1990 conversion.
    output units kPa
    Cribbed from CRAN `bigleaf` model -- port that thing!
    """

    a = 611.2
    b = 17.62
    c = 243.12
    esat = a * np.exp((b * tair) / (c + tair))
    return esat * PA2KPA


def rad_to_mj(data):
    """
    From Daymet: 'Daily total radiation (MJ/m2/day) can be calculated as follows:
     ((srad (W/m2) * dayl (s/day)) / 1,000,000'
    """
    return (data["srad"] * data["dayl"]) / 1000000


def get_daymet_vpd(data):
    """
    Daymet returns min/max of daily temp AND water vapor pressure.
    Truncate vpd at zero.
    """
    es_min = get_esat(data["tmin"])
    es_max = get_esat(data["tmax"])
    avg_es = np.mean([es_min, es_max])
    vpd = avg_es - (data["vp"] * PA2KPA)
    vpd[vpd < 0] = 0
    return vpd


def daymet_to_iland(data):
    """
    Reformat daymet data from `daymetpy.daymet_timeseries` to conform w iLand driver database
    Format: id, year, month, day, min_temp, max_temp, prec, rad, vpd
    """
    data["month"] = data["ts"].apply(lambda x: pd.Timestamp(x).month)
    data["day"] = data["ts"].apply(lambda x: pd.Timestamp(x).day)
    data["rad"] = rad_to_mj(data)
    data["vpd"] = get_daymet_vpd(data)

    rename_d = {"tmax": "max_temp", "tmin": "min_temp", "prcp": "prec"}
    data = data.rename(columns=rename_d)
    usecols = ["year", "month", "day", "min_temp", "max_temp", "prec", "rad", "vpd"]
    return data[usecols].round(3)


class DaymetTimeseries(luigi.ExternalTask):
    lat = luigi.FloatParameter()
    lon = luigi.FloatParameter()

    def output(self):
        gh6 = gh.encode(latitude=self.lat, longitude=self.lon, precision=6)
        fname = os.path.join(Config().daymet_dir, f"{gh6}.csv")
        return luigi.LocalTarget(fname)

    def run(self):
        ts = daymetpy.daymet_timeseries(
            lat=self.lat, lon=self.lon, start_year=1980, end_year=2019
        )
        int_cols = ["year", "yday"]
        ts[int_cols] = ts[int_cols].astype(int)
        ts.write_csv(self.output().path, index_label="ts", float_format="%.1f")


class BRFDaymetDatabase(luigi.Task):
    def requires(self):
        sites = None  # list of (lat, lon) tuples
        return [DaymetTimeseries(lat=lat, lon=lon) for lat, lon in sites]

    def run(self):
        con = sqlite3.connect(self.output().path)
        daymet_fnames = self.input()

        for daymet_fname in daymet_fnames:
            daymet_data = pd.read_csv(daymet_fname)
            iland_data = daymet_to_sql(daymet_data)
            table_name = os.path.basename(daymet_fname).split(".")[0]  # gh6
            iland_data.to_sql(table_name, con, if_exists="fail")
