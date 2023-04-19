#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

   Copyright 2021 Recurve Analytics, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""
import sys
import csv
import logging
import sqlalchemy
import psycopg

from datetime import datetime
from flexvalue.config import FLEXValueConfig, FLEXValueException
from jinja2 import Environment, PackageLoader, select_autoescape
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google import api_core


SUPPORTED_DBS = ("postgresql", "sqlite", "bigquery")

__all__ = (
    "get_db_connection",
    "get_deer_load_shape",
    "get_filtered_acc_elec",
    "get_filtered_acc_gas",
)

PROJECT_INFO_FIELDS = [
    "project_id",
    "state",
    "utility",
    "region",
    "mwh_savings",
    "therms_savings",
    "elec_load_shape",
    "therms_profile",
    "start_year",
    "start_quarter",
    "units",
    "eul",
    "ntg",
    "discount_rate",
    "admin_cost",
    "measure_cost",
    "incentive_cost",
]
ELEC_AV_COSTS_FIELDS = [
    "utility",
    "region",
    "year",
    "hour_of_year",
    "total",
    "marginal_ghg",
]
GAS_AV_COSTS_FIELDS = [
    "state",
    "utility",
    "region",
    "year",
    "quarter",
    "month",
    "market",
    "t_d",
    "environment",
    "btm_methane",
    "total",
    "upstream_methane",
    "marginal_ghg",
]
ELEC_AVOIDED_COSTS_FIELDS = [
    "state",
    "utility",
    "region",
    "datetime",
    "year",
    "quarter",
    "month",
    "hour_of_day",
    "hour_of_year",
    "energy",
    "losses",
    "ancillary_services",
    "capacity",
    "transmission",
    "distribution",
    "cap_and_trade",
    "ghg_adder",
    "ghg_rebalancing",
    "methane_leakage",
    "total",
    "marginal_ghg",
    "ghg_adder_rebalancing",
]

ELEC_ADDL_FIELDS = ["utility", "region", "month", "quarter", "hour_of_day", "total", "discount", "hour_of_year", "timestamp"]
GAS_ADDL_FIELDS = ["timestamp", "total", "timestamp"]

logging.basicConfig(stream=sys.stderr, format="%(levelname)s:%(message)s", level=logging.DEBUG)

# This is the number of bytes to read when determining whether a csv file has
# a header. 4096 was determined empirically; I don't recommend reading fewer
# bytes than this, since some files can have many columns.
HEADER_READ_SIZE = 4096

# The number of rows to read from csv files when chunking
INSERT_ROW_COUNT = 100000

# Number of rows to insert into BigQuery at once
BIG_QUERY_CHUNK_SIZE = 10000

class DBManager:

    @staticmethod
    def get_db_manager(fv_config: FLEXValueConfig):
        """ Factory for the correct instance of DBManager child class."""
        if not fv_config.database_type or fv_config.database_type not in SUPPORTED_DBS:
            raise FLEXValueException(f"You must specify a database_type in your config file.\nThe valid choices are {SUPPORTED_DBS}")
        if fv_config.database_type == "sqlite":
            return SqliteManager(fv_config)
        elif fv_config.database_type == "postgresql":
            return PostgresqlManager(fv_config)
        elif fv_config.database_type == "bigquery":
            return BigQueryManager(fv_config)
        else:
            raise FLEXValueException(f"Unsupported database_type. Please choose one of {SUPPORTED_DBS}")

    def __init__(self, fv_config: FLEXValueConfig) -> None:
        self.template_env = Environment(
            loader=PackageLoader("flexvalue"), autoescape=select_autoescape()
        )
        self.config = fv_config
        self.engine = self._get_db_engine(fv_config)

    def _get_db_connection_string(self, config: FLEXValueConfig) -> str:
        """Get the sqlalchemy db connection string for the given settings."""
        # Nobody should be calling the method in the base class
        return ""

    def _get_db_engine(
        self, config: FLEXValueConfig
    ) -> Engine:
        conn_str = self._get_db_connection_string(config)
        logging.debug(f'conn_str ={conn_str}')
        engine = create_engine(conn_str)
        logging.debug(f"dialect = {engine.dialect.name}")
        return engine

    def _get_default_db_conn_str(self) -> str:
        """If no db config file is provided, default to a local sqlite database."""
        return "sqlite+pysqlite:///flexvalue.db"

    def process_elec_load_shape(self, elec_load_shapes_path: str, truncate=False):
        """Load the hourly electric load shapes (csv) file. The first 7 columns
        are fixed. Then there are a variable number of columns, one for each
        load shape. This function parses that file to construct a SQL INSERT
        statement with the data, then inserts the data into the elec_load_shape
        table.
        """
        self._prepare_table(
            "elec_load_shape",
            "flexvalue/sql/create_elec_load_shape.sql",
            # index_filepaths=["flexvalue/sql/elec_load_shape_index.sql"],
            truncate=truncate,
        )
        rows = self._csv_file_to_rows(elec_load_shapes_path)
        num_columns = len(rows[0])
        buffer = []
        for col in range(7, num_columns):
            for row in range(1, len(rows)):
                buffer.append(
                    {
                        "state": rows[row][0].upper(),
                        "utility": rows[row][1].upper(),
                        "region": rows[row][2].upper(),
                        "quarter": rows[row][3],
                        "month": rows[row][4],
                        "hour_of_day": rows[row][5],
                        "hour_of_year": rows[row][6],
                        "load_shape_name": rows[0][col].upper(),
                        "value": rows[row][col]
                    }
                )
        insert_text = self._file_to_string(
            "flexvalue/templates/load_elec_load_shape.sql"
        )
        with self.engine.begin() as conn:
            conn.execute(text(insert_text), buffer)

    def process_elec_av_costs(self, elec_av_costs_path: str, truncate=False):
        self._prepare_table(
            "elec_av_costs",
            "flexvalue/sql/create_elec_av_cost.sql",
            #index_filepaths=["flexvalue/sql/elec_av_costs_index.sql"],
            truncate=truncate,
        )
        logging.debug("about to load elec av costs")
        self._load_csv_file(
            elec_av_costs_path,
            "elec_av_costs",
            ELEC_AVOIDED_COSTS_FIELDS,
            "flexvalue/templates/load_elec_av_costs.sql",
            dict_processor=self._eac_dict_mapper,
        )

    def process_therms_profile(self, therms_profiles_path: str, truncate: bool=False):
        """Loads the therms profiles csv file. This file has 5 fixed columns and then
        a variable number of columns after that, each of which represents a therms
        profile. This method parses that file to construct a SQL INSERT statement, then
        inserts the data into the therms_profile table."""
        self._prepare_table(
            "therms_profile",
            "flexvalue/sql/create_therms_profile.sql",
            truncate=truncate,
        )
        rows = self._csv_file_to_rows(therms_profiles_path)
        num_columns = len(rows[0])
        buffer = []
        for col in range(5, num_columns):
            for row in range(1, len(rows)):
                buffer.append(
                    {
                        "state": rows[row][0],
                        "utility": rows[row][1],
                        "region": rows[row][2],
                        "quarter": rows[row][3],
                        "month": rows[row][4],
                        "profile_name": rows[0][col],
                        "value": rows[row][col],
                    }
                )
        insert_text = self._file_to_string(
            "flexvalue/templates/load_therms_profiles.sql"
        )
        with self.engine.begin() as conn:
            conn.execute(text(insert_text), buffer)

    def process_gas_av_costs(self, gas_av_costs_path: str, truncate=False):
        self._prepare_table(
            "gas_av_costs", "flexvalue/sql/create_gas_av_cost.sql", truncate=truncate
        )
        self._load_csv_file(
            gas_av_costs_path,
            "gas_av_costs",
            GAS_AV_COSTS_FIELDS,
            "flexvalue/templates/load_gas_av_costs.sql",
        )

    def _eac_dict_mapper(self, dict_to_process):
        dict_to_process["date_str"] = dict_to_process["datetime"][
            :10
        ]  # just the 'yyyy-mm-dd'
        return dict_to_process

    def _file_to_string(self, filename):
        ret = None
        with open(filename) as f:
            ret = f.read()
        return ret

    def reset_elec_load_shape(self):
        logging.debug("Resetting elec load shape")
        self._reset_table("elec_load_shape")

    def reset_elec_av_costs(self):
        logging.debug("Resetting elec_av_costs")
        self._reset_table("elec_av_costs")

    def reset_therms_profiles(self):
        logging.debug("Resetting therms_profile")
        self._reset_table("therms_profile")

    def reset_gas_av_costs(self):
        logging.debug("Resetting gas avoided costs")
        self._reset_table("gas_av_costs")

    def _reset_table(self, table_name):
        truncate_prefix = self._get_truncate_prefix()
        sql = f"{truncate_prefix} {table_name}"
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(sql))
        except sqlalchemy.exc.ProgrammingError:
            # in case this is called before the table is created
            pass

    def _get_truncate_prefix(self):
        raise FLEXValueException("You need to implement _get_truncate_prefix for your database manager.")

    def _prepare_table(
        self,
        table_name: str,
        sql_filepath: str,
        index_filepaths=[],
        truncate: bool = False,
    ):
        # if the table doesn't exist, create it and all related indexes
        with self.engine.begin() as conn:
            if not self._table_exists(table_name):
                sql = self._file_to_string(sql_filepath)
                _ = conn.execute(text(sql))
            for index_filepath in index_filepaths:
                sql = self._file_to_string(index_filepath)
                _ = conn.execute(text(sql))
        if truncate:
            self._reset_table(table_name)

    def _table_exists(self, table_name):
        inspection = inspect(self.engine)
        table_exists = inspection.has_table(table_name)
        return table_exists

    def run(self):
        logging.debug(f"About to start calculation, it is {datetime.now()}")
        self._perform_calculation()
        logging.debug(f"after calc, it is {datetime.now()}")

    def process_project_info(self, project_info_path: str):
        self._prepare_table(
            "project_info",
            "flexvalue/sql/create_project_info.sql",
            index_filepaths=[
                "flexvalue/sql/project_info_index.sql",
                "flexvalue/sql/project_info_dates_index.sql",
            ],
            truncate=True,
        )
        dicts = self._csv_file_to_dicts(
            project_info_path,
            fieldnames=PROJECT_INFO_FIELDS,
            fields_to_upper=["elec_load_shape", "state", "region", "utility"],
        )
        for d in dicts:
            start_year = int(d["start_year"])
            eul = int(d["eul"])
            quarter = d["start_quarter"]
            month = self._quarter_to_month(quarter)
            d["start_date"] = f"{start_year}-{month}-01"
            d["end_date"] = f"{start_year + eul}-{month}-01"

        insert_text = self._file_to_string("flexvalue/templates/load_project_info.sql")
        self._load_project_info_data(insert_text, dicts)

    def _load_project_info_data(self, insert_text, project_info_dicts):
        with self.engine.begin() as conn:
            conn.execute(text(insert_text), project_info_dicts)

    def _quarter_to_month(self, qtr):
        quarter = int(qtr)
        return "{:02d}".format(((quarter - 1) * 3) + 1)

    def _get_empty_tables(self):
        empty_tables = []
        inspection = inspect(self.engine)
        with self.engine.begin() as conn:
            for table_name in [
                "therms_profile",
                "project_info",
                "elec_av_costs",
                "gas_av_costs",
                "elec_load_shape",
            ]:
                if not inspection.has_table(table_name):
                    empty_tables.append(table_name)
                    continue
                sql = f"SELECT COUNT(*) FROM {table_name}"
                result = conn.execute(text(sql))
                first = result.first()
                if first[0] == 0:
                    empty_tables.append(table_name)
        return empty_tables

    # TODO: allow better configuration of gas vs electric table names
    def _perform_calculation(self):
        empty_tables = self._get_empty_tables()
        if empty_tables:
            raise FLEXValueException(
                f"Not all data has been loaded. Please provide data for the following tables: {', '.join(empty_tables)}"
            )
        if self.config.separate_output_tables:
            sql = self._get_calculation_sql(mode="electric")
            logging.info(f'electric sql =\n{sql}')
            self._run_calc(sql)
            sql = self._get_calculation_sql(mode="gas")
            logging.info(f'gas sql =\n{sql}')
            self._run_calc(sql)
        else:
            sql = self._get_calculation_sql()
            logging.info(f'sql =\n{sql}')
            self._run_calc(sql)

    def _run_calc(self, sql):
        with self.engine.begin() as conn:
            result = conn.execute(text(sql))
            if not self.config.output_table:
                print(", ".join(result.keys()))
                for row in result:
                    print(", ".join([str(col) for col in row]))

    def _get_calculation_sql(self, mode="both"):
        if mode == "both":
            context = self._get_calculation_sql_context()
            template = self.template_env.get_template("calculation.sql")
        elif mode == "electric":
            context = self._get_calculation_sql_context(mode=mode)
            template = self.template_env.get_template("elec_calculation.sql")
        elif mode == "gas":
            context = self._get_calculation_sql_context(mode=mode)
            template = self.template_env.get_template("gas_calculation.sql")
        sql = template.render(context)
        return sql

    def _get_calculation_sql_context(self, mode=""):
        context = {
            "project_info_table": "project_info",
            "eac_table": "elec_av_costs",
            "els_table": "elec_load_shape",
            "gac_table": "gas_av_costs",
            "therms_profile_table": "therms_profile",
            "float_type": self.config.float_type(),
            "database_type": self.config.database_type,
            "elec_aggregation_columns": self._elec_aggregation_columns(),
            "gas_aggregation_columns": self._gas_aggregation_columns(),
            "show_elec_components": self.config.show_elec_components,
            "show_gas_components": self.config.show_gas_components,
        }
        if self.config.include_addl_fields:
            context['elec_addl_fields'] = self._elec_addl_fields("pcwdea")
            context['elec_output_addl_fields'] = self._elec_addl_fields("elec_calculations")
            context['gas_addl_fields'] = self._gas_addl_fields("pcwdga")
            context['gas_output_addl_fields'] = self._gas_addl_fields("gas_calculations")
        if self.config.output_table:
            table_name = self.config.output_table
            if mode:
                table_name = mode + "_" + table_name
            context['create_clause'] = f"DROP TABLE IF EXISTS {table_name};CREATE TABLE {table_name} AS ("

        return context

    def _elec_aggregation_columns(self):
        prefix_map = {
            "hour_of_year": "pcwdea",
            "year": "pcwdea",
            "eul": "pcwdea",
            "utility": "pcwdea",
            "region": "pcwdea",
            "month": "pcwdea",
            "quarter": "pcwdea",
            "discount": "pcwdea",
            "hour_of_day": "pcwdea",
            "timestamp": "pcwdea",
            "load_shape_name": "elec_load_shape"
        }
        columns = []
        aggregation_columns = set(self.config.aggregation_columns) - set(ELEC_ADDL_FIELDS)
        for col in aggregation_columns:
            try:
                columns.append(f"{prefix_map[col]}.{col}")
            except KeyError:
                pass
        return ", ".join(columns)

    def _gas_aggregation_columns(self):
        prefix_map = {
            "year": "pcwdga",
            "eul": "pcwdga",
            "utility": "pcwdga",
            "region": "pcwdga",
            "month": "pcwdga",
            "quarter": "pcwdga",
            "discount": "pcwdga",
            "timestamp": "pcwdga",
            "profile_name": "therms_profile"
        }
        columns = []
        aggregation_columns = set(self.config.aggregation_columns) - set(GAS_ADDL_FIELDS)
        for col in aggregation_columns:
            try:
                columns.append(f"{prefix_map[col]}.{col}")
            except KeyError:
                pass
        return ", ".join(columns)

    def _elec_addl_fields(self, prefix):
        fields = ELEC_ADDL_FIELDS
        columns = []
        for col in fields:
            columns.append(f"{prefix}.{col}")
        return ", ".join(columns)

    def _gas_addl_fields(self, prefix):
        fields = GAS_ADDL_FIELDS
        columns = []
        for col in fields:
            columns.append(f"{prefix}.{col}")
        return ", ".join(columns)

    def _csv_file_to_dicts(
        self, csv_file_path: str, fieldnames: str, fields_to_upper=None
    ):
        """Returns a dictionary representing the data in the csv file pointed
        to at csv_file_path.
        fields_to_upper is a list of strings. The strings in this list must
        be present in the header row of the csv file being read, and are
        capitalized (with string.upper()) before returning the dict."""
        dicts = []
        with open(csv_file_path, newline="") as f:
            has_header = csv.Sniffer().has_header(f.read(HEADER_READ_SIZE))
            f.seek(0)
            csv_reader = csv.DictReader(f, fieldnames=fieldnames)
            if has_header:
                next(csv_reader)
            for row in csv_reader:
                processed = row
                for field in fields_to_upper:
                    processed[field] = processed[field].upper()
                dicts.append(processed)
        return dicts

    def _csv_file_to_rows(self, csv_file_path: str):
        """Reads a csv file into memory and returns a list of tuples representing
        the data. If no header row is present, it raises a FLEXValueException."""
        rows = []
        with open(csv_file_path, newline="") as f:
            has_header = csv.Sniffer().has_header(f.read(HEADER_READ_SIZE))
            if not has_header:
                raise FLEXValueException(
                    f"The file you provided, {csv_file_path}, \
                                 doesn't seem to have a header row. Please provide a header row \
                                 containing the column names."
                )
            f.seek(0)
            csv_reader = csv.reader(f)
            rows = []
            # Note that we're reading the whole file into memory - don't use this on big files.
            for row in csv_reader:
                rows.append(row)
        return rows

    def _load_csv_file(
        self,
        csv_file_path: str,
        table_name: str,
        fieldnames,
        load_sql_file_path: str,
        dict_processor=None,
    ):
        """Loads the table_name table, Since some of the input data can be over a gibibyte,
        the load reads in chunks of data and inserts them sequentially. The chunk size is
        determined by INSERT_ROW_COUNT in this file.
        fieldnames is the list of expected values in the header row of the csv file being read.
        dict_processor is a function that takes a single dictionary and returns a single dictionary
        """
        with open(csv_file_path, newline="") as f:
            has_header = csv.Sniffer().has_header(f.read(HEADER_READ_SIZE))
            f.seek(0)
            csv_reader = csv.DictReader(f, fieldnames=fieldnames)
            if has_header:
                next(csv_reader)
            buffer = []
            rownum = 0
            insert_text = self._file_to_string(load_sql_file_path)
            with self.engine.begin() as conn:
                for row in csv_reader:
                    buffer.append(dict_processor(row) if dict_processor else row)
                    rownum += 1
                    if rownum == INSERT_ROW_COUNT:
                        conn.execute(text(insert_text), buffer)
                        buffer = []
                        rownum = 0
                else:  # this is for/else
                    conn.execute(text(insert_text), buffer)


    def _exec_select_sql(self, sql: str):
        """Returns a list of tuples that have been copied from the sqlalchemy result."""
        # This is just here to support testing
        ret = None
        with self.engine.begin() as conn:
            result = conn.execute(text(sql))
            ret = [x for x in result]
        return ret

class PostgresqlManager(DBManager):
    def __init__(self, fv_config: FLEXValueConfig) -> None:
        super().__init__(fv_config)
        self.connection = psycopg.connect(
            dbname=self.config.database,
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password
        )
        logging.debug(f"connection = {self.connection}")

    def _get_db_connection_string(self, config: FLEXValueConfig) -> str:
            user = config.user
            password = config.password
            host = config.host
            port = config.port
            database = config.database
            conn_str = (
                f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
            )
            return conn_str

    def _get_truncate_prefix(self):
        return "TRUNCATE TABLE"

    def process_gas_av_costs(self, gas_av_costs_path: str, truncate=False):
        def copy_write(cur, rows):
            with cur.copy("""COPY gas_av_costs (
                    state,
                    utility,
                    region,
                    year,
                    quarter,
                    month,
                    timestamp,
                    market,
                    t_d,
                    environment,
                    btm_methane,
                    total,
                    upstream_methane,
                    marginal_ghg)
                    FROM STDIN"""
            ) as copy:
                for row in rows:
                    copy.write_row(row)
        self._prepare_table(
            "gas_av_costs",
            "flexvalue/sql/create_gas_av_cost.sql"
        )
        MAX_ROWS = sys.maxsize
        logging.info("IN PG VERSION OF LOAD GAS AV COSTS")
        try:
            cur = self.connection.cursor()
            buf = []
            with open(gas_av_costs_path) as f:
                reader = csv.DictReader(f)
                for i, r in enumerate(reader):
                    dt = datetime(year=int(r["year"]), month=int(r["month"]), day=1, hour=0, minute=0, second=0)
                    gac_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S %Z")
                    buf.append(
                        [
                            r["state"],
                            r["utility"],
                            r["region"],
                            int(r["year"]),
                            int(r["quarter"]),
                            int(r["month"]),
                            gac_timestamp,
                            float(r["market"]),
                            float(r["t_d"]),
                            float(r["environment"]),
                            float(r["btm_methane"]),
                            float(r["total"]),
                            float(r["upstream_methane"]),
                            float(r["marginal_ghg"])
                        ]
                    )
                    if len(buf) == MAX_ROWS:
                        copy_write(cur, buf)
                        buf = []
                else:
                    copy_write(cur, buf)
            self.connection.commit()
        except Exception as e:
            logging.error(f"Error loading the gas avoided costs: {e}")

    def process_elec_av_costs(self, elec_av_costs_path):
        def copy_write(cur, rows):
            with cur.copy(
                """COPY elec_av_costs (
                    state,
                    utility,
                    region,
                    timestamp,
                    year,
                    quarter,
                    month,
                    hour_of_day,
                    hour_of_year,
                    energy,
                    losses,
                    ancillary_services,
                    capacity,
                    transmission,
                    distribution,
                    cap_and_trade,
                    ghg_adder,
                    ghg_rebalancing,
                    methane_leakage,
                    total,
                    marginal_ghg,
                    ghg_adder_rebalancing)
                    FROM STDIN"""
            ) as copy:
                for row in rows:
                    copy.write_row(row)

        self._prepare_table(
            "elec_av_costs",
            "flexvalue/sql/create_elec_av_cost.sql",
            # index_filepaths=["flexvalue/sql/elec_av_costs_index.sql"]
        )

        logging.debug("in pg version of load_elec_av_costs")
        # if you're concerned about RAM change this to sane number
        MAX_ROWS = sys.maxsize

        try:
            cur = self.connection.cursor()
            buf = []
            with open(elec_av_costs_path) as f:
                reader = csv.DictReader(f)
                for i, r in enumerate(reader):
                    eac_timestamp = datetime.strptime(
                        r["datetime"], "%Y-%m-%d %H:%M:%S %Z"
                    )
                    buf.append(
                        [
                            r["state"],
                            r["utility"],
                            r["region"],
                            eac_timestamp,
                            r["year"],
                            r["quarter"],
                            r["month"],
                            r["hour_of_day"],
                            r["hour_of_year"],
                            r["energy"],
                            r["losses"],
                            r["ancillary_services"],
                            r["capacity"],
                            r["transmission"],
                            r["distribution"],
                            r["cap_and_trade"],
                            r["ghg_adder"],
                            r["ghg_rebalancing"],
                            r["methane_leakage"],
                            float(r["total"]),
                            r["marginal_ghg"],
                            r["ghg_adder_rebalancing"],
                        ]
                    )
                    if len(buf) == MAX_ROWS:
                        copy_write(cur, buf)
                        buf = []
                else:
                    copy_write(cur, buf)
            self.connection.commit()
        except Exception as e:
            logging.error(f"Error loading the electric avoided costs: {e}")

    def process_elec_load_shape(self, elec_load_shapes_path: str):
        def copy_write(cur, rows):
            with cur.copy(
                "COPY elec_load_shape (state, utility, region, quarter, month, hour_of_day, hour_of_year, load_shape_name, value) FROM STDIN"
            ) as copy:
                for row in rows:
                    copy.write_row(row)

        self._prepare_table(
            "elec_load_shape",
            "flexvalue/sql/create_elec_load_shape.sql",
            # index_filepaths=["flexvalue/sql/elec_load_shape_index.sql"]
        )
        cur = self.connection.cursor()
        # if you're concerned about RAM change this to sane number
        MAX_ROWS = sys.maxsize

        buf = []
        with open(elec_load_shapes_path) as f:
            # this probably escapes fine but a csv reader is a safer bet
            columns = f.readline().split(",")
            load_shape_names = [
                c.strip()
                for c in columns
                if columns.index(c) > columns.index("hour_of_year")
            ]

            f.seek(0)
            reader = csv.DictReader(f)
            for r in reader:
                for load_shape in load_shape_names:
                    buf.append(
                        (
                            r["state"].upper(),
                            r["utility"].upper(),
                            r["region"].upper(),
                            int(r["quarter"]),
                            int(r["month"]),
                            int(r["hour_of_day"]),
                            int(r["hour_of_year"]),
                            load_shape.upper(),
                            float(r[load_shape]),
                        )
                    )
                if len(buf) >= MAX_ROWS:
                    copy_write(cur, buf)
                    buf = []
            else:
                copy_write(cur, buf)
        self.connection.commit()
        self.connection.close()

    def _load_project_info_data(self, insert_text, project_info_dicts):
        """ insert_text isn't needed for postgresql """
        def copy_write(cur, rows):
            with cur.copy(
                "COPY project_info (project_id, state, utility, region, mwh_savings, therms_savings, elec_load_shape, therms_profile, start_year, start_quarter, start_date, end_date, units, eul, ntg, discount_rate, admin_cost, measure_cost, incentive_cost ) FROM STDIN"
            ) as copy:
                for row in rows:
                    copy.write_row(row)
        rows = [
            (x["project_id"], x["state"], x["utility"], x["region"], x["mwh_savings"], x["therms_savings"], x["elec_load_shape"], x["therms_profile"], x["start_year"], x["start_quarter"], x["start_date"], x["end_date"], x["units"], x["eul"], x["ntg"], x["discount_rate"], x["admin_cost"], x["measure_cost"], x["incentive_cost"])
            for x in project_info_dicts
        ]
        cursor = self.connection.cursor()
        copy_write(cursor, rows)
        self.connection.commit()


class SqliteManager(DBManager):
    def __init__(self, fv_config: FLEXValueConfig):
        super().__init__(fv_config)
        self.template_env = Environment(
            loader=PackageLoader("flexvalue"), autoescape=select_autoescape()
        )
        self.config = fv_config

    def _get_truncate_prefix(self):
        """ sqlite doesn't support TRUNCATE"""
        return "DELETE FROM"

    def _get_db_connection_string(self, config: FLEXValueConfig) -> str:
        database = config.database
        conn_str = f"sqlite+pysqlite://{database}"
        return conn_str


class BigQueryManager(DBManager):
    def __init__(self, fv_config: FLEXValueConfig):
        super().__init__(fv_config)
        self.template_env = Environment(
            loader=PackageLoader("flexvalue"), autoescape=select_autoescape()
        )
        self.config = fv_config
        self.table_names = [
            f"{self.config.dataset}.{x}" for x in
                [self.config.elec_av_costs_table,
                self.config.elec_load_shape_table,
                self.config.therms_profiles_table,
                self.config.gas_av_costs_table,
                self.config.project_info_table
            ]
        ]
        self.client = bigquery.Client(project=self.config.project)
        # self._test_connection()

    def _test_connection(self):
        logging.debug('in bigquerymanager._test_connection')
        query = """select count(*) from flexvalue_refactor_tables.example_user_inputs"""
        query_job = self.client.query(query)
        rows = query_job.result()
        for row in rows:
            print(f"There are {row.values()[0]} rows in example_user_inputs")

    def _get_truncate_prefix(self):
        # in BQ, TRUNCATE TABLE deletes row-level security, so using DELETE instead:
        return "DELETE"

    def _get_db_engine(self, config: FLEXValueConfig) -> Engine:
        # Not using sqlalchemy in BigQuery; TODO refactor so this isn't necessary
        return None

    def _table_exists(self, table_name):
        # This is basically straight from the google docs:
        # https://cloud.google.com/bigquery/docs/samples/bigquery-table-exists#bigquery_table_exists-python
        try:
            self.client.get_table(table_name)
            return True
        except NotFound:
            return False

    def _get_empty_tables(self):
        empty_tables = []
        for table_name in self.table_names:
            if not self._table_exists(table_name):
                empty_tables.append(table_name)
                continue
            sql = f"SELECT COUNT(*) FROM {table_name}"
            query_job = self.client.query(sql)  # API request
            result = query_job.result()
            for row in result: # there will be only one, but we have to iterate
                if row.get("count") == 0:
                    empty_tables.append(table_name)
        return empty_tables

    def _prepare_table(self, table_name: str, sql_filepath: str, index_filepaths=[], truncate: bool = False):
        if not self._table_exists(f"{self.config.dataset}.{table_name}"):
            template = self.template_env.get_template(sql_filepath)
            sql = template.render({"dataset": self.config.dataset})
            logging.debug(f"create sql = \n{sql}")
            query_job = self.client.query(sql)
            result = query_job.result()
        else:
            if truncate:
                sql = f"DELETE FROM {self.config.dataset}.{table_name} WHERE TRUE;"
                query_job = self.client.query(sql)
                result = query_job.result()

    def process_elec_av_costs(self, elec_av_costs_path: str, truncate=False):
        # We don't need to do anything with this in BQ, just use the table provided
        pass

    def process_gas_av_costs(self, gas_av_costs_path: str, truncate=False):
        """ Add a timestamp column if none exists, and populate it. It
        will be used to join on in later calculations.
        """
        logging.debug("In bq process_gas_av_costs")
        table_name = f"{self.config.dataset}.{self.config.gas_av_costs_table}"
        self._ensure_timestamp_column(table_name)
        sql = f'UPDATE {table_name} gac SET timestamp = (TIMESTAMP(FORMAT("%d-%d-01 00:00:00", gac.year, gac.month))) WHERE TRUE;'
        query_job = self.client.query(sql)
        result = query_job.result()

    def _ensure_timestamp_column(self, table_name):
        """ Ensure that the table with name `table_name` has a column
        named `timestamp`, of type `TIMESTAMP`.
        """
        table = self.client.get_table(table_name)
        has_timestamp = False
        for column in table.schema:
            if column.name == 'timestamp' and column.field_type == "TIMESTAMP":
                has_timestamp = True
                break
        if not has_timestamp:
            original_schema = table.schema
            new_schema = original_schema[:]  # Creates a copy of the schema.
            new_schema.append(bigquery.SchemaField("timestamp", "TIMESTAMP"))

            table.schema = new_schema
            table = self.client.update_table(table, ["schema"])  # Make an API request.

            if len(table.schema) == len(original_schema) + 1 == len(new_schema):
                print("A new column has been added.")
            else:
                raise FLEXValueException(f"Unable to add a timestamp column to {table_name}; can't process gas avoided costs.")

    def process_elec_load_shape(self, elec_load_shapes_path: str, truncate=False):
        self._prepare_table(
            "elec_load_shape",
            "bq_create_elec_load_shape.sql",
            truncate=True
        )
        template = self.template_env.get_template("bq_populate_elec_load_shape.sql")
        sql = template.render({
            "project": self.config.project,
            "dataset": self.config.dataset,
            "elec_load_shape_table": self.config.elec_load_shape_table
        })
        query_job = self.client.query(sql)
        result = query_job.result()

    def process_therms_profile(self, therms_profiles_path: str, truncate: bool=False):
        logging.debug("In bq version of process therms")
        self._prepare_table(
            "therms_profile",
            "flexvalue/sql/create_therms_profile.sql",
            truncate=truncate,
        )
        template = self.template_env.get_template("bq_populate_therms_profile.sql")
        sql = template.render({
            "project": self.config.project,
            "dataset": self.config.dataset,
            "therms_profiles_table": self.config.therms_profiles_table
        })
        query_job = self.client.query(sql)
        result = query_job.result()

    def _get_calculation_sql_context(self, mode=""):
        context = {
            "project_info_table": f"`{self.config.dataset}.{self.config.project_info_table}`",
            "eac_table": f"`{self.config.dataset}.{self.config.elec_av_costs_table}`",
            "els_table": f"`{self.config.dataset}.elec_load_shape`",
            "gac_table": f"`{self.config.dataset}.{self.config.gas_av_costs_table}`",
            "therms_profile_table": f"`{self.config.dataset}.therms_profile`",
            "float_type": self.config.float_type(),
            "database_type": self.config.database_type,
            "elec_aggregation_columns": self._elec_aggregation_columns(),
            "gas_aggregation_columns": self._gas_aggregation_columns(),
            "show_elec_components": self.config.show_elec_components,
            "show_gas_components": self.config.show_gas_components,
            "include_addl_fields": self.config.include_addl_fields
        }
        if self.config.output_table:
            table_name = f"{self.config.dataset}.{mode}_{self.config.output_table}" if mode else f"{self.config.dataset}.{self.config.output_table}"
            context["create_clause"] = f"CREATE OR REPLACE TABLE {table_name} AS ("
        return context

    def _run_calc(self, sql):
        query_job = self.client.query(sql)
        result = query_job.result()
        for row in result:
            print(",".join([f"{x}" for x in row.values()]))

    def _get_original_elec_load_shape(self):
        """ Generator to fetch existing electric load shape data from BigQuery. """
        template = self.template_env.get_template("get_elec_load_shape.sql")
        sql = template.render({
            'dataset': self.config.dataset,
            'elec_load_shape_table': self.config.elec_load_shape_table
        })
        query_job = self.client.query(sql)
        result = query_job.result()
        for row in result:
            yield row.values()

    def _get_project_info_data(self):
        template = self.template_env.get_template("get_project_info.sql")
        sql = template.render({
            'dataset': self.config.dataset,
            'project_info_table': self.config.project_info_table
        })
        logging.debug(f"project_info sql = {sql}")
        query_job = self.client.query(sql)
        result = query_job.result()
        project_info_data = []
        for row in result:
            start_year = row.start_year
            eul = row.eul
            month = self._quarter_to_month(row.start_quarter)
            project_info_data.append({
                "project_id":row.project_id,
                "start_year": start_year,
                "start_quarter": row.start_quarter,
                "month": month,
                "eul": eul,
                "discount_rate": row.discount_rate,
                "start_date": f"{start_year}-{month}-01",
                "end_date": f"{start_year + eul}-{month}-01"
            })
        return project_info_data

    def process_project_info(self, project_info_path: str):
        project_info = self._get_project_info_data()

    def reset_elec_av_costs(self):
        # The elec avoided costs table doesn't get changed; the super()'s
        # reset_elec_av_costs will truncate this table, so add a no-op here.
        pass

    def reset_gas_av_costs(self):
        # FLEXvalue adds and populates the `timestamp` column, so remove it:
        sql = f"ALTER TABLE {self.config.dataset}.{self.config.gas_av_costs_table} DROP COLUMN timestamp;"
        query_job = self.client.query(sql)
        try:
            result = query_job.result()
        except api_core.exceptions.BadRequest as e:
            # We are resetting before timestamp was added, ignore exception
            pass

    def _reset_table(self, table_name):
        truncate_prefix = self._get_truncate_prefix()
        sql = f"{truncate_prefix} {self.config.dataset}.{table_name} WHERE TRUE;"
        query_job = self.client.query(sql)
        result = query_job.result()