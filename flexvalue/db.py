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
from collections import defaultdict
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
from sqlalchemy.exc import ResourceClosedError
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
    "id",
    "state",
    "utility",
    "region",
    "mwh_savings",
    "therms_savings",
    "load_shape",
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
    "value_curve_name",
]
ELEC_AV_COSTS_FIELDS = [
    "utility",
    "region",
    "year",
    "hour_of_year",
    "total",
    "marginal_ghg",
    "value_curve_name",
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
    "value_curve_name",
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
    "value_curve_name",
]

logging.basicConfig(
    stream=sys.stderr, format="%(levelname)s:%(message)s", level=logging.INFO
)

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
    def get_db_manager(
        fv_config: FLEXValueConfig, bigquery_client: bigquery.Client = None
    ):
        """Factory for the correct instance of DBManager child class."""
        if not fv_config.database_type or fv_config.database_type not in SUPPORTED_DBS:
            raise FLEXValueException(
                f"You must specify a database_type in your config file.\nThe valid choices are {SUPPORTED_DBS}"
            )
        if fv_config.database_type == "sqlite":
            return SqliteManager(fv_config)
        elif fv_config.database_type == "postgresql":
            return PostgresqlManager(fv_config)
        elif fv_config.database_type == "bigquery":
            return BigQueryManager(fv_config, bigquery_client)
        else:
            raise FLEXValueException(
                f"Unsupported database_type. Please choose one of {SUPPORTED_DBS}"
            )

    def __init__(self, fv_config: FLEXValueConfig) -> None:
        self.template_env = Environment(
            loader=PackageLoader("flexvalue", "templates"),
            autoescape=select_autoescape(),
            trim_blocks=True,
        )
        self.config = fv_config
        self.engine = self._get_db_engine(fv_config)

    def _get_db_connection_string(self, config: FLEXValueConfig) -> str:
        """Get the sqlalchemy db connection string for the given settings."""
        # Nobody should be calling the method in the base class
        return ""

    def _get_db_engine(self, config: FLEXValueConfig) -> Engine:
        conn_str = self._get_db_connection_string(config)
        logging.debug(f"conn_str ={conn_str}")
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
                        "value": rows[row][col],
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
            # index_filepaths=["flexvalue/sql/elec_av_costs_index.sql"],
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

    def process_therms_profile(self, therms_profiles_path: str, truncate: bool = False):
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
        raise FLEXValueException(
            "You need to implement _get_truncate_prefix for your database manager."
        )

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

    def _prepare_table_from_str(
        self,
        table_name: str,
        create_table_sql: str,
        index_filepaths=[],
        truncate: bool = False,
    ):
        # if the table doesn't exist, create it and all related indexes
        with self.engine.begin() as conn:
            if not self._table_exists(table_name):
                _ = conn.execute(text(create_table_sql))
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
            fields_to_upper=["load_shape", "state", "region", "utility"],
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
            logging.info(f"electric sql =\n{sql}")
            self._run_calc(sql)
            sql = self._get_calculation_sql(mode="gas")
            logging.info(f"gas sql =\n{sql}")
            self._run_calc(sql)
        else:
            sql = self._get_calculation_sql()
            logging.info(f"sql =\n{sql}")
            self._run_calc(sql)

    def _run_calc(self, sql):
        with self.engine.begin() as conn:
            result = conn.execute(text(sql))
            if (
                not self.config.output_table
                and not self.config.electric_output_table
                and not self.config.gas_output_table
            ):
                if self.config.output_file:
                    with open(self.config.output_file, "w") as outfile:
                        outfile.write(", ".join(result.keys()) + "\n")
                        for row in result:
                            outfile.write(", ".join([str(col) for col in row]) + "\n")
                else:
                    try:
                        print(", ".join(result.keys()))
                        for row in result:
                            print(", ".join([str(col) for col in row]))
                    except ResourceClosedError:
                        # If the query doesn't return rows (e.g. we are writing to
                        # an output table), don't error out.
                        pass

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
        elec_agg_columns = self._elec_aggregation_columns()
        gas_agg_columns = self._gas_aggregation_columns()
        elec_addl_fields = self._elec_addl_fields(elec_agg_columns)
        gas_addl_fields = self._gas_addl_fields(gas_agg_columns)
        context = {
            "project_info_table": "project_info",
            "eac_table": "elec_av_costs",
            "els_table": "elec_load_shape",
            "gac_table": "gas_av_costs",
            "therms_profile_table": "therms_profile",
            "float_type": self.config.float_type(),
            "database_type": self.config.database_type,
            "elec_components": self._elec_components(),
            "gas_components": self._gas_components(),
            "use_value_curve_name_for_join": self.config.use_value_curve_name_for_join,
        }
        if mode == "electric":
            context["elec_aggregation_columns"] = elec_agg_columns
            context["elec_addl_fields"] = elec_addl_fields
        elif mode == "gas":
            context["gas_aggregation_columns"] = gas_agg_columns
            context["gas_addl_fields"] = gas_addl_fields
        else:
            context["elec_aggregation_columns"] = elec_agg_columns
            context["gas_aggregation_columns"] = gas_agg_columns
            context["elec_addl_fields"] = elec_addl_fields
            context["gas_addl_fields"] = set(gas_addl_fields) - set(elec_addl_fields)

        if (
            self.config.output_table
            or self.config.electric_output_table
            or self.config.gas_output_table
        ):
            table_name = self.config.output_table
            if mode == "electric":
                table_name = self.config.electric_output_table
            elif mode == "gas":
                table_name = self.config.gas_output_table
            context[
                "create_clause"
            ] = f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} AS ("

        return context

    def _elec_aggregation_columns(self):
        ELECTRIC_AGG_COLUMNS = set(
            [
                "hour_of_year",
                "year",
                "region",
                "month",
                "quarter",
                "hour_of_day",
                "datetime",
            ]
        )
        aggregation_columns = (
            set(self.config.aggregation_columns) & ELECTRIC_AGG_COLUMNS
        )
        return aggregation_columns

    def _gas_aggregation_columns(self):
        GAS_AGG_COLUMNS = set(
            [
                "region",
                "year",
                "month",
                "quarter",
                "datetime",
            ]
        )
        aggregation_columns = set(self.config.aggregation_columns) & GAS_AGG_COLUMNS
        return aggregation_columns

    def _elec_addl_fields(self, elec_agg_columns):
        fields = set(self.config.elec_addl_fields) - set(elec_agg_columns)
        logging.debug(
            f"elec_addl_fields = {self.config.elec_addl_fields}\nelec_agg_columns = {elec_agg_columns}\nset diff = {fields}"
        )
        return fields

    def _gas_addl_fields(self, gas_agg_columns):
        fields = (
            set(self.config.gas_addl_fields) - set(gas_agg_columns) - set(["total"])
        )
        logging.debug(
            f"gas_addl_fields = {self.config.gas_addl_fields}\ngas_agg_columns = {gas_agg_columns}\nset diff = {fields}"
        )
        return fields

    def _elec_components(self):
        fields = set(self.config.elec_components) - set(["total"])
        logging.debug(
            f"elec_components = {self.config.elec_components}\n diff = {fields}"
        )
        return fields

    def _gas_components(self):
        fields = set(self.config.gas_components) - set(["total"])
        logging.debug(
            f"gas_components = {self.config.gas_components}\n diff = {fields}"
        )
        return fields

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
            password=self.config.password,
        )
        logging.debug(f"connection = {self.connection}")

    def _get_db_connection_string(self, config: FLEXValueConfig) -> str:
        user = config.user
        password = config.password
        host = config.host
        port = config.port
        database = config.database
        conn_str = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
        return conn_str

    def _get_truncate_prefix(self):
        return "TRUNCATE TABLE"

    def process_gas_av_costs(self, gas_av_costs_path: str, truncate=False):
        def copy_write(cur, rows):
            with cur.copy(
                """COPY gas_av_costs (
                    state,
                    utility,
                    region,
                    year,
                    quarter,
                    month,
                    datetime,
                    market,
                    t_d,
                    environment,
                    btm_methane,
                    total,
                    upstream_methane,
                    marginal_ghg,
                    value_curve_name)
                    FROM STDIN"""
            ) as copy:
                for row in rows:
                    copy.write_row(row)

        self._prepare_table("gas_av_costs", "flexvalue/sql/create_gas_av_cost.sql")
        MAX_ROWS = 10000
        logging.info("IN PG VERSION OF LOAD GAS AV COSTS")
        try:
            cur = self.connection.cursor()
            buf = []
            with open(gas_av_costs_path) as f:
                reader = csv.DictReader(f)
                for i, r in enumerate(reader):
                    dt = datetime(
                        year=int(r["year"]),
                        month=int(r["month"]),
                        day=1,
                        hour=0,
                        minute=0,
                        second=0,
                    )
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
                            float(r["marginal_ghg"]),
                            r["value_curve_name"],
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
                    datetime,
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
                    ghg_adder_rebalancing,
                    value_curve_name)
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
        MAX_ROWS = 10000

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
                            r["value_curve_name"],
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
        MAX_ROWS = 10000

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

    def process_metered_load_shape(self, metered_load_shape_path: str):
        """Note this has to be run after process_project_info, as it depends
        on the utility for each project having been loaded"""

        def copy_write(cur, rows):
            with cur.copy(
                "COPY elec_load_shape (hour_of_year, utility, load_shape_name, value) FROM STDIN"
            ) as copy:
                for row in rows:
                    copy.write_row(row)

        # get the list of load shape names we care about from project_info
        metered_load_shape_query = "SELECT distinct utility, load_shape from project_info where load_shape not in (select distinct load_shape_name from elec_load_shape);"
        load_shapes_utils = defaultdict(list)
        with self.engine.begin() as conn:
            result = conn.execute(text(metered_load_shape_query))
            for row in result:
                load_shapes_utils[row[1].upper()].append(row[0])

        # get the load shapes in this file
        with open(metered_load_shape_path) as f:
            # this probably escapes fine but a csv reader is a safer bet
            columns = [x.strip() for x in f.readline().split(",")]
            metered_load_shapes = [
                c.strip()
                for c in columns
                if columns.index(c) > columns.index("hour_of_year")
            ]

        cur = self.connection.cursor()
        MAX_ROWS = 10000

        buf = []
        # This is so deeply nested because the project info could have more
        # than one utility per a given metered load shape.
        with open(metered_load_shape_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                for load_shape in metered_load_shapes:
                    try:
                        utils = load_shapes_utils[load_shape.upper()]
                    except KeyError:
                        # If load shape not in load_shapes_utils, don't load it
                        continue
                    for util in utils:
                        buf.append(
                            [
                                int(row["hour_of_year"]),
                                util.upper(),
                                load_shape.upper(),
                                float(row[load_shape]),
                            ]
                        )
                if len(buf) >= MAX_ROWS:
                    copy_write(cur, buf)
                    buf = []
            else:
                copy_write(cur, buf)
        self.connection.commit()

    def _load_project_info_data(self, insert_text, project_info_dicts):
        """insert_text isn't needed for postgresql"""

        def copy_write(cur, rows):
            with cur.copy(
                "COPY project_info (id, state, utility, region, mwh_savings, therms_savings, load_shape, therms_profile, start_year, start_quarter, start_date, end_date, units, eul, ntg, discount_rate, admin_cost, measure_cost, incentive_cost, value_curve_name) FROM STDIN"
            ) as copy:
                for row in rows:
                    copy.write_row(row)

        rows = [
            (
                x["id"],
                x["state"],
                x["utility"],
                x["region"],
                x["mwh_savings"],
                x["therms_savings"],
                x["load_shape"],
                x["therms_profile"],
                x["start_year"],
                x["start_quarter"],
                x["start_date"],
                x["end_date"],
                x["units"],
                x["eul"],
                x["ntg"],
                x["discount_rate"],
                x["admin_cost"],
                x["measure_cost"],
                x["incentive_cost"],
                x["value_curve_name"],
            )
            for x in project_info_dicts
        ]
        cursor = self.connection.cursor()
        copy_write(cursor, rows)
        self.connection.commit()


class SqliteManager(DBManager):
    def __init__(self, fv_config: FLEXValueConfig):
        super().__init__(fv_config)
        self.template_env = Environment(
            loader=PackageLoader("flexvalue", "templates"),
            autoescape=select_autoescape(),
        )
        self.config = fv_config

    def _get_truncate_prefix(self):
        """sqlite doesn't support TRUNCATE"""
        return "DELETE FROM"

    def _get_db_connection_string(self, config: FLEXValueConfig) -> str:
        database = config.database
        conn_str = f"sqlite+pysqlite://{database}"
        return conn_str


class BigQueryManager(DBManager):
    def __init__(self, fv_config: FLEXValueConfig, client: bigquery.Client = None):
        super().__init__(fv_config)
        self.template_env = Environment(
            loader=PackageLoader("flexvalue", "templates"),
            autoescape=select_autoescape(),
        )
        self.config = fv_config
        self.table_names = [
            self.config.elec_av_costs_table,
            self.config.gas_av_costs_table,
            self.config.elec_load_shape_table,
            self.config.therms_profiles_table,
            self.config.project_info_table,
        ]
        self.client = (
            client
            if client is not None
            else bigquery.Client(project=self.config.project)
        )

    def _get_target_dataset(self):
        # Use an output table because we know those have write permissions
        if self.config.separate_output_tables:
            # both are required, so just pick one:
            return ".".join(self.config.electric_output_table.split(".")[:-1])
        else:
            return ".".join(self.config.output_table.split(".")[:-1])

    def _test_connection(self):
        logging.debug("in bigquerymanager._test_connection")
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
            for row in result:  # there will be only one, but we have to iterate
                if row.get("count") == 0:
                    empty_tables.append(table_name)
        return empty_tables

    def _prepare_table(
        self,
        table_name: str,
        sql_filepath: str,
        index_filepaths=[],
        truncate: bool = False,
    ):
        """table_name: includes the dataset for the table
        sql_filepath: the path to the template that will be rendered to produce the preparation sql
        truncate: if True, all data will be removed from the table; the table will not be dropped
        """
        if not self._table_exists(table_name):
            dataset = ".".join(
                table_name.split(".")[:-1]
            )  # get everything before last '.'
            template = self.template_env.get_template(sql_filepath)
            sql = template.render({"dataset": dataset})
            logging.debug(f"create sql = \n{sql}")
            query_job = self.client.query(sql)
            result = query_job.result()
        else:
            if truncate:
                sql = f"DELETE FROM {table_name} WHERE TRUE;"
                query_job = self.client.query(sql)
                result = query_job.result()

    def process_elec_av_costs(self, elec_av_costs_path: str, truncate=False):
        # We don't need to do anything with this in BQ, just use the table provided
        pass

    def process_gas_av_costs(self, gas_av_costs_path: str, truncate=False):
        """Add a datetime column if none exists, and populate it. It
        will be used to join on in later calculations.
        """
        logging.debug("In bq process_gas_av_costs")
        self._ensure_datetime_column(self.config.gas_av_costs_table)
        sql = f'UPDATE {self.config.gas_av_costs_table} gac SET datetime = (DATETIME(FORMAT("%d-%d-01 00:00:00", gac.year, gac.month))) WHERE TRUE;'
        query_job = self.client.query(sql)
        result = query_job.result()

    def _ensure_datetime_column(self, table_name):
        """Ensure that the table with name `table_name` has a column
        named `datetime`, of type `DATETIME`.
        """
        table = self.client.get_table(table_name)
        has_datetime = False
        for column in table.schema:
            if column.name == "datetime" and column.field_type == "DATETIME":
                has_datetime = True
                break
        if not has_datetime:
            original_schema = table.schema
            new_schema = original_schema[:]  # Creates a copy of the schema.
            new_schema.append(bigquery.SchemaField("datetime", "DATETIME"))

            table.schema = new_schema
            table = self.client.update_table(table, ["schema"])  # Make an API request.

            if len(table.schema) == len(original_schema) + 1 == len(new_schema):
                print("A new column has been added.")
            else:
                raise FLEXValueException(
                    f"Unable to add a datetime column to {table_name}; can't process gas avoided costs."
                )

    def _copy_table(self, source_table, target_table):
        """source_table and target_table must include the dataset in their values, like {dataset}.{table}.
        This deletes target_table before copying source_table to it.
        """
        self.client.delete_table(target_table, not_found_ok=True)
        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        copy_job = self.client.copy_table(
            source_table, target_table, job_config=job_config
        )
        copy_job.result()

    def process_elec_load_shape(self, elec_load_shapes_path: str, truncate=False):
        """Transforms data in the table specified by config.elec_load_shape_table, and loads it into `elec_load_shape`."""
        dataset = self._get_target_dataset()
        self._prepare_table(
            f"{dataset}.elec_load_shape", "bq_create_elec_load_shape.sql", truncate=True
        )
        template = self.template_env.get_template("bq_populate_elec_load_shape.sql")
        # Black ruins readability here, disable
        # fmt: off
        sql = template.render(
            {
                "target_dataset": dataset,
                "source_dataset": ".".join(self.config.elec_load_shape_table.split(".")[:-1]),
                "elec_load_shape_table": self.config.elec_load_shape_table,
                "elec_load_shape_table_name_only": self.config.elec_load_shape_table.split(".")[-1],
            }
        )
        # fmt: on
        logging.info(f"elec_load_shape sql = {sql}")
        query_job = self.client.query(sql)
        result = query_job.result()

    def process_metered_load_shape(self, metered_load_shapes_path: str, truncate=False):
        """Transforms data in the table specified by config.metered_load_shape_table, and
        loads it into `elec_load_shape`. First copies the specified elec_load_shape table
        into {target_dataset}.elec_load_shape."""
        dataset = self._get_target_dataset()
        self._prepare_table(
            f"{dataset}.elec_load_shape",
            "bq_create_elec_load_shape.sql",
            truncate=truncate,
        )

        # if we process the elec load shape, that will create target_dataset.elec_load_shape; otherwise...
        if not self.config.process_elec_load_shape:
            self._copy_table(
                self.config.elec_load_shape_table,
                f"{self._get_target_dataset()}.elec_load_shape",
            )
        template = self.template_env.get_template("bq_populate_metered_load_shape.sql")
        # Black ruins readability here, disable
        # fmt: off
        sql = template.render(
            {
                "source_dataset": ".".join(self.config.metered_load_shape_table.split(".")[:-1]),
                "target_dataset": dataset,
                "project_info_table": self.config.project_info_table,
                "metered_load_shape_table": self.config.metered_load_shape_table,
                "metered_load_shape_table_only_name": self.config.metered_load_shape_table.split(".")[-1],
            }
        )
        # fmt: on
        logging.info(f"metered_load_shape sql = {sql}")
        query_job = self.client.query(sql)
        result = query_job.result()

    def process_therms_profile(self, therms_profiles_path: str, truncate: bool = False):
        """Transforms data in the table specified by config.therms_profile_table, and loads it into `therms_profile`."""
        dataset = self._get_target_dataset()
        self._prepare_table(
            f"{dataset}.therms_profile",
            "bq_create_therms_profile.sql",
            truncate=truncate,
        )
        template = self.template_env.get_template("bq_populate_therms_profile.sql")
        # Black ruins readability here, disable
        # fmt: off
        sql = template.render(
            {
                "source_dataset": ".".join(self.config.therms_profiles_table.split(".")[:-1]),
                "target_dataset": dataset,
                "therms_profiles_table": self.config.therms_profiles_table,
                "therms_profiles_table_only_name": self.config.therms_profiles_table.split(".")[-1],
            }
        )
        # fmt: on
        logging.debug(f"therms_profile sql = {sql}")
        query_job = self.client.query(sql)
        result = query_job.result()

    def _elec_load_shape_for_context(self):
        if (
            self.config.process_metered_load_shape
            or self.config.process_elec_load_shape
        ):
            return f"{self._get_target_dataset()}.elec_load_shape"
        return self.config.elec_load_shape_table

    def _therms_profile_for_context(self):
        if self.config.process_therms_profiles:
            return f"{self._get_target_dataset()}.therms_profile"
        return self.config.therms_profiles_table

    def _get_calculation_sql_context(self, mode=""):
        elec_agg_columns = self._elec_aggregation_columns()
        gas_agg_columns = self._gas_aggregation_columns()
        elec_addl_fields = self._elec_addl_fields(elec_agg_columns)
        gas_addl_fields = self._gas_addl_fields(gas_agg_columns)
        # TODO double-check this: should the av_costs tables be treated the same as the load shapes?
        context = {
            "project_info_table": self.config.project_info_table,
            "eac_table": self.config.elec_av_costs_table,
            "els_table": self._elec_load_shape_for_context(),
            "gac_table": self.config.gas_av_costs_table,
            "therms_profile_table": self._therms_profile_for_context(),
            "float_type": self.config.float_type(),
            "database_type": self.config.database_type,
            "elec_components": self._elec_components(),
            "gas_components": self._gas_components(),
            "use_value_curve_name_for_join": self.config.use_value_curve_name_for_join,
        }
        if mode == "electric":
            context["elec_aggregation_columns"] = elec_agg_columns
            context["elec_addl_fields"] = elec_addl_fields
        elif mode == "gas":
            context["gas_aggregation_columns"] = gas_agg_columns
            context["gas_addl_fields"] = gas_addl_fields
        else:
            context["elec_aggregation_columns"] = elec_agg_columns
            context["gas_aggregation_columns"] = gas_agg_columns
            context["elec_addl_fields"] = elec_addl_fields
            context["gas_addl_fields"] = set(gas_addl_fields) - set(elec_addl_fields)

        if (
            self.config.output_table
            or self.config.electric_output_table
            or self.config.gas_output_table
        ):
            table_name = self.config.output_table
            if mode == "electric":
                table_name = self.config.electric_output_table
            elif mode == "gas":
                table_name = self.config.gas_output_table
            context["create_clause"] = f"CREATE OR REPLACE TABLE {table_name} AS ("
        return context

    def _run_calc(self, sql):
        query_job = self.client.query(sql)
        result = query_job.result()
        if (
            not self.config.output_table
            and not self.config.electric_output_table
            and not self.config.gas_output_table
        ):
            if self.config.output_file:
                with open(self.config.output_file, "w") as outfile:
                    for row in result:
                        outfile.write(",".join([f"{x}" for x in row.values()]) + "\n")
            else:
                for row in result:
                    print(",".join([f"{x}" for x in row.values()]))

    def process_project_info(self, project_info_path: str):
        pass

    def reset_elec_av_costs(self):
        # The elec avoided costs table doesn't get changed; the super()'s
        # reset_elec_av_costs will truncate this table, so add a no-op here.
        pass

    def reset_gas_av_costs(self):
        # FLEXvalue adds and populates the `datetime` column, so remove it:
        sql = f"ALTER TABLE {self.config.gas_av_costs_table} DROP COLUMN datetime;"
        query_job = self.client.query(sql)
        try:
            result = query_job.result()
        except api_core.exceptions.BadRequest as e:
            # We are resetting before datetime was added, ignore exception
            pass

    def reset_elec_load_shape(self):
        logging.debug("Resetting elec load shape")
        self._reset_table(f"{self._get_target_dataset()}.elec_load_shape")

    def reset_therms_profiles(self):
        logging.debug("Resetting therms_profile")
        self._reset_table(f"{self._get_target_dataset()}.therms_profile")

    def _reset_table(self, table_name):
        truncate_prefix = self._get_truncate_prefix()
        try:
            sql = f"{truncate_prefix} {table_name} WHERE TRUE;"
            query_job = self.client.query(sql)
            result = query_job.result()
        except NotFound as e:
            # If the table doesn't exist yet, it will be created later
            pass

    def _exec_select_sql(self, sql: str):
        # This is just here to support testing
        query_job = self.client.query(sql)
        result = query_job.result()
        return [x for x in result]
