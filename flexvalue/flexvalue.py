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
from flexvalue.config import FLEXValueConfig

from .db import (
    DBManager,
)


class FlexValueRun:
    def __init__(self, config_file=None, *, bigquery_client=None, **kwargs):
        try:
            self.config = FLEXValueConfig.from_file(config_file)
        except TypeError:
            self.config = FLEXValueConfig(**kwargs)
        if bigquery_client is not None and self.config.database_type == "bigquery":
            self.config.project = bigquery_client.project
        self.config.validate()
        self.db_manager = DBManager.get_db_manager(
            self.config, bigquery_client=bigquery_client
        )
        # if resetting any tables, do those before we load:
        if self.config.reset_elec_load_shape:
            self.db_manager.reset_elec_load_shape()
        if self.config.reset_elec_av_costs:
            self.db_manager.reset_elec_av_costs()
        if self.config.reset_therms_profiles:
            self.db_manager.reset_therms_profiles()
        if self.config.reset_gas_av_costs:
            self.db_manager.reset_gas_av_costs()

        if self.config.process_elec_av_costs:
            self.db_manager.process_elec_av_costs(
                self.config.elec_av_costs_file
                if self.config.elec_av_costs_file
                else self.config.elec_av_costs_table
            )
        if self.config.process_elec_load_shape:
            self.db_manager.process_elec_load_shape(
                self.config.elec_load_shape_file
                if self.config.elec_load_shape_file
                else self.config.elec_load_shape_table
            )
        if self.config.process_gas_av_costs:
            self.db_manager.process_gas_av_costs(
                self.config.gas_av_costs_file
                if self.config.gas_av_costs_file
                else self.config.gas_av_costs_table
            )
        if self.config.process_therms_profiles:
            self.db_manager.process_therms_profile(
                self.config.therms_profiles_file
                if self.config.therms_profiles_file
                else self.config.therms_profiles_file
            )
        if self.config.project_info_file or self.config.project_info_table:
            self.db_manager.process_project_info(
                self.config.project_info_file
                if self.config.project_info_file
                else self.config.project_info_table
            )
        # Have to load metered load shapes after project_info, so we can get the
        # utility for the metered shapes
        if self.config.process_metered_load_shape:
            self.db_manager.process_metered_load_shape(
                self.config.metered_load_shape_file
                if self.config.metered_load_shape_file
                else self.config.metered_load_shape_table
            )

    def run(self):
        self.db_manager.run()
