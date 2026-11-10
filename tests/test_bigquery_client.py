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

import google.auth
import pytest
import toml
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from flexvalue.config import FLEXValueException
from flexvalue.db import SqliteManager
from flexvalue.flexvalue import FlexValueRun

BIGQUERY_TABLES = dict(
    project_info_table="dataset.project_info",
    elec_load_shape_table="dataset.elec_load_shape",
    therms_profiles_table="dataset.therms_profiles",
    elec_av_costs_table="dataset.elec_av_costs",
    gas_av_costs_table="dataset.gas_av_costs",
)
IN_MEMORY_SQLITE = "/:memory:"


def offline_client(project):
    return bigquery.Client(project=project, credentials=AnonymousCredentials())


def config_file(tmp_path, **database):
    path = tmp_path / "config.toml"
    path.write_text(toml.dumps({"database": database}))
    return str(path)


@pytest.fixture
def anonymous_default_credentials(monkeypatch):
    monkeypatch.setattr(
        google.auth, "default", lambda *args, **kwargs: (AnonymousCredentials(), None)
    )


def test_bigquery_run_uses_the_client_and_its_project():
    client = offline_client("client-project")

    run = FlexValueRun(
        database_type="bigquery", bigquery_client=client, **BIGQUERY_TABLES
    )

    assert run.db_manager.client is client
    assert run.config.project == "client-project"


def test_client_project_overrides_a_configured_project():
    run = FlexValueRun(
        database_type="bigquery",
        project="configured-project",
        bigquery_client=offline_client("client-project"),
        **BIGQUERY_TABLES,
    )

    assert run.config.project == "client-project"


def test_non_bigquery_run_ignores_the_client():
    run = FlexValueRun(
        database_type="sqlite",
        database=IN_MEMORY_SQLITE,
        project="configured-project",
        bigquery_client=offline_client("client-project"),
    )

    assert isinstance(run.db_manager, SqliteManager)
    assert run.config.project == "configured-project"


def test_bigquery_config_file_run_takes_the_client_project(tmp_path):
    client = offline_client("client-project")

    run = FlexValueRun(
        config_file(
            tmp_path,
            database_type="bigquery",
            project="configured-project",
            **BIGQUERY_TABLES,
        ),
        bigquery_client=client,
    )

    assert run.db_manager.client is client
    assert run.config.project == "client-project"


def test_non_bigquery_config_file_run_ignores_the_client(tmp_path):
    run = FlexValueRun(
        config_file(
            tmp_path,
            database_type="sqlite",
            database=IN_MEMORY_SQLITE,
            project="configured-project",
        ),
        bigquery_client=offline_client("client-project"),
    )

    assert isinstance(run.db_manager, SqliteManager)
    assert run.config.project == "configured-project"


def test_without_a_client_the_run_builds_one_for_the_configured_project(
    anonymous_default_credentials,
):
    run = FlexValueRun(
        database_type="bigquery", project="configured-project", **BIGQUERY_TABLES
    )

    assert isinstance(run.db_manager.client, bigquery.Client)
    assert run.db_manager.client.project == "configured-project"


def test_without_a_client_the_project_is_still_required():
    with pytest.raises(FLEXValueException):
        FlexValueRun(database_type="bigquery", **BIGQUERY_TABLES)
