import json
from typing import override, Any

import google.auth
import numpy as np
import pandas as pd
import param
from tqdm import tqdm

from google.cloud import bigquery, exceptions
from google.cloud.bigquery.client import Client
from google.auth.credentials import Credentials
from google.auth.exceptions import DefaultCredentialsError
from lumen.sources.base import BaseSQLSource
from sqlalchemy.engine.create import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.sql.expression import text


class SQLAlchemySource(BaseSQLSource):
    """SQLAlchemy source object allowing connections to databases via SQLAlchemy URIs.

    Attributes
    ----------
    drivername : param.String, default is ""
    from_url : param.String, default is ""
    username : param.String, default is ""
    password : param.String, default is ""
    host : param.String, default is ""
    port : param,.Integer, default is 0
    database : param.String, default is ""
    query : param.Dict, default is None
    url : str
    engine : sqlalchemy.engine.base.Engine
    """

    drivername = param.String(doc="SQL driver.")
    from_url = param.String(doc="SQLAlchemy URL.")
    username = param.String(doc="Username used for authentication.")
    password = param.String(doc="The password for the given username.")
    host = param.String(doc="IP address of the database.")
    port = param.Integer(doc="Port used to connect to the database.")
    database = param.String(doc="Database name.")
    query = param.Dict(
        doc=(
            "A dictionary of string keys to string values to be passed to the dialect "
            "and/or the DBAPI upon connect."
        ),
    )

    def __init__(self, **params) -> None:
        if self.from_url == "":
            self.url = self.create_url()
        else:
            self.url = str(self.from_url)
        self.engine = create_engine(url=self.url)
        super().__init__(**params)

    def create_url(self) -> URL:
        PARAM_STR_DEFAULT = ""
        PARAM_INT_DEFAULT = 0

        drivername = str(self.drivername)
        username = str(self.username)
        username = username if username != PARAM_STR_DEFAULT else None
        password = str(self.password)
        password = password if password != PARAM_STR_DEFAULT else None
        host = str(self.host)
        host = host if host != PARAM_STR_DEFAULT else None
        port = int(self.port)  # pyright: ignore [reportArgumentType]
        port = port if port != PARAM_INT_DEFAULT else None
        database = str(self.database)
        database = database if database != PARAM_STR_DEFAULT else None
        query = self.query

        url = URL(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            query=query,  # pyright: ignore [reportArgumentType]
        )
        return url

    def run_query(self, query: str):
        records = None
        with self.engine.begin() as connection:
            query_ = text(query)
            records = connection.execute(query_)
        return records


class BigQuerySource(SQLAlchemySource):
    """BigQuery Lumen Source object.

    Notes
    -----
    - A `dataset` in BigQuery is similar to a `database` in a traditional relational database.

    Attributes
    ----------
    tables : list[str] | dict[str, str]
        Tables found in the BigQuery project. Table names have the form
        f"{project_id}.{dataset_id}.{table_id}".
    project_id : str, optional, default is ""
        If no `project_id` is supplied, then this value will be set through authentication.
    location : str
    driver : str, default is `bigquery`
    dialect : str, default is `bigquery`
    client : Client
        Google BigQuery client object.
    credentials : Credentials
        Default credentials found in the current environment.
    """

    # NOTE: Can be either a dictionary or a list to allow for "synthetic" tables.
    tables = param.ClassSelector(
        class_=(list, dict),
        doc="Tables, also known as datasets in BigQuery.",
    )
    project_id = param.String(doc="The Google Cloud's project ID.")
    location = param.String(doc="Location where the project resides.")
    driver = "bigquery"
    dialect = "bigquery"

    def __init__(self, **params) -> None:
        self.client: Client | None = None
        self.credentials: Credentials | None = None
        super().__init__(**params)

    def authorize(self) -> None:
        """Get the default credentials for the current environment.

        To enable application default credentials with the Cloud SDK for the first time, run the
        following manually.

            gcloud init

        To manually enable authentication, run the following.

            gcloud auth application-default login

        This method will attempt to connect to Google Cloud using the SDK after you have run
        `gcloud init`.

        Returns
        -------
        None

        Raises
        ------
        DefaultCredentialsError
            This error is raised if no default credentials can be found. To prevent this in the
            future, you will need to run `gcloud init` at least once.
        """
        try:
            self.credentials, project_id = google.auth.default()  # pyright: ignore [reportAttributeAccessIssue]
        except DefaultCredentialsError:
            msg = (
                "No default credentials can be found. Please run `gcloud init` at least once "
                "to create default credentials to your Google Cloud project."
            )
            raise DefaultCredentialsError(msg)

        if self.project_id == "":
            self.project_id = project_id

    def get_tables(self) -> list[str]:
        """Get a list of available tables for the project.

        Returns
        -------
        list[str]
            Table names are composed of f"{project_id}.{dataset_id}.{table_id}".
        """
        # NOTE: The return here prevents this method, which is called multiple times, from
        #       repopulating the self.tables list.
        if isinstance(self.tables, list):  # type check for param.List
            if len(self.tables) != 0:
                return self.tables

        table_references = []
        if self.client is not None:
            project_id = self.client.project
            dataset_records = list(self.client.list_datasets())
            for dataset_record in tqdm(dataset_records):
                dataset_id = dataset_record.dataset_id
                table_records = self.client.list_tables(dataset=dataset_id)
                table_ids = [
                    table_record.table_id
                    for table_record in table_records
                    if table_record.table_id is not None
                ]
                for table_id in table_ids:
                    table_references.append(f"{project_id}.{dataset_id}.{table_id}")

        return table_references

    def get_sql_expr(self, table: str) -> str:
        if isinstance(self.tables, list | dict):
            if table not in self.tables:
                tables = self.tables if isinstance(self.tables, list) else list(self.tables.keys())
                raise KeyError(f"Table {table} not found in {tables}.")
        return f"SELECT * FROM {table} LIMIT 5"

    def create_sql_expr_source(self, tables: dict[str, str], **kwargs):
        # FIXME:
        pass

    def execute(self, sql_query: str) -> pd.DataFrame:
        output = pd.DataFrame()
        if self.client is not None:
            results_df = self.client.query_and_wait(sql_query)
            output = results_df.to_dataframe() if results_df is not None else output
        return output

    def create_client(
        self,
        project_id: str | None = None,
        location: str | None = None,
    ) -> Client | None:
        client = None
        try:
            if project_id is not None:
                if location is not None:
                    client = bigquery.Client(project=project_id, location=location)
                else:
                    client = bigquery.Client(project=project_id)
            if project_id is None:
                if location is not None:
                    client = bigquery.Client(location=location)
                else:
                    client = bigquery.Client()
        except Exception as e:
            msg = "Unable to create a BigQuery client."
            raise exceptions.ClientError(msg) from e

        return client

    def get_all_datasets_metadata(self) -> list[dict[str, str]]:
        """Get metadata for all available datasets in the project.

        Returns
        -------
        list[dict[str, str]]
        """
        data = []
        if self.client is not None:
            for record in self.client.list_datasets():
                project_id = record.project
                dataset_id = record.dataset_id
                dataset = self.client.get_dataset(dataset_ref=f"{project_id}.{dataset_id}")
                datum = {
                    "project_id": project_id,
                    "dataset_id": dataset_id,
                    "dataset_name": dataset.friendly_name,
                    "dataset_description": dataset.description,
                    "dataset_created": str(dataset.created),
                    "dataset_modified": str(dataset.modified),
                }
                data.append(datum)
        return data

    def get_table_metadata(self, project_id: str, dataset_id: str) -> list[dict[str, str | int]]:
        """Get metadata for all tables associated with the given dataset.

        Parameters
        ----------
        project_id : str
            The BigQuery project ID.
        dataset_id : str
            The dataset ID within the given project.

        Returns
        -------
        list[dict[str, str | int]]
        """
        data = []
        if self.client is not None:
            for record in self.client.list_tables(dataset=dataset_id):
                table_id = record.table_id
                table_reference = f"{project_id}.{dataset_id}.{table_id}"
                table = self.client.get_table(table=table_reference)
                datum = {
                    "project_id": project_id,
                    "dataset_id": dataset_id,
                    "table_id": table.table_id,
                    "table_name": table.friendly_name,
                    "table_description": table.description,
                    "table_created": str(table.created),
                    "table_modified": str(table.modified),
                    "table_num_rows": table.num_rows,
                    "table_clustering_fields": json.dumps(table.clustering_fields),
                    "table_labels": json.dumps(table.labels),
                }
                data.append(datum)
        return data

    def get_column_metadata(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
    ) -> list[dict[str, str]]:
        """Get metadata for all columns associated with the given table.

        Parameters
        ----------
        project_id : str
            The BigQuery project ID.
        dataset_id : str
            The dataset ID within the given project.
        table_id : str
            The table ID for the given dataset.

        Returns
        -------
        list[dict[str, str]]
        """
        data = []
        if self.client is not None:
            table = self.client.get_table(table=f"{project_id}.{dataset_id}.{table_id}")
            columns = table.schema
            for column in columns:
                column_fields = []
                for column_field in column.fields:
                    field_datum = column_field.__dict__["_properties"]
                    column_fields.append(field_datum)
                datum = {
                    "project_id": project_id,
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "column_id": column.name,
                    "column_type": column.field_type,
                    "column_description": column.description,
                    "column_fields": json.dumps(column_fields),
                }
                data.append(datum)
        return data

    def get_project_metadata(self) -> pd.DataFrame:
        """Get metadata about the BigQuery project.

        The `project_id` is associated with the authenticated client.

        Returns
        -------
        pd.DataFrame
        """
        data = []
        if self.client is not None:
            dataset_records = list(self.client.list_datasets())
            for dataset_record in tqdm(dataset_records):
                project_id = dataset_record.project
                dataset_id = dataset_record.dataset_id
                dataset_reference = f"{project_id}.{dataset_id}"
                dataset = self.client.get_dataset(dataset_ref=dataset_reference)
                table_records = list(self.client.list_tables(dataset=dataset_id))
                for table_record in table_records:
                    table_id = table_record.table_id
                    table_reference = f"{project_id}.{dataset_id}.{table_id}"
                    table = self.client.get_table(table=table_reference)

                    columns = table.schema
                    for column in columns:
                        column_fields = []
                        for column_field in column.fields:
                            field_datum = column_field.__dict__["_properties"]
                            column_fields.append(field_datum)
                        datum = {
                            "project_id": project_id,
                            "dataset_id": dataset_id,
                            "dataset_name": dataset.friendly_name,
                            "dataset_description": dataset.description,
                            "dataset_created": dataset.created,
                            "dataset_modified": dataset.modified,
                            "table_id": table_id,
                            "table_name": table.friendly_name,
                            "table_description": table.description,
                            "table_created": table.created,
                            "table_modified": table.modified,
                            "table_num_rows": table.num_rows,
                            "table_clustering_fields": json.dumps(table.clustering_fields),
                            "table_labels": json.dumps(table.labels),
                            "column_id": column.name,
                            "column_type": column.field_type,
                            "column_description": column.description,
                            "column_fields": json.dumps(column_fields),
                        }
                        data.append(datum)
        df = pd.DataFrame(data)
        return df


class ISBSource(BigQuerySource):
    enum_threshold = 0.10
    location = "us"

    def __init__(self, ignore_versioned: bool = True) -> None:
        self.ignore_versioned = ignore_versioned

        # Uses stored default Google Cloud credentials to authenticate.
        self.authorize()

        # The `isb_client` is used to acquire metadata about the ISB-CGC-BQ project.
        self.isb_client = self.create_client(project_id="isb-cgc-bq", location=self.location)

        # The `sql_client` is used to run queries on the ISB project, using your Google Cloud
        # project ID. This is why it is not supplied here.
        self.sql_client = self.create_client(location=self.location)

        # Set the tables value on initialization to prevent multiple attempts to populate it.
        self.tables = self.get_tables()

        # self.metadata = self.get_project_metadata()

    @override
    def get_tables(self) -> list[str]:
        """Get a list of available tables for the ISB-CGC-BQ project.

        Returns
        -------
        list[str]
            Table names are composed of f"{project_id}.{dataset_id}.{table_id}".
        """
        # NOTE: The return here prevents this method, which is called multiple times, from
        #       repopulating the self.tables list.
        if isinstance(self.tables, list):  # type check for param.List
            if len(self.tables) != 0:
                return self.tables

        table_references = []
        if self.isb_client is not None:
            project_id = self.isb_client.project
            dataset_records = list(self.isb_client.list_datasets())
            for dataset_record in tqdm(dataset_records):
                dataset_id = dataset_record.dataset_id

                dataset_is_versioned = dataset_id.split("_")[-1] == "versioned"
                if dataset_is_versioned and self.ignore_versioned:
                    continue

                table_records = self.isb_client.list_tables(dataset=dataset_id)
                table_ids = [
                    table_record.table_id
                    for table_record in table_records
                    if table_record.table_id is not None
                ]
                for table_id in table_ids:
                    table_references.append(f"{project_id}.{dataset_id}.{table_id}")

        return table_references

    def get_project_metadata(self) -> pd.DataFrame:
        """Get metadata about the ISB-CGC-BQ project.

        The ISB data is quite large and difficult to navigate. Using a local vector store gives the
        user an opportunity to use a LLM to query against the metadata before generating SQL queries
        in BigQuery.

        Returns
        -------
        pd.DataFrame
        """
        data = []
        if self.isb_client is not None:
            dataset_records = list(self.isb_client.list_datasets())
            for dataset_record in tqdm(dataset_records):
                project_id = dataset_record.project
                dataset_id = dataset_record.dataset_id

                dataset_is_versioned = dataset_id.split("_")[-1] == "versioned"
                if dataset_is_versioned and self.ignore_versioned:
                    continue

                dataset_ref = f"{project_id}.{dataset_id}"
                dataset = self.isb_client.get_dataset(dataset_ref=dataset_ref)

                dataset_created = (
                    pd.to_datetime(dataset.created) if dataset.created is not None else None
                )
                if dataset_created is not None:
                    if dataset_created.tz:
                        dataset_created = dataset_created.astimezone("UTC").tz_localize(None)
                    dataset_created = pd.to_datetime(dataset_created).strftime(
                        "%Y-%m-%dT%H:%M:%S.%f"
                    )

                dataset_modified = (
                    pd.to_datetime(dataset.modified) if dataset.modified is not None else None
                )
                if dataset_modified is not None:
                    if dataset_modified.tz:
                        dataset_modified = dataset_modified.astimezone("UTC").tz_localize(None)
                    dataset_modified = pd.to_datetime(dataset_modified).strftime(
                        "%Y-%m-%dT%H:%M:%S.%f"
                    )

                table_records = list(self.isb_client.list_tables(dataset=dataset_id))
                for table_record in table_records:
                    table_id = table_record.table_id
                    table_ref = f"{project_id}.{dataset_id}.{table_id}"
                    table = self.isb_client.get_table(table=table_ref)

                    table_created = (
                        pd.to_datetime(table.created) if table.created is not None else None
                    )
                    if table_created is not None:
                        if table_created.tz:
                            table_created = table_created.astimezone("UTC").tz_localize(None)
                        table_created = pd.to_datetime(table_created).strftime(
                            "%Y-%m-%dT%H:%M:%S.%f"
                        )

                    table_modified = (
                        pd.to_datetime(table.modified) if table.modified is not None else None
                    )
                    if table_modified is not None:
                        if table_modified.tz:
                            table_modified = table_modified.astimezone("UTC").tz_localize(None)
                        table_modified = pd.to_datetime(table_modified).strftime(
                            "%Y-%m-%dT%H:%M:%S.%f"
                        )

                    columns = table.schema
                    for column in columns:
                        column_fields = []
                        for column_field in column.fields:
                            field_datum = column_field.__dict__["_properties"]
                            column_fields.append(field_datum)
                        datum = {
                            "project_id": project_id,
                            "dataset_id": dataset_id,
                            "dataset_name": dataset.friendly_name,
                            "dataset_description": dataset.description,
                            "dataset_created": dataset_created,
                            "dataset_modified": dataset_modified,
                            "table_id": table_id,
                            "table_name": table.friendly_name,
                            "table_description": table.description,
                            "table_created": table_created,
                            "table_modified": table_modified,
                            "table_num_rows": table.num_rows,
                            "table_clustering_fields": json.dumps(table.clustering_fields),
                            "table_labels": json.dumps(table.labels),
                            "column_id": column.name,
                            "column_type": column.field_type,
                            "column_description": column.description,
                            "column_fields": json.dumps(column_fields),
                        }
                        data.append(datum)
        df = pd.DataFrame(data)
        return df

    @override
    def get_schema(  # type: ignore [reportIncompatibleMethodOverride]
        self,
        table: str,
        limit: int | None = None,
    ) -> dict[str, dict[str, Any]] | dict[str, Any]:
        """Determine the schema of the given `table`.

        This method overrides the inherited `get_schema` from the base class `Source`. The reason
        why we override

        Parameters
        ----------
        table : str
            The name of the table. Must be in the form: f"{project_id}.{dataset_id}.{table_id}".

        Returns
        -------
        dict[str, dict[str, Any]] | dict[str, Any]
        """
        schema = {}
        if self.isb_client is not None and self.sql_client is not None:
            bq_table = self.isb_client.get_table(table=table)
            project_id = bq_table.project
            dataset_id = bq_table.dataset_id
            table_id = bq_table.table_id
            table_name = f"{project_id}.{dataset_id}.{table_id}"
            num_rows = bq_table.num_rows if bq_table.num_rows is not None else 1e-6

            columns = bq_table.schema
            for column in columns:
                column_name = column.name
                column_type = column.field_type

                if table_name not in schema:
                    schema[table_name] = {}

                if column_type == "STRING":
                    if column_name not in schema[table_name]:
                        schema[table_name][column_name] = {"type": column_type.lower(), "enum": []}

                    # Determine if the column should be enumerated in the schema.
                    query = (
                        f"SELECT COUNT(DISTINCT {column_name}) "
                        f"FROM {project_id}.{dataset_id}.{table_id};"
                    )
                    job = self.sql_client.query(query)
                    num_distict_values = [result[0] for result in job.result()]
                    num_distict_values = (
                        num_distict_values[0] if num_distict_values else self.enum_threshold
                    )
                    perc_distinct_values = num_distict_values / num_rows
                    if perc_distinct_values <= self.enum_threshold:
                        query = (
                            f"SELECT {column_name} "
                            f"FROM {project_id}.{dataset_id}.{table_id} "
                            f"GROUP BY {column_name};"
                        )
                        job = self.sql_client.query(query)
                        results = sorted(
                            result[0] for result in job.result() if result[0] is not None
                        )
                        schema[table_name][column_name]["enum"] = results

                elif column_type in ["TIMESTAMP", "DATE"]:
                    if column_name not in schema[table_name]:
                        schema[table_name][column_name] = {
                            "type": "str",
                            "format": "datetime",
                            "min": "",
                            "max": "",
                        }

                    query = (
                        f"SELECT MIN({column_name}) AS min_date, MAX({column_name}) AS max_date "
                        f"FROM {project_id}.{dataset_id}.{table_id};"
                    )
                    job = self.sql_client.query(query)
                    results = list(job.result())

                    min_date = results[0][0]
                    if min_date.tz:
                        min_date = min_date.astimezone("UTC").tz_localize(None)
                    min_date_str = pd.to_datetime(min_date).strftime("%Y-%m-%dT%H:%M:%S.%f")
                    schema[table_name][column_name]["min"] = min_date_str

                    max_date = results[0][1]
                    if max_date.tz:
                        max_date = max_date.astimezone("UTC").tz_localize(None)
                    max_date_str = pd.to_datetime(max_date).strftime("%Y-%m-%dT%H:%M:%S.%f")
                    schema[table_name][column_name]["max"] = max_date_str

                elif column_type == "BOOLEAN":
                    if column_name not in schema[table_name]:
                        schema[table_name][column_name] = {"type": column_type.lower()}

                elif column_type == "RECORD":
                    # FIXME: Ignore nested data structures for now.
                    continue

                elif column_type in ["FLOAT", "INTEGER"]:
                    if column_name not in schema[table_name]:
                        schema[table_name][column_name] = {
                            "type": column_type.lower() if column_type == "INTEGER" else "number",
                            "inclusiveMinimum": -1.0 * np.inf,
                            "inclusiveMaximum": np.inf,
                        }

                    # We will query for the min/max values in the column.
                    query = (
                        f"SELECT APPROX_QUANTILES({column_name, 1}) AS bounds "
                        f"FROM {project_id}.{dataset_id}.{table_id}"
                    )
                    job = self.sql_client.query(query)
                    results = dict(job.result())
                    inclusiveMinimum, inclusiveMaximum = results["bounds"]
                    schema[table_name][column_name]["inclusiveMinimum"] = inclusiveMinimum
                    schema[table_name][column_name]["inclusiveMaximum"] = inclusiveMaximum
        return schema
