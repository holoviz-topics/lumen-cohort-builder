from typing import override, Any

import google.auth
import numpy as np
import pandas as pd
import param
from tqdm import tqdm

from google.cloud import bigquery, exceptions
from google.cloud.bigquery.client import Client
from lumen.sources.base import BaseSQLSource
from sqlalchemy.engine.create import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.sql.expression import text


class SQLAlchemySource(BaseSQLSource):
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

    def get_sql_expr(self, table: str): ...

    def create_sql_expr_source(self, tables: dict[str, str]): ...

    def execute(self, sql_query: str) -> pd.DataFrame: ...

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
        port = int(self.port)  # type: ignore [reportArgumentType]
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
            query=query,  # type: ignore [reportArgumentType]
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
    tables : list | dict
    dialect : str, default is `bigquery`
    driver : str, default is `bigquery`
    project_id : str
    location : str
    client : Client
        Google BigQuery client object.

    Parameters
    ----------
    project_id : str
    location : str
    """

    # NOTE: Can be either a dictionary or a list to allow for "synthetic" tables.
    tables = param.ClassSelector(
        class_=(list, dict),
        doc="Tables, also known as datasets in BigQuery.",
    )
    dialect = param.String(default="bigquery", doc="SQL dialect to use.")

    def __init__(self, project_id: str, location: str, **params) -> None:
        self.driver = "bigquery"
        self.project_id = project_id
        self.location = location
        self.client: Client | None = None

    def get_tables(self) -> list[str]:
        # NOTE: The return here prevents this method, which is called multiple times, from
        #       repopulating the self.tables list.
        if isinstance(self.tables, dict | list):  # type check for param.[Dict|List]
            if len(self.tables) != 0:
                return list(self.tables)

        dataset_ids = []
        if self.client is not None:
            datasets = list(self.client.list_datasets())
            dataset_ids = [str(dataset.dataset_id) for dataset in datasets]

        return dataset_ids

    def authorize(self) -> None:
        self.credentials, project_id = google.auth.default()
        if self.project_id is None:
            self.project_id = project_id

    def create_client(self) -> None | Client:
        client = None
        try:
            client = bigquery.Client(self.project_id, location=self.location)
        except Exception as e:
            msg = "Unable to create a BigQuery client."
            raise exceptions.ClientError(msg) from e
        return client

    def get_all_datasets_metadata(self) -> list[dict[str, str]]:
        data = []
        if self.client is not None:
            for record in self.client.list_datasets():
                dataset = self.client.get_dataset(
                    dataset_ref=f"{record.project}.{record.dataset_id}"
                )
                created = dataset.created.timestamp() if dataset.created is not None else None
                modified = dataset.modified.timestamp() if dataset.modified is not None else None
                datum = {
                    "project": dataset.project,
                    "dataset_id": dataset.dataset_id,
                    "dataset_name": dataset.friendly_name,
                    "dataset_description": dataset.description,
                    "dataset_created": created,
                    "dataset_modified": modified,
                }
                data.append(datum)
        return data

    def get_table_metadata(self, project: str, dataset_id: str) -> list[dict[str, str]]:
        data = []
        if self.client is not None:
            for record in self.client.list_tables(dataset=dataset_id):
                table = self.client.get_table(table=f"{project}.{dataset_id}.{record.table_id}")
                created = table.created.timestamp() if table.created is not None else None
                modified = table.modified.timestamp() if table.modified is not None else None
                datum = {
                    "project": project,
                    "dataset_id": dataset_id,
                    "table_id": table.table_id,
                    "table_name": table.friendly_name,
                    "table_description": table.description,
                    "table_created": created,
                    "table_modified": modified,
                    "table_num_rows": table.num_rows,
                    "table_clustering_fields": table.clustering_fields,
                    "table_labels": table.labels,
                }
                data.append(datum)
        return data

    def get_column_metadata(
        self,
        project: str,
        dataset_id: str,
        table_id: str,
    ) -> list[dict[str, str]]:
        data = []
        if self.client is not None:
            table = self.client.get_table(table=f"{project}.{dataset_id}.{table_id}")
            columns = table.schema
            for column in columns:
                column_fields = []
                for column_field in column.fields:
                    field_datum = column_field.__dict__["_properties"]
                    # The mode key indicates if the field is nullable or not.
                    field_datum.pop("mode")
                    column_fields.append(field_datum)
                datum = {
                    "project": project,
                    "dataset_id": dataset_id,
                    "table_id": table.table_id,
                    "column_id": column.name,
                    "column_type": column.field_type,
                    "column_description": column.description,
                    "column_fields": column_fields,
                }
                data.append(datum)
        return data


class ISBSource(BigQuerySource):
    def __init__(self, project_id: str, location: str = "us") -> None:
        self.dialect = "bigquery"
        self.driver = "bigquery"
        self.project_id = project_id
        self.location = location
        self.authorize()
        self.client = self.create_client()
        self.metadata = self.get_project_metadata()

    def get_project_metadata(self) -> pd.DataFrame:
        """Get metadata about the ISB-CGC-BQ project.

        The method contains the side-effect of populating the `self.tables` object if it has not
        already been generated.

        Returns
        -------
        pd.DataFrame
        """
        data = []
        print("Acquiring metadata about the BigQuery project.")
        if self.client is not None:
            datasets = list(self.client.list_datasets())
            for dataset_record in tqdm(datasets):
                project = dataset_record.project
                dataset_id = dataset_record.dataset_id
                dataset_ref = f"{project}.{dataset_id}"
                dataset = self.client.get_dataset(dataset_ref=dataset_ref)
                dataset_created = (
                    dataset.created.timestamp() if dataset.created is not None else None
                )
                dataset_modified = (
                    dataset.modified.timestamp() if dataset.modified is not None else None
                )
                tables = list(self.client.list_tables(dataset=dataset_id))
                for table_record in tables:
                    table_id = table_record.table_id
                    table_ref = f"{project}.{dataset_id}.{table_id}"
                    table = self.client.get_table(table=table_ref)
                    table_created = table.created.timestamp() if table.created is not None else None
                    table_modified = (
                        table.modified.timestamp() if table.modified is not None else None
                    )
                    columns = table.schema
                    for column in columns:
                        column_fields = []
                        for column_field in column.fields:
                            field_datum = column_field.__dict__["_properties"]
                            # The mode key indicates if the field is nullable or not.
                            field_datum.pop("mode")
                            column_fields.append(field_datum)
                        datum = {
                            "project": project,
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
                            "table_clustering_fields": table.clustering_fields,
                            "table_labels": table.labels,
                            "column_id": column.name,
                            "column_type": column.field_type,
                            "column_description": column.description,
                            "column_fields": column_fields,
                        }
                        data.append(datum)
        df = pd.DataFrame(data)
        if not self.tables:
            self.tables = sorted(df["dataset_id"].unique())
        return df

    @override
    def get_schema(
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
            The name of the table. Must be in the form: f"{project}.{dataset_id}.{table_id}".

        Returns
        -------
        dict[str, dict[str, Any]] | dict[str, Any]
        """
        schema = {}
        if self.client is not None:
            bq_table = self.client.get_table(table=table)
            project = bq_table.project
            dataset_id = bq_table.dataset_id
            table_id = bq_table.table_id
            table_name = f"{project}.{dataset_id}.{table_id}"

            columns = bq_table.schema
            for column in columns:
                column_name = column.name
                column_type = column.field_type

                if table_name not in schema:
                    schema[table_name] = {}

                if column_type == "STRING":
                    if column_name not in schema[table_name]:
                        schema[table_name][column_name] = {"type": column_type.lower(), "enum": []}

                    query = (
                        f"SELECT {column_name} "
                        f"FROM {project}.{dataset_id}.{table_id} "
                        f"GROUP BY {column_name};"
                    )
                    job = self.client.query(query)
                    results = sorted(result[0] for result in job.result() if result[0] is not None)
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
                        f"FROM {project}.{dataset_id}.{table_id};"
                    )
                    job = self.client.query(query)
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
                        f"FROM {project}.{dataset_id}.{table_id}"
                    )
                    job = self.client.query(query)
                    results = dict(job.result())
                    inclusiveMinimum, inclusiveMaximum = results["bounds"]
                    schema[table_name][column_name]["inclusiveMinimum"] = inclusiveMinimum
                    schema[table_name][column_name]["inclusiveMaximum"] = inclusiveMaximum
        return schema
