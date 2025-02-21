from typing import override

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

    def __init__(self) -> None:
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
        port = int(self.port)
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
            query=query,
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
    - A `dataset` in BigQuery is a `table` in SQL.

    Attributes
    ----------
    driver : str, default is `bigquery`
    dialect : str, default is `bigquery`
    client : Client
        Google BigQuery client object.

    Parameters
    ----------
    project_id : str
    location : str
    """

    tables = param.List(doc="List tables, known as datasets in BigQuery.")

    def __init__(self, project_id: str, location: str) -> None:
        self.driver = "bigquery"
        self.dialect = "bigquery"
        self.project_id = project_id
        self.location = location
        self.client: Client | None = None

    def get_tables(self) -> list[str]:
        # NOTE: The return here prevents calling this method multiple times.
        if isinstance(self.tables, list):  # type check for param.List
            if len(self.tables) != 0:
                return self.tables

        dataset_ids = []
        if self.client is not None:
            if not self.tables:  # pyright: ignore [reportGeneralTypeIssues] (param) (param.List)
                datasets = list(self.client.list_datasets())
                dataset_ids = [str(dataset.dataset_id) for dataset in datasets]

        self.tables = dataset_ids
        return self.tables

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
                    "dataset_description": dataset.description,
                    "dataset_name": dataset.friendly_name,
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
                    "table_description": table.description,
                    "table_name": table.friendly_name,
                    "table_created": created,
                    "table_modified": modified,
                    "table_num_rows": table.num_rows,
                }
                data.append(datum)
        return data

    def get_column_metadata(
        self, project: str, dataset_id: str, table_id: str
    ) -> list[dict[str, str]]:
        data = []
        if self.client is not None:
            table = self.client.get_table(table=f"{project}.{dataset_id}.{table_id}")
            columns = table.schema
            for column in columns:
                datum = {
                    "project": project,
                    "dataset_id": dataset_id,
                    "table_id": table.table_id,
                    "table_name": table.friendly_name,
                    "column_id": column.name,
                    "column_description": column.description,
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
        self.client: Client = self.create_client()
        # self.metadata = self.get_project_metadata()

    def get_project_metadata(self) -> pd.DataFrame:
        """Get metadata about the ISB-CGC-BQ project.

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
                            "column_name": column.name,
                            "column_type": column.field_type,
                            "column_description": column.description,
                            "column_fields": column_fields,
                        }
                        data.append(datum)
        df = pd.DataFrame(data)
        self.tables = sorted(df["dataset_id"].unique())
        return df

    @override
    def get_schema(self):
        if self.metadata is None:
            self.metadata = self.get_project_metadata()

        schema = {}
        for row_index, row in self.metadata.iterrows():
            project = row["project"]
            dataset_id = row["dataset_id"]
            table_id = row["table_id"]

            dataset_name = f"{project}.{dataset_id}.{table_id}"
            if dataset_name not in schema:
                schema[dataset_name] = {}

            column_name = row["column_name"]
            column_type = str(row["column_type"])

            if column_type == "STRING":
                if column_name not in schema[dataset_name]:
                    schema[dataset_name][column_name] = {"type": column_type.lower(), "enum": []}

            elif column_type == "TIMESTAMP":
                if column_name not in schema[dataset_name]:
                    schema[dataset_name][column_name] = {"type": column_type.lower()}

            elif column_type == "DATE":
                if column_name not in schema[dataset_name]:
                    schema[dataset_name][column_name] = {"type": column_type.lower()}

            elif column_type == "BOOLEAN":
                if column_name not in schema[dataset_name]:
                    schema[dataset_name][column_name] = {"type": column_type.lower()}

            elif column_type == "RECORD":
                if column_name not in schema[dataset_name]:
                    schema[dataset_name][column_name] = {"type": "items"}

            elif column_type in ["FLOAT", "INTEGER"]:
                if column_name not in schema[dataset_name]:
                    schema[dataset_name][column_name] = {
                        "type": column_type.lower() if column_type == "INTEGER" else "number",
                        "inclusiveMinimum": np.nan,
                        "inclusiveMaximum": np.inf,
                    }

                # We will query for the min/max values in the column.
                query = (
                    f"SELECT APPROX_QUANTILES({column_name, 4}) AS bounds "
                    f"FROM `{project}.{dataset_id}.{table_id}`"
                )
                job = self.client.query(query)
                results = dict(job.result())
                inclusiveMinimum, inclusiveMaximum = results["bounds"]
                schema[dataset_name][column_name]["inclusiveMinimum"] = inclusiveMinimum
                schema[dataset_name][column_name]["inclusiveMaximum"] = inclusiveMaximum
