import pandas as pd
from lumen.sources.base import cached_schema
from lumen.sources.bigquery import BigQuerySource


class ISBSource(BigQuerySource):
    def __init__(self, project_id: str, location: str = "us") -> None:
        self.dialect = "bigquery"
        self.driver = "bigquery"
        self.project_id = project_id
        self.location = location
        self.authorize()
        self.client = self.create_client()
        super().__init__(project_id=self.project_id, location=self.location)

    @cached_schema
    def cache_schema(self, skip_versioned: bool = True) -> None:
        """Cache the ISB-CGC-BQ project schema locally.

        Parameters
        ----------
        skip_versioned : bool, optional, default is `True`
            Flag to determine if `_versioned` datasets are skipped. Versioned datasets are stable
            representations of the data, however, they do not have the most up-to-date data in them.
            - `True`: DO NOT select versioned datasets.
            - `False`: ONLY select versioned datasets.

        Returns
        -------
        None
        """
        data = []
        for dataset_record in self.client.list_datasets():
            project = dataset_record.project
            dataset_id = dataset_record.dataset_id

            if skip_versioned:
                if dataset_id.endswith("_versioned"):
                    continue
                if not dataset_id.endswith("_versioned"):
                    data.extend(self._get_schema(project=project, dataset_id=dataset_id))

            if not skip_versioned:
                if dataset_id.endswith("_versioned"):
                    data.extend(self._get_schema(project=project, dataset_id=dataset_id))
                if not dataset_id.endswith("_versioned"):
                    continue

    def _get_schema(self, project: str, dataset_id: str) -> pd.DataFrame:
        """Construct the BigQuery schema.

        Parameters
        ----------
        project : str
            The project ID to use.
        dataset_id : str
            The ID of the dataset within the project.

        Returns
        -------
        pd.DataFrame
        """
        data = []
        dataset_ref = f"{project}.{dataset_id}"
        dataset = self.client.get_dataset(dataset_ref=dataset_ref)
        for table_record in self.client.list_tables(dataset=dataset_id):
            table_id = table_record.table_id
            table_ref = f"{project}.{dataset_id}.{table_id}"
            table = self.client.get_table(table=table_ref)
            columns = table.schema
            for column in columns:
                datum = {
                    "project": project,
                    "dataset_id": dataset_id,
                    "dataset_name": dataset.friendly_name,
                    "dataset_description": dataset.description,
                    "dataset_created": dataset.created.timestamp(),
                    "dataset_modified": dataset.modified.timestamp(),
                    "table_id": table.table_id,
                    "table_name": table.friendly_name,
                    "table_description": table.description,
                    "table_created": table.created.timestamp(),
                    "table_modified": table.modified.timestamp(),
                    "column_name": column.name,
                    "column_type": column.field_type,
                    "column_description": column.description,
                }
                data.append(datum)
        df = pd.DataFrame(data)
        return df
