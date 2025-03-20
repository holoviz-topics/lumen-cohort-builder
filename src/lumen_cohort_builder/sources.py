import param
from lumen.sources.bigquery import BigQuerySource


class ISBSource(BigQuerySource):
    datasets = param.List(default=["TCGA"])

    ignore_versioned = param.Boolean(default=True, doc="Whether to ignore versioned datasets.")

    location = param.String(default="us", doc="Location where the project resides.")

    project_id = param.String(default="isb-cgc-bq", doc="The Google Cloud's project ID.")

    def _filter_dataset(self, dataset: str) -> bool:
        dataset_is_versioned = dataset.split("_")[-1] == "versioned"
        if dataset_is_versioned and self.ignore_versioned:
            return True
        return False
