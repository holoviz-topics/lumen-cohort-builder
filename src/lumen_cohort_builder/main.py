import os

import lumen.ai as lmai
from lumen.sources.duckdb import DuckDBSource


PROJECT_ID = "isb-cgc-bq"
os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
app = lmai.ExplorerUI(
    # data=["./data/penguins.csv"],
    # data=[ISBSource(project_id=PROJECT_ID, location="us")],
    data=DuckDBSource(tables=["./data/penguins.parquet"]),
    llm=lmai.llm.LlamaCpp(),
    # agents=[],
)
app.servable()
