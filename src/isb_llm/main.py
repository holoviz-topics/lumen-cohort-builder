import lumen.ai as lmai

from isb_llm.sources import ISBSource


app = lmai.ExplorerUI(
    ISBSource(project_id="isb-cgc-bq", location="us"),
    llm=lmai.llm.LlamaCpp(),
    agents=[],
)
app.servable()
