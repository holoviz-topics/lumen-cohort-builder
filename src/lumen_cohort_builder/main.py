import lumen.ai as lmai

from lumen_cohort_builder.sources import ISBSource


app = lmai.ExplorerUI(
    data=[ISBSource()],
    llm=lmai.llm.LlamaCpp(),
)
app.servable()
