import os

import param
import lumen.ai as lmai
import panel as pn
import pandas as pd

from lumen.ai.schemas import get_metaset
from panel_material_ui import (
    Button, Card, ChatFeed, ChatAreaInput, Dialog, MultiChoice,
    Progress, Page, TextInput
)
from panel.viewable import Viewer
from lumen.pipeline import Pipeline

from lumen_cohort_builder.sources import ISBSource


pn.extension('tabulator', 'codeeditor')


class CohortBuilder(Viewer):

    llm = param.ClassSelector(class_=lmai.llm.Llm)

    width = param.Integer(default=1200)

    def __init__(self, **params):
        super().__init__(**params)
        lmai.memory['source'] = self._source = pn.state.as_cached(
            'source', ISBSource, cache_dir='./cache', cache_data=False
        )
        lmai.memory['sources'] = [self._source]
        self._interface = ChatFeed(sizing_mode='stretch_both')
        if 'lookup_tool' in pn.state.cache:
            self._lookup = pn.state.cache['lookup_tool']
            self._vector_store = self._lookup.vector_store
        else:
            self._vector_store = pn.state.as_cached(
                'vectors',
                lmai.vector_store.NumpyVectorStore,
                embeddings=lmai.embeddings.HuggingFaceEmbeddings()
            )
            self._lookup = lmai.tools.TableLookup(
                vector_store=self._vector_store,
                include_metadata=True,
                interface=self._interface,
                min_similarity=0.2,
                sync_sources=False
            )

        self._sql_agent = lmai.agents.SQLAgent(interface=self._interface, llm=self.llm)
        self._table_ui = self._table_search()
        self._filter_ui = self._filter_table()
        self._dialog = Dialog()
        self._page = Page(
            header=[self._dialog],
            main=[self._table_ui],
            sidebar=[self._interface],
            sidebar_width=500,
            sidebar_open=False,
            title='Cohort Builder'
        )
        self._metadata = {}
        self._current_table = None

    def _table_search(self):
        self._table_df = pd.DataFrame(self._source.get_tables(), columns=['Table'])
        self._search_progress = Progress(visible=False, value=True, variant='indeterminate', sizing_mode='stretch_width')
        self._table = pn.widgets.Tabulator(
            self._table_df,
            pagination='remote',
            sizing_mode='stretch_width',
            max_width=self.width,
            theme='simple',
            selectable='checkbox'
        )
        self._table.param.watch(self._table_selection, 'selection')
        self._search_input = TextInput(sizing_mode='stretch_width', label='Keywords', max_width=self.width)
        self._search_input.param.watch(self._handle_table_search, 'value')
        return Card(
            self._search_input,
            self._search_progress,
            self._table,
            Button(label='Next', on_click=self._handle_next, button_type='success', styles={'margin': '0 0 0 auto'}, disabled=self._table.param.selection.rx().rx.len()==0),
            title='Search tables',
            collapsible=False,
            max_width=self.width,
            styles={'margin': '1em auto'}
        )

    async def _handle_table_search(self, event):
        with self._search_progress.param.update(visible=True):
            results = self._vector_store.query(event.new)
            result_data = []
            for result in results:
                table = result['metadata']['table_name']
                if table in self._metadata:
                    continue
                self._metadata[table] = metadatum = self._source.get_metadata(table)
                similarity = result['similarity']
                result_data.append((table, metadatum['friendly_name'], metadatum['num_rows'], similarity))

            def lookup_metadata(s):
                metadata = self._metadata[s.Table]
                info = "\n\n".join(f"**{key}**: {value}" for key, value in metadata.items() if isinstance(value, (int, str)))
                return info

            df = pd.DataFrame(result_data, columns=['Table', 'Name', 'Num Rows', 'Similarity'])
            self._table.param.update(row_content=lookup_metadata, value=df)

    async def _table_selection(self, event):
        if not event.new:
            return
        for sel in event.new[:1]:
            table_name = self._table.value.iloc[sel, 0]
            break
        self._current_table = table_name
        lmai.memory['table_sql_metaset'] = await get_metaset({self._source.name: self._source}, [table_name])

    def _filter_table(self):
        self._filter_input = TextInput(sizing_mode='stretch_width', max_width=self.width, label='Filters')
        self._filter_input.param.watch(self._handle_filter_table, 'value')
        self._filter_progress = Progress(
            visible=False, value=True, variant='indeterminate', sizing_mode='stretch_width'
        )
        self._sql_result = pn.pane.Markdown(sizing_mode='stretch_width')
        self._results_table = pn.widgets.Tabulator(
            pagination='remote',
            sizing_mode='stretch_width',
            page_size=20,
            max_width=self.width,
            theme='simple'
        )
        return Card(
            self._filter_input,
            self._filter_progress,
            self._sql_result,
            self._results_table,
            title='Filter Table',
            collapsible=False,
            max_width=self.width,
            styles={'margin': '1em auto'}
        )

    async def _handle_filter_table(self, event):
        with self._filter_progress.param.update(visible=True):
            response = await self._sql_agent.respond(
                [{'content': f'The user has provided the following filtering request: {event.new}', 'role': 'user'}]
            )
            sql = lmai.memory['sql']
            self._sql_result.object = f'```sql\n{sql}\n```'
            self._results_table.value = lmai.memory['pipeline'].data

    def _handle_next(self, event=None):
        if self._dialog.open:
            self._dialog.open = False
        elif (rows := self._metadata[self._current_table]['num_rows']) > 10000:
            self._dialog[:] = [
                f'### `{self._current_table}` has {rows} rows and may take a while to load.',
                pn.Row(
                    Button(
                        label='Cancel', button_type='error', on_click=lambda _: self._dialog.param.update(open=False), width=60
                    ),
                    Button(
                        label='Confirm', button_type='success', on_click=lambda _: self._handle_next(),
                        width=60
                    )
                )
            ]
            self._dialog.open = True
            return
        self._page.main = [self._filter_ui]
        with self._filter_progress.param.update(visible=True):
            pipeline = Pipeline(source=self._source, table=self._current_table)
            self._results_table.value = pipeline.param.data

    def __panel__(self):
        return self._page

if "OPENAI_API_KEY" in os.environ:
    llm = lmai.llm.OpenAI()
else:
    llm = lmai.llm.LlamaCpp()

CohortBuilder(llm=llm).servable()
