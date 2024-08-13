# Configuration file for the Sphinx documentation builder.
from urllib.parse import quote

from sphinx.application import Sphinx
from sphinxcontrib.serializinghtml import JSONHTMLBuilder

project = 'Sail'
copyright = 'LakeSail, Inc.'
author = 'LakeSail'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

autosummary_generate = True


class _JSONHTMLBuilder(JSONHTMLBuilder):
    def get_target_uri(self, docname: str, typ: str | None = None) -> str:
        # We generate the URI in the same way as the HTML builder.
        return quote(docname) + self.link_suffix


def setup(app: Sphinx):
    app.add_builder(_JSONHTMLBuilder, override=True)
