# Configuration file for the Sphinx documentation builder.
from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote

from sphinxcontrib.serializinghtml import JSONHTMLBuilder

if TYPE_CHECKING:
    from sphinx.application import Sphinx

project = "Sail"
copyright = "LakeSail, Inc."  # noqa: A001
author = "LakeSail"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

autosummary_generate = True


class _JSONHTMLBuilder(JSONHTMLBuilder):
    def get_target_uri(self, docname: str, typ: str | None = None) -> str:  # noqa: ARG002
        # We generate the URI in the same way as the HTML builder.
        return quote(docname) + self.link_suffix


def setup(app: Sphinx):
    app.add_builder(_JSONHTMLBuilder, override=True)
