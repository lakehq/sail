# Configuration file for the Sphinx documentation builder.

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
