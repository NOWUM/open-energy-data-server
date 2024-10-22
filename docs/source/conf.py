# Configuration file for the Sphinx documentation builder.

# -- Project information

project = "OEDS"
copyright = "2024, NOWUM-Energy"
author = "NOWUM-Energy"

release = "0.1"
version = "0.1.0"

# -- General configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "recommonmark",
    "sphinx_rtd_theme",
]


intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

templates_path = ["_templates"]

# -- Options for HTML output

html_theme = "sphinx_rtd_theme"

# -- Options for EPUB output
epub_show_urls = "footnote"

source_suffix = [".rst", ".md"]

# Path setup for including parent directory markdown files
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))
