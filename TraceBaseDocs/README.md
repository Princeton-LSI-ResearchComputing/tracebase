# TraceBaseDocs

Documentation for
[TraceBase](https://github.com/Princeton-LSI-ResearchComputing/tracebase) users

Markdown documents stored in `/TraceBaseDocs` are used to generate a static
site using [MkDocs](https://www.mkdocs.org/) (with a
[readthedocs](https://readthedocs.org/) template).

## Development

To generate the static site from these Markdown documents, first install mkdocs
and related plugins (which are listed in the general requirements document for
tracebase):

1) `python -U pip install -r requirements/dev.txt`

2) serve locally or build (see next steps)

### Use mkdocs to generate a local static site

1) `cd TraceBaseDocs`

2) `mkdocs serve --verbose` (verbose is optional but recommended)

The site can be accessed at
[http://127.0.0.1:8000/repo-name/](http://127.0.0.1:8000/repo-name/). The
locally served site updates live as changes are made to the Markdown documents.

### Build the static site on GitHub Pages

Build the site to serve publicly on GitHub Pages: at:

1) `mkdocs gh-deploy -m "description of site update"`

This generates HTML based on Markdown documents.  It can be accessed by anyone
at
[https://Princeton-LSI-ResearchComputing.github.io/tracebase/](https://Princeton-LSI-ResearchComputing.github.io/tracebase/).
Note that updated Markdown docs in the repository will not be reflected in the public
site until a developer deploys the revised site.
