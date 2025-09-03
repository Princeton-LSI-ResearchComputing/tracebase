# TraceBaseDocs

Documentation for [TraceBase](https://github.com/Princeton-LSI-ResearchComputing/tracebase) users

Markdown documents stored in `/TraceBaseDocs` are used to generate a static site using [MkDocs](https://www.mkdocs.org/)
(with a [readthedocs](https://readthedocs.org/) template).

## Development

To generate the static site from these Markdown documents, first install mkdocs and related plugins (which are listed in
the general requirements document for tracebase):

1) `pip install -U -r requirements/dev.txt`

2) serve locally or build (see next steps)

### Use mkdocs to generate a local static site

1) `cd TraceBaseDocs`

2) `mkdocs serve --verbose` (verbose is optional but recommended)

The site can be accessed at [http://127.0.0.1:8000/repo-name/](http://127.0.0.1:8000/repo-name/). The locally served
site updates live as changes are made to the Markdown documents.

### Build the static site on GitHub Pages

Build the site to serve publicly on GitHub Pages:

1) `mkdocs gh-deploy -m "description of site update"`

If you get an error that states:

> Deployment terminated: Previous deployment was made with MkDocs version 3.1; you are attempting to deploy with an
> older version (1.6.1). Use --ignore-version to deploy anyway.

This appears to possibly be a bug in either `mkdocs` or one of its plugins.  It should be safe to use `--ignore-version`.

This generates HTML based on Markdown documents.  It can be accessed by anyone at
[https://Princeton-LSI-ResearchComputing.github.io/tracebase/](https://Princeton-LSI-ResearchComputing.github.io/tracebase/).
Note that updated Markdown docs in the repository will not be reflected in the public site until a developer deploys the
revised site.

## User-facing Exception Documentation Generation Notes

The Upload page described the submission building process, which involved guidance on resolving errors and warnings. The
documentation for those errors and warnings is aided by [Sphinx](https://www.sphinx-doc.org/), which automatically
generates documentatiomn from python docstrings.  The following are tips to use when refreshing/updating that
documentation.

For a Sphinx overview, see this [YouTube Demo](https://youtu.be/BWIrhgCAae0?si=_U98ZRF60E86Jht4).

### Install

To integrate the manually maintained documentation and the [Sphinx](https://www.sphinx-doc.org/)-generated
documentation, we generate the sphinx dopcumentation in Markdown and copy it to the manually generated documentation
using `sphinx-markdown-builder`.  Install `sphinx` and `sphinx-markdown-builder` in your `venv`:

```bash
pip install -U sphinx
pip install -U sphinx-markdown-builder
```

### Setup

```bash
sphinx-quickstart docs
```

Answer all the prompts with the defaults except these items:

> Project name: `TraceBase`
> Author name(s): `Fan Kang, Robert Leach, John Matese, Michael Neinast, Rachid Ounit, Lance Parsons, Josh Rabinowitz`
> Project release []: `3.1.6`

Edit `docs/conf.py` to add the following necessary imports and setup at the top:

```python
import os
import sys

# Ad-hoc applying commonmark works **way** better than myst_parser
import commonmark
# We must set up Django so that sphinx can import modules like model
import django

# Tell sphinx where the codebase is relative to the sphinx config directory
sys.path.insert(0, os.path.abspath(".."))

# Set the DJANGO_SETTINGS_MODULE environment variable
os.environ["DJANGO_SETTINGS_MODULE"] = "TraceBase.settings"

# Now we can initialize Django
django.setup()
```

Set the `extensions` and add `autodoc_default_options` like so:

```python
extensions = [
    "sphinx.ext.autodoc",
    "sphinx_markdown_builder",
]

autodoc_default_options = {
    "undoc-members": False,  # Exclude undocumented members
    "no-undoc-members": True,
    "show-inheritance": False,  # Exclude inheritance
    "no-show-inheritance": True,  # Exclude inheritance
    "class-doc-from": "class",
}
```

Finally, at the bottom of the file, add the following:

```python

SKIP_CLASSES = [
    # Manually added
    "InfileError",
    "SummarizableError",
    "SummarizedInfileError",
    "AggregatedErrors",
    "AggregatedErrorsSet",
    "MultiLoadStatus",
    # Empty (classes without docstrings)
    # No setting of `'undoc-members': False` and/or `'no-undoc-members': True` seems to work, so I skipped them manually
    # To generate the "# Empty" classes...
    # grep -A 1 ^class ../DataRepo/utils/exceptions.py | grep -v "^--" | grep -B 1 -E '^    pass|^    @property|^    #' | \
    # grep -v '^--' | grep ^class | perl -e 'while(<>){s/^class ([^\(:]+).*/$1/;print}'
    "HeaderError",
    "SheetMergeError",
    # Summarization classes (found using grep '^class All')
    "AllMissingSamples",
    "AllMissingCompounds",
    "AllMissingTissues",
    "AllMissingStudies",
    "AllMissingTreatments",
    "AllUnexpectedLabels",
    "AllNoScans",
    "AllMzxmlSequenceUnknown",
    "AllMzXMLSkipRowErrors",
    "AllMultiplePeakGroupRepresentations",
    # Manually found summarization classes
    "UnexpectedLabels",
    "UnmatchedBlankMzXMLs",
    "AnimalsWithoutSamples",
    "AnimalsWithoutSerumSamples",
    "AssumedMzxmlSampleMatches",
    "ConflictingValueErrors",
    "DuplicateValueErrors",
    "MissingC12ParentPeaks",
    "MissingFCircCalculationValues",
    "MultiplePeakGroupRepresentations",
    "NewResearchers",
    "NoTracerLabeledElementsError",
    "RequiredColumnValues",
    "RequiredValueErrors",
    "PossibleDuplicateSamples",
    "ProhibitedCompoundNames",
    "UnmatchedMzXMLs",
    # Commented as not (or rarely) "user facing" using grep
    # grep -E "^class|^    # NOTE:" ../DataRepo/utils/exceptions.py | grep -B 1 facing | grep ^class | \
    # cut -d ' ' -f 2 | cut -d '(' -f 1
    "MissingColumnGroup",
    "UnequalColumnGroups",
    "MissingRecords",
    "MissingModelRecords",
    "MissingModelRecordsByFile",
    "RequiredArgument",
    "DryRun",
    "NoCommonLabel",
    "MultipleDefaultSequencesFound",
    "UnmatchedMzXML",
    "DuplicateFileHeaders",
    "InvalidDtypeDict",
    "InvalidDtypeKeys",
    "ExcelSheetsNotFound",
    "InvalidHeaderCrossReferenceError",
    "OptionsNotAvailable",
    "MutuallyExclusiveOptions",
    "MutuallyExclusiveArgs",
    "MutuallyExclusiveMethodArgs",
    "RequiredOptions",
    "ConditionallyRequiredOptions",
    "ConditionallyRequiredArgs",
    "NoLoadData",
    "StudyDocConversionException",
    "PlaceholdersAdded",
    "PlaceholderAdded",
    "BlanksRemoved",
    "BlankRemoved",
    "PlaceholderDetected",
    "NotATableLoader",
    "RollbackException",
    "InvalidStudyDocVersion",
    "StudyDocVersionException",
    "ProhibitedStringValue",
    "DeveloperWarning",
]


def docstring(app, what, name, obj, options, lines):
    """See https://stackoverflow.com/a/56428123"""
    md = "\n".join(lines)
    ast = commonmark.Parser().parse(md)
    rst = commonmark.ReStructuredTextRenderer().render(ast)
    lines[:] = rst.splitlines()


def skip_functions(app, what, name, obj, would_skip, options):
    """Returns bool indicating whether the doc should be skipped"""
    if type(obj).__name__ in ["function", "method"]:
        return True
    elif name in SKIP_CLASSES:
        return True
    elif would_skip is False:
        return False
    return True


def setup(app):
    app.connect("autodoc-skip-member", skip_functions)
    app.connect("autodoc-process-docstring", docstring)
```

Generate the `rst` files using `autodoc`.  This will only generate documentation for the exceptions file:

```bash
sphinx-apidoc -o docs -d 1 --remove-old . TraceBase TraceBaseDocs DataRepo/models DataRepo/schemas DataRepo/templates DataRepo/templatetags DataRepo/tests DataRepo/views DataRepo/widgets DataRepo/data DataRepo/fixtures DataRepo/formats DataRepo/loaders DataRepo/management DataRepo/migrations DataRepo/admin.py DataRepo/apps.py DataRepo/context_processors.py DataRepo/forms.py \
DataRepo/multiforms.py DataRepo/pager.py DataRepo/urls.py DataRepo/utils/file_utils.py DataRepo/utils/func_utils.py DataRepo/utils/infusate_name_parser.py DataRepo/utils/queryset_to_pandas_dataframe.py DataRepo/utils/studies_exporter.py DataRepo/utils/text_utils.py manage.py
```

### Build the Sphinx Documentation

```bash
cd docs
sphinx-build -M markdown . _build/markdown
```

where:

`.` = The "source" direcotry doesn't refer to the source code of the project.  It is the "source" code of sphinx, aka
the sphinx config directory.  I.e. The directory where the conf.py file exists.

### Massage the putput using some perl magic

```bash
cat _build/markdown/markdown/DataRepo.utils.md | \
# Remove all but class name in the header
perl -e 'while(<>){s/^### \*[^\*]+\* [^\(]+\.([^\(]+)(?:\(.*\)|\z)/### $1/;print;}' | \
# Skip Attributes sections in the class docstrings
perl -e '$skip=0;while(<>){if(/Attributes:/){$skip=1;}elsif($skip){if(/^#/){$skip=0;}}if($skip){next}print;}' | \
# Skip undocumented member attributes documentation that sphinx adds and refuses to skip
perl -e '$skip=0;while(<>){if(/^####/){$skip=1;}elsif($skip){if(/^#/){$skip=0;}}if($skip){next}print;}' | \
# Clean up lists with long lines (this removes gaps that commonmark adds when it concatenates the lines)
perl -e 'while(<>){if(/^ *[\-\*] /){s/       */ /g;}print;}' > _build/markdown/markdown/user_facing_exceptions.md
```

### Manual work

Finally, what's left over is the manual work of adjusting the headers and reformatting the "Summarized by" content to
add it to the headers.
