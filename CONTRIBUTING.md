# Contributing to the TraceBase project

This document describes the basics of how to set up the TraceBase Project
repository in order to start developing/contributing.

## Getting Started

### Install Python and Postgres Dependencies

#### Python

TraceBase has been tested with Python 3.7 through 3.10.  TraceBase may work with later Python versions, but currently
there are dependency issues with Python 3.14.

Install a Python version that is 3.9.1 or greater (3.10 is the current recommendation) from:

    https://www.python.org/downloads/

Make sure that version of python is in your path:

    $ python --version
    Python 3.10.11

Test to make sure that the `python` command now shows your latest python install:

    $ python --version
    Python 3.10.11

#### Postgres

Install Postgres via package installer from
[https://www.postgresql.org](https://www.postgresql.org).  Be sure to make note
of where it installs the `psql` command-line utility, so you can add it to your
PATH, e.g. if you see:

    Command Line Tools Installation Directory: /Library/PostgreSQL/13

Then, add this to your PATH:

    /Library/PostgreSQL/13/bin

Configuration:

    Username: postgres
    Password: tracebase
    Port: 5432

In the Postgres app interface, you can find where the `postgresql.conf` file is
located.  Open it in a text editor and make sure these settings are uncommented
& correct:

    client_encoding: 'UTF8'
    default_transaction_isolation: 'read committed'
    log_timezone = 'America/New_York'

Manually create the tracebase database (`tracebase`) in postgres:

    createdb -U postgres tracebase

Create a tracebase postgres user:

    > create user tracebase with encrypted password 'mypass';
    > ALTER USER tracebase CREATEDB;
    > grant all privileges on database tracebase to tracebase;

### Setup the TraceBase project

#### Clone the repository

    git clone https://github.com/Princeton-LSI-ResearchComputing/tracebase.git
    cd tracebase

#### Create a virtual environment

Create a virtual environment (from a bash shell) and activate it, for example:

    python3 -m venv .venv
    source .venv/bin/activate

#### Install dependencies in the virtual environment

Install Django and psycopg2 dependencies as well as linters and other
development related tools. Use `requirements/prod.txt` for production
dependencies.

    python -m pip install -U pip
    python -m pip install -r requirements/dev.txt

#### Verify Installations

Django:

    python3 -m django --version
    4.2.27

### Configure TraceBase

Create a new secret:

    python -c "import secrets; print(secrets.token_urlsafe())"

Database and secret key information should not be stored directly in settings
that are published to the repository.  We use environment variables to store
configuration data.  This makes it possible to easily change between
environments of a deployed application (see [The Twelve-Factor
App](https://www.12factor.net/config)).  The `.env` file you create here is pre-
configured to be ignored by the repository, so do not explicitly check it in.

Copy the TraceBase environment example:

    cp TraceBase/.env.example TraceBase/.env

Update the .env file to reflect the new secret key and the database credentials
you used when setting up Postgres.

Set up the project's postgres database:

    python manage.py migrate
    python manage.py createcachetable

### Create an Admin User

To be able to access the admin page, on the command-line, run:

    python manage.py createsuperuser

and supply your desired account credentials for testing.

### Load Some Example Data (Optional)

#### Load underlying data needed by the example studies

    python manage.py loaddata data_types data_formats
    python manage.py load_study --infile DataRepo/data/examples/compounds_tissues_treatments_lcprotocols/study.xlsx

#### Load the example studies

    python manage.py load_study --infile DataRepo/data/examples/13C_Valine_and_PI3Ki_in_flank_KPC_mice/study.xlsx
    python manage.py load_study --infile DataRepo/data/examples/obob_fasted/study.xlsx
    python manage.py load_study --infile DataRepo/data/examples/obob_fasted_ace_glycerol_3hb_citrate_eaa_fa_multiple_tracers/study.xlsx
    python manage.py load_study --infile DataRepo/data/examples/obob_fasted_glc_lac_gln_ala_multiple_labels/study.xlsx

### Start TraceBase

To run the development server in your sandbox, execute:

    python manage.py runserver

Then go to this site in your web browser:

    http://127.0.0.1:8000/

## Pull Requests

### Code Formatting Standards

All pull requests must pass linting prior to being merged.

Currently, all pushes are linted using [GitHub's
Super-Linter](https://github.com/github/super-linter). The configuration files
for the most used linters have been setup in the project root to facilitate
linting on developers' machines.

#### Linting

Linting for this project runs automatically on GitHub when a PR is submitted,
but this section describes how to lint your changes locally.

##### Individual linters

For the most commonly used linters (*e.g.* for python, HTML, and Markdown
files) it is recommended to install linters locally and run them in your
editor. Some linters that may be useful to install locally include:

- Code
  - [jscpd](https://github.com/kucherenko/jscpd)
- Python
  - [flake8](https://flake8.pycqa.org/en/latest/)
  - [pylint](https://www.pylint.org/)
  - [black](https://black.readthedocs.io/en/stable/)
  - [isort](https://pycqa.github.io/isort/)
  - [mypy](https://mypy.readthedocs.io/)
- JavaScript
  - [standard](https://standardjs.com)
- HTML
  - [HTMLHint](https://htmlhint.com/)
- CSS
  - [stylelint](https://stylelint.io)
- Markdown
  - [markdownlint](https://github.com/igorshubovych/markdownlint-cli#readme)
    - Example install: `npm install --save-dev markdownlint-cli`
    - Recommended version: `0.45.0`
  - [textlint](https://github.com/textlint/textlint)
    - Example install
      - `npm install --save-dev textlint`
      - `npm install --save-dev textlint-rule-terminology`
      - `npm install --save-dev textlint-filter-rule-comments`
- Config
  - [editorconfig-checker](https://www.npmjs.com/package/editorconfig-checker)

It is recommended to run superlinter (described below) routinely or
automatically before submitting a PR, but if you want a quick check while
developing, you can run these example linting commands on the command-line,
using each linter's config that we've set up for superlinter:

    find . \( -type f -not -path '*/\.*' -not -path "*bootstrap*" \
        -not -path "*__pycache__*" \) -exec jscpd {} \;
    flake8 --config .flake8 --extend-exclude migrations,.venv .
    pylint --rcfile .pylintrc --load-plugins pylint_django \
        --django-settings-module TraceBase.settings -d E1101 \
        TraceBase DataRepo *.py
    black --exclude '\.git|__pycache__|migrations|\.venv' .
    isort --sp .isort.cfg -c -s migrations -s .venv -s .git -s __pycache__ .
    mypy --config-file .mypy.ini --disable-error-code annotation-unchecked .
    find . \( ! -iname "*bootstrap*" -not -path '*/\.*' -iname "*.js" \) \
        -exec standard --fix --verbose {} \;
    htmlhint -c .htmlhintrc .
    stylelint --config .stylelintrc.json --ip '**/bootstrap*' **/*.css
    markdownlint --config .markdown-lint.yml .
    find . \( ! -iname "*bootstrap*" -not -path '*/\.*' -not -path '*node_modules*' \
        -iname "*.md" \) -exec npx textlint -c .textlintrc.json {} \;
    editorconfig-checker -v -exclude '__pycache__|\.DS_Store|\~\$.*' TraceBase DataRepo

Note, some of these linter installs can be rather finicky, so if you have
trouble, consider running Super-Linter locally, as described below.

##### Superlinter

In addition to linting files as you write them, developers may wish to [run
Superlinter on the entire repository
locally](https://github.com/github/super-linter/blob/master/docs/run-linter-locally.md).
This is most easily accomplished using [Docker](https://docs.docker.com/get-docker/).
Create a script outside of the repository that runs superlinter via docker and run it
from the repository root directory. Example script:

    #!/usr/bin/env sh
    docker pull github/super-linter:slim-v6

    docker run \
        -e FILTER_REGEX_EXCLUDE="(\.pylintrc|migrations|static\/bootstrap.*)" \
        -e LINTER_RULES_PATH="/" \
        -e IGNORE_GITIGNORED_FILES=true \
        -e RUN_LOCAL=true \
        -v /full/path/to/tracebase/:/tmp/lint github/super-linter

Note: The options `FILTER_REGEX_EXCLUDE`, `LINTER_RULES_PATH`, and
`IGNORE_GITIGNORED_FILES` should match the settings in the GitHub Action in
`.github/workflows/superlinter.yml`

### Testing

#### Test Implementation

All pull requests must implement tests of the changes implemented prior to being
merged.  Each app should either contain `tests.py` or a `tests` directory
containing multiple test scripts.  Currently, all tests are implemented using
the TestCase framework.

See these resources for help implementing tests:

- [Testing in Django (Part 1) - Best Practices and
  Examples](https://realpython.com/testing-in-django-part-1-best-practices-and-examples/)
- [Django Tutorial Part 10: Testing a Django web
  application](https://developer.mozilla.org/en-US/docs/Learn/Server-side/Django/Testing)

#### Quality Control

All pull requests must pass new and all previous continuous integration tests,
all JavaScript tests, and pass a migration check before merging.  Run the
following locally before submitting a pull request:

    python manage.py test
    python manage.py makemigrations --check --dry-run
    python -m http.server

Then after the last command, in a major browser, go to:

    http://127.0.0.1:8000/DataRepo/tests/static/js/tests.html

and confirm all of the JavaScript tests pass.

### Model Updates

Any pull requests that include changes to the model, must include an update to
the migrations and the resulting auto-generated migration scripts must be
checked in.

#### Migration Process

Create the migration scripts:

    python manage.py makemigrations

Check for unapplied migrations:

    python manage.py showmigrations

Apply migrations to the postgres database:

    python manage.py migrate

### Archive Files

TraceBase has an `ArchiveFile` class that is used to [store data files on the
file system](https://docs.djangoproject.com/en/3.2/topics/files/). The files
are stored locally using the
[`MEDIA_ROOT`](https://docs.djangoproject.com/en/3.2/ref/settings/#std-setting-MEDIA_ROOT)
and
[`MEDIA_URL`](https://docs.djangoproject.com/en/3.2/ref/settings/#std-setting-MEDIA_URL)
settings. A
[`FileField`](https://docs.djangoproject.com/en/3.2/ref/models/fields/#django.db.models.FileField)
is used to manage store the files and to track the storage location in the
database.

Archived files are stored in
`{MEDIA_ROOT}/archive_files/{YYYY-MM}/{DATA_TYPE}/{FILENAME}"`. Duplicate file
names are made unique by Django's
[`Storage.save()`](https://docs.djangoproject.com/en/3.2/ref/files/storage/#django.core.files.storage.Storage.save)
method.

When running tests, it is desirable that the file stored do not remain on the
file system after testing is complete. This is accomplished in TraceBase by
using a custom test runner `Tracebase/runner.py`. The test runner changes the
`MEDIA_ROOT` and `DEFAULT_FILE_STORAGE` settings during test runs to use a
temporary location on local file storage.

Per the
[`FileField.delete()`](https://docs.djangoproject.com/en/4.2/ref/models/fields/#django.db.models.fields.files.FieldFile.delete)
documentation, when a model is deleted, related files are not deleted. If you
need to cleanup orphaned files, you’ll need to handle it yourself (for
instance, with a custom management command that can be run manually or
scheduled to run periodically via e.g. cron). See [Management command to list
orphaned files in `MEDIA_ROOT` #718](https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/718).
