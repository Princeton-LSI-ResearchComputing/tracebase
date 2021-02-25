# Contributing to the TraceBase project

This document described the basics of how to set up the TraceBase Project Repo
in order to start developing/contributing.

## Getting Started

### Install Python and Postgres Dependencies

#### Python

Install the latest Python (version 3.9.1), and make sure it is in your path:

    $ python --version
    Python 3.9.1

Test to make sure that the `python` command now shows your latest python install:

    $ python --version
    Python 3.9.1

#### Postgres

Install Postgres via package installer from
[https://www.postgresql.org](https://www.postgresql.org).  Be sure to make note
of where it installs the `psql` command line utility, so you can add it to your
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

    psql -U postgres
    > CREATE DATABASE tracebase;

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

Install Django and psycopg2 dependencies as well as linters and other
development related tools. Use `requirements/prod.txt` for production
dependencies.

    python -m pip install -U pip  # Upgrade pip
    python -m pip install -r requirements/dev.txt  # Install requirements

#### Verify Installations

Django:

    python
    > import django
    > print(django.get_version())
    3.1.6

### Configure TraceBase

Copy the `TraceBase/.env.example` file to `TraceBase/.env` and update the
values to reflect those used when setting up Postgres.  A new secret can be
created using:

    python -c "import secrets; print(secrets.token_urlsafe())"

Secrets and database settings should not be stored directly in settings. Using
environment variables to store configuration likely to change between
environments allows these settings to be easily updated in a deployed
application (see [The Twelve-Factor App](https://www.12factor.net/config)).

### Start TraceBase

Set up the project's postgres database:

    python manage.py migrate

Verify you can run the development server.  Run:

    python manage.py runserver

Then go to this site in your web browser:

    http://127.0.0.1:8000/

## Code Formatting Standards

All Pull Requests must pass linting prior to being merged.

Currently, all pushes are linted using [GitHub's
Super-Linter](https://github.com/github/super-linter). The configuration files
for the most used linters have been setup in the project root to facilitate
linting on developers machines. These include:

* [Markdown-lint](https://github.com/igorshubovych/markdownlint-cli#readme) - `.markdown-lint.yml`
* [Flake8](https://flake8.pycqa.org/en/latest/) - `.flake8`
* [Pylint](https://www.pylint.org/) - `.python-lint` -> `.pylintrc`
* [Black](https://black.readthedocs.io/en/stable/) - `.python-black`
* [isort](https://pycqa.github.io/isort/) - `.isort.cfg`
