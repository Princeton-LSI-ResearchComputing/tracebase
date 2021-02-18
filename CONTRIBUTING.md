This document described the basics of how to set up the TraceBase Project Repo in order to start developing/contributing.

# Requirements

## Python

Install the latest Python (version 3.9.1), and make sure it is in your path:

    $ python --version
    Python 3.9.1

## Virtual Environment

Create a virtual environment (from a bash shell) and activate it, for example:

    python3 -m venv tracebaseenv
    source tracebaseenv/bin/activate

Test to make sure that the `python` command now shows your latest python install:

    $ python --version
    Python 3.9.1

## Django

Install Django by bootstrapping pip:

    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python get-pip.py
    python -m pip install Django

## Postgres

Install Postgres via package installer from https://www.postgresql.org.  Be sure to make note of where it installes the `psql` command line utility, so you can add it to your PATH, e.g. if you see:

    Command Line Tools Installation Directory: /Library/PostgreSQL/13

Then, add this to your PATH:

    /Library/PostgreSQL/13/bin

Configuration:

    Username: postgres
    Password: tracebase
    Port: 5432

In the Postgres app interface, you can find where the `postgresql.conf` file is located.  Open it in a text editor and make sure these settings are uncommented & correct:

    client_encoding: 'UTF8'
    default_transaction_isolation: 'read committed'
    log_timezone = 'America/New_York'

Manually create the tracebase database (`tracebasedb`) in postgres:

    psql -U postgres
    > CREATE DATABASE tracebasedb;
    > \q

## psycopg2

Django's Postgres pre-built psycop2 binary dependency (https://pypi.org/project/psycopg2/) can be installed (in your tracebaseenv) via:

    python -m pip install psycopg2-binary

# Verify Installations

Django:

    python
    > import django
    > print(django.get_version())
    3.1.6

Postgres:

    psql -U postgres

# Clone & test the Repo

git clone https://github.com/Princeton-LSI-ResearchComputing/tracebase.git

Check out the desired branch (e.g. master) and cd into the tracebase project.

Set up the project's postgres database:

    python manage.py migrate

Verify you can run the development server.  Run:

    python manage.py runserver

Then go to this site in your web browser:

    http://127.0.0.1:8000/

