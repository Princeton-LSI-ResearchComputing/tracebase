This document described the basics of how to set up the TraceBase Project Repo in order to start developing/contributing.

# Requirements

## Python

Install the latest Python (version 3.9.1), and make sure it is in your path:

    $ python --version
    Python 3.9.1

Test to make sure that the `python` command now shows your latest python install:

    $ python --version
    Python 3.9.1

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

Manually create the tracebase database (`tracebase`) in postgres:

    psql -U postgres
    > CREATE DATABASE tracebase;

Create a tracebase postgres user:

    > create user tracebase with encrypted password 'mypass';
    > grant all privileges on database tracebase to tracebase;

## Clone the repository

    git clone https://github.com/Princeton-LSI-ResearchComputing/tracebase.git
    cd tracebase

## Virtual Environment

Create a virtual environment (from a bash shell) and activate it, for example:

    python3 -m venv .venv
    source .venv/bin/activate

Install Django and psycopg2 dependencies

    python -m pip install -U pip  # Upgrade pip
    python -m pip -r requirements/dev.txt  # Install requirements

## Verify Installations

Django:

    python
    > import django
    > print(django.get_version())
    3.1.6

Postgres:

    psql -U postgres

## Setup TraceBase and run the development server

Set up the project's postgres database:

    python manage.py migrate

Verify you can run the development server.  Run:

    python manage.py runserver

Then go to this site in your web browser:

    http://127.0.0.1:8000/
