# TraceBase Install

## Introduction and System Requirements

### Overview

TraceBase is a data repository and analysis tool for mass-spectrometry data from isotope tracing studies used to
quantify metabolism in vivo.  Lab members submit peak data exported from El-Maven and processed for natural abundance
correction by AccuCor, IsoCor, or Iso-AutoCor along with details about their experiment.  TraceBase computes useful
metrics such as enrichment, normalized labeling, and estimated FCirc (rate of appearance/disappearance), making it
simple to browse, collate, compare, and download the data.

This document will walk you through setting up your own private instance of TraceBase for the private use of an entire
metabolomics lab that does tracing experiments.  This document is for installation and configuration only.  Maintaining
a TraceBase instance (e.g. loading data) is covered in MAINTENANCE.md.  For a local development version of TraceBase
installed on a workstation (if you want to try it out before going through this for rigorous setup), see out
`CONTRIBUTING.md` document.

### Target Audience

This document is written for system administrators and/or developers.

### Prerequisites

- Minimum Hardware Recommendations:
  - CPUs: 1 (2 recommended) <!-- TODO: What speed is tb? -->
  - RAM: 32G (6G swap on /swapfile - Configure after OS install - adjust as needed - See /etc/fstab)
  - Disk Space: 60G (Default size on install for /boot)
    - 2G swap
    - 19G /var
    - 39G /
- Software Requirements:
  - Operating system: RHEL version `9`
  - Database: PostgreSQL version `13`
  - Web server: Apache version <!-- TODO: version? -->
  - Language: [Python version `3.10`](https://www.python.org/downloads/)
  - Package manager: pip version `25.3`

## Installation

### Environment Setup

Ensure you are using Python 3.10, e.g.:

    python3 --version
    # Python 3.10.11

Create a virtual environment (from a bash shell) in `/usr/local` and activate it, for example:

    python3 -m venv /usr/local/tracebase
    source /usr/local/tracebase/bin/activate

### Create a `tracebase` User Account

Create a `tracebase` user account that we will use to install TraceBase.

<!-- TODO: permissions -->

### Installing System Dependencies

#### Apache Installation

<!-- TODO: Apache Installation Instructions -->

#### Postgres Installation

Install Postgres via package installer from [https://www.postgresql.org](https://www.postgresql.org).

During installation, use these settings for a postgres user for admin privileges:

    Username: postgres
    Password: ########
    Port: 5432

Be sure to make note of where the `psql` command-line utility gets installed and add this to the `tracebase` user's
PATH, e.g. if the utility is located in `/usr/pgsql-13/bin/`:

    export PATH="/usr/pgsql-13/bin/:$PATH"

and add it to the `tracebase` user's `.bashrc`.

### Installing Application Dependencies

As the `tracebase` user:

    sudo -iu tracebase

Download the latest stable release of TraceBase from:

- [TraceBase Releases](https://github.com/Princeton-LSI-ResearchComputing/tracebase/releases)

Save the file in:

    /var/www/

Decompress the download.  E.g., if you download the tarballed gzip file:

    tar -zxvf tracebase-v1.0.2.tar.gz

Install dependencies for the production environment (`requirements/prod.txt`).

    cd /var/www/tracebase
    python -m pip install -U pip
    python -m pip install -r requirements/prod.txt

Verify the installation by checking the Django version:

    python3 -m django --version
    4.2.29

## Configuration

### Postgres Setup

Set these settings in the `postgresql.conf` file.  Open it in a text editor and make sure these settings are uncommented
& correct.  E.g.:

    client_encoding: 'UTF8'
    default_transaction_isolation: 'read committed'
    log_timezone = 'America/New_York'
    shared_buffers = 6GB
    work_mem = 60MB
    maintenance_work_mem = 1GB
    effective_cache_size = 6GB

Manually create the tracebase database (`tracebase`) in postgres:

    createdb -U postgres tracebase

Create a tracebase postgres user:

    > create user tracebase with encrypted password '########';
    > ALTER USER tracebase CREATEDB;
    > grant all privileges on database tracebase to tracebase;

See **TraceBase Setup** below for adding these `tracebase` user credentials to the `TraceBase/.env` file.

### TraceBase Setup

Ensure you are the `tracebase` user, the environment is activated, and that you are in the repository directory:

    sudo -iu tracebase
    source /usr/local/tracebase/bin/activate
    cd /var/www/tracebase

Create a TraceBase environment file using the example file in the repository:

    cp TraceBase/.env.example TraceBase/.env

Create a secret token for secure API access.  This will be saved in the `TraceBase/.env` file.

    python -c "import secrets; print(secrets.token_urlsafe())"

Update the `TraceBase/.env` file to:

- Add the new secret key that was just generated above.
- Add the `tracebase` user database credentials you used in the **Postgres Setup** section.
- Set `DEBUG` to `False`
- Set the archive location (must match the `alias` in the **Apache Setup** section).

#### TraceBase Database Migration

Set up the project's postgres database:

    python manage.py migrate
    python manage.py createcachetable

### Static and Media Files

Explanation of how Django handles static files in production.
Steps to run python manage.py collectstatic.
Configuration for serving static and media files efficiently (e.g., via Apache or a CDN).

## Web Server Configuration

### Apache Setup

<!-- TODO: Apache config instructions -->

Apache config is in `/etc/httpd/conf.d/tracebase.conf`

- Create an alias for the archive, which should be independent of any other tracebase instances (if you intend to run a
  public instance for sharing data).
- Be sure that the ARCHIVE_DIR variable in `/var/www/tracebase/TraceBase/.env` matches the alias in
  `/etc/httpd/conf.d/tracebase.conf`.

<!-- TODO: Description of how to allow access to archive and static files in the apache config. -->

### Domain and SSL

<!-- TODO: Guidance on configuring a domain name & setting up SSL/HTTPS using tools like Let's Encrypt for security. -->

## Authorization and Security

### User Management

Create a superuser for admin access:

    python manage.py createsuperuser

Supply your desired account credentials for testing.

### Security

<!-- TODO: Documentation for enable HTTPS and set appropriate security headers. -->

## Post-Installation

### Verification

Running the test suite will verify everything is installed correctly.  Note, this can take up to 20 minutes:

    python manage.py test

You can check your environment to ensure it is set up securely using the following command:

    python manage.py check --deploy

### Backups

<!-- TODO: Add Fan's backup script to the repo, (or create a separate gist?)
Database backup cronjob scheduled for postgres user:
   00 05 * * * /var/lib/pgsql/dbadmin/tracebase_dump.sh |/bin/mail -s "dev:tracebase_dump.sh" fkang@princeton.edu
Database dump files:
   two copies of dump files are kept ( sudo into postgres for access):
   generated on current date: ~postgres/dbadmin/dev.tracebase.dump.sql
   generated a day before current date: ~postgres/dev.tracebase.dump.sql.old
README_test_db_restore_from_dump
   ~postgres/dbadmin/test_db_restore/README_test_db_restore_from_dump
-->

### Updates

<!-- TODO: Add instructions for updating from the nplcadmindocs. -->

### Load Supporting Data

<!-- TODO: Describe how to load compounds, tissues (if mouse), DataTypes, DataFormats, and LCMethods.
           This will require access to some supporting files. -->
