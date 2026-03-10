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
installed on a workstation (if you want to try it out before going through this for rigorous setup), see our
`CONTRIBUTING.md` document.
<!-- TODO: Create MAINTENANCE.md -->
### Target Audience

This document is written for system administrators and developers familiar with setting up users, authorization, web
servers, and databases.

### Prerequisites

- Minimum Hardware Recommendations:
  - CPUs: 1 (2 recommended) <!-- TODO: What speed is tb? -->
  - RAM: 32G (6G swap on /swapfile - Configure after OS install - adjust as needed - See /etc/fstab)
  - Disk Space: 60G (Default size on install for /boot)
    - 2G swap
    - 19G /var
    - 39G /
  - A mount for the archive whose size corresponds to the lab's current rate of mzXML raw file accumulation is advised.
- Software Requirements:
  - Operating system: RHEL version `9`
  - Database: PostgreSQL version `13`
  - Web server: Apache version `2.4.62` <!-- TODO: Can this be just `2.4`? -->
  - Language: [Python version `3.10`](https://www.python.org/downloads/)
  - Package manager: pip version `25.3`

## Installation

### Environment Setup

Ensure you are using Python 3.10, e.g.:

    python3 --version
    # Python 3.10.11

Create a virtual environment (from a bash shell) in `/usr/local` and activate it, for example:

    python3 -m venv /usr/local/tracebase <!-- TODO: Is this correct? -->
    source /usr/local/tracebase/bin/activate

### Create a `tracebase` User Account

Create a `tracebase` user account that we will use to install TraceBase.

<!-- TODO: What permissions should we advise? Should we give explicit account creation guidance here? -->

### Installing System Dependencies

#### Apache Installation

<!-- TODO: Apache Installation Instructions? `sudo dnf install httpd`? How to set it up to auto-start? -->
<!-- TODO: Is there a guide we can just link to? -->

#### Postgres Installation
<!-- TODO: Is this correct? -->
Install Postgres via package installer from [https://www.postgresql.org](https://www.postgresql.org).

During installation, use these settings for a postgres user for admin privileges:

    Username: postgres
    Password: ########
    Port: 5432

Be sure to make note of where the `psql` command-line utility gets installed and add this to the `tracebase` user's
PATH, e.g. if the utility is located in `/usr/pgsql-13/bin/`:

    export PATH="/usr/pgsql-13/bin/:$PATH"

and add it to the `tracebase` user's `.bashrc`.
<!-- TODO: Should this describe adding to the .bashrc file? -->
### Installing Application Dependencies

As the `tracebase` user:

    sudo -iu tracebase

Download the latest stable release of TraceBase from:

- [TraceBase Releases](https://github.com/Princeton-LSI-ResearchComputing/tracebase/releases)
<!-- TODO: Create a downloadable release in the github repo -->
Save the file in:

    /var/www/

Decompress the download.  E.g., if you download the tarballed gzip file:

    tar -zxvf tracebase-v1.0.2.tar.gz

Install dependencies for the production environment (`requirements/prod.txt`).

    cd /var/www/tracebase
    python -m pip install -U pip
    python -m pip install -r requirements/prod.txt

## Configuration

### Postgres Setup

Set these settings in the `postgresql.conf` file.  Open it in a text editor and make sure these settings are uncommented
& correct.  E.g.:

- `client_encoding` = `UTF8`
- `default_transaction_isolation` = `read committed`
- `log_timezone` = `America/New_York`
- `shared_buffers` = 6GB
- `work_mem` = 60MB
- `maintenance_work_mem` = 1GB
- `effective_cache_size` = 6GB

Manually create the database (`tracebase`) in postgres:

    createdb -U postgres tracebase

Create a tracebase postgres user:

    sudo -iu postgres
    psql
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

- Add the new `SECRET_KEY` that was just generated above.
- Add the `tracebase` user database credentials you used in the **Postgres Setup** section.
- Set `DEBUG` to `False`
- Set the `ARCHIVE` location (must match the `alias` in the **Apache Setup** section).

If `READONLY` is set to `False`, the following `SUBMISSION` environment variables must be defined and a shared drive
(not managed by TraceBase) must be set up for lab members to deposite their submission data.  This is where
administrators will go to retrieve data for loading.  The environment variables are for your own internal documentation
for drive access.

- `SUBMISSION_DRIVE_DOC_URL` - A URL to documentation about access to the shared drive where submissions are deposited.
- `SUBMISSION_DRIVE_TYPE` - This is a display name for the drive, e.g. "MS Data Shre", for display of the doc URL.
- `SUBMISSION_DRIVE_FOLDER` - This is a demonstrative path in the shared drive showing where to deposit submissions,
  e.g. `\\gen-iota-cifs\msdata\tracebase-submissions`.

#### TraceBase Database Migration

Set up the project's postgres database:

    python manage.py migrate
    python manage.py createcachetable

### Static and Media Files

TraceBase has a single `static` directory, containing:

- JavaScript
- CSS
- Images
- favicon.ico

It also serves files from the administrator-selected archive location, which should be set up outside the `tracebase`
code repository directory and configured with the `ARCHIVE` variable in the `TraceBase/.env` file.

The webserver needs to be set up to allow file access to both directories.

Both locations need to be configured as aliases in the webserver.  See **Apache Setup** below.

## Web Server Configuration

### Apache Setup

<!-- TODO: Apache config instructions?? -->

Apache config is in `/etc/httpd/conf.d/tracebase.conf`

- Create an alias for the archive, which should be independent of any other tracebase instances (if you intend to run a
  public instance for sharing data).
- Be sure that the ARCHIVE_DIR variable in `/var/www/tracebase/TraceBase/.env` matches the alias in
  `/etc/httpd/conf.d/tracebase.conf`.
- Set the gateway timeout to match what's in the `TraceBase/.env` file.  This allows the software to end gracefully if
  submission processing takes too long.
- Create an `alias` to match the `ARCHIVE` location in the `TraceBase/.env` file to enable archive file downloads.
- Create an `alias` to match the `/var/www/tracebase/static` directory.

<!-- TODO: Description of how to allow access to archive and static files in the apache config. -->

### Domain and SSL

<!-- TODO: Guidance on configuring a domain name & setting up SSL/HTTPS using tools like Let's Encrypt for security. -->

## Authorization and Security

### User Management

Add superusers for admin access.  This allows select lab users to login to the admin page linked at the top right of
each TraceBase page.  This is a limited interface that is yet to be fully featured and contains currently, only the
ability to edit Compound records.

You can create multiple admin users, each with this command, which will prompt for a username, email, and password:

    python manage.py createsuperuser

<!-- TODO: Add a section that talks about authenticating users -0 which is not supported by the codebase, and how -->
<!-- TODO: to add/remove users using `/var/www/tracebase/groups.txt`. -->

### Security

<!-- TODO: Documentation for enable HTTPS and set appropriate security headers. -->

## Post-Installation

### Verification

Verify the installation by checking the Django version:

    python3 -m django --version
    4.2.29

You can check your environment to ensure it is set up securely using the following command:

    python manage.py check --deploy

Running the test suite will verify everything is installed correctly.  Note, this can take up to 20 minutes:

    python manage.py test

### Backups

<!-- TODO: Add Fan's backup script to the repo, (or create a separate gist?).  This is from nplcadmindocs:
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

1. Log into the tracebase server, sudo to the tracebase user, and go to the www directory.

        sudo -iu tracebase
        cd /var/www/

2. As tracebase user, download, decompress, and replace the tracebase directory, copying in the `.env` file.  (This
   assumes you have not modified the TraceBase codebase and that the archive is not under `/var/www/tracebase`.)

        mv tracebase tracebase-old
        tar -zxvf tracebase-vX.X.X.tar.gz
        cp tracebase-old/TraceBase/.env tracebase/TraceBase/

3. Update the virtual environment.

        python -m pip install -U pip
        python -m pip install -r requirements/prod.txt

4. Update the database.

        python manage.py migrate

5. Update for new or deleted environment variables in `TraceBase/.env` by comparing it with `TraceBase/.env.example`.

        diff --side-by-side TraceBase/.env TraceBase/.env.example
        vi TraceBase/.env

8. Check the deployment for security issues.

        python manage.py check --deploy

9. Restart the web server.

        exit  # logout of sudo tracebase to your user account
        sudo apachectl graceful

### Load Supporting Data

TraceBase has supported data types and data formats that are required to be loaded from fixtures to support ArchiveFile
records.  This data must be loaded for TraceBase to work:

    python manage.py loaddata data_types data_formats

An optional fixture to load is the liquid chromatography protocols.  Users can define their own protocols as a part of
the study submission process, so these protocols are optional to load:

    python manage.py loaddata lc_methods

All available fixtures are located in `DataRepo/fixtures`.

<!-- TODO: Describe how to load compounds, tissues (if mouse), DataTypes, DataFormats, and LCMethods.
           This will require access to some supporting files. -->
