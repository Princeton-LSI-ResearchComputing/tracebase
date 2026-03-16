# TraceBase Install

## Introduction and System Requirements

### Overview

This document will walk you through setting up your own private instance of TraceBase for the private use of an entire
metabolomics lab that does tracing experiments.  This document is for installation and configuration only.  Maintaining
a TraceBase instance (e.g. loading data) is covered in MAINTENANCE.md.  For a local development version of TraceBase
installed on a workstation (if you want to try it out before going through this for rigorous setup), see our
`CONTRIBUTING.md` document.

### Target Audience

This document is written for system administrators and developers familiar with setting up users, authorization, web
servers, and databases.

### Prerequisites

- Minimum Hardware Recommendations:
  - CPUs: 4 Intel(R) Xeon(R) Gold 6230R CPU @ 2.10GHz (1 can suffice)
  - RAM: 32G (6G swap on /swapfile - Configure after OS install - adjust as needed - See /etc/fstab)
  - Disk Space: 60G (Default size on install for /boot)
    - 2G swap
    - 19G /var
    - 39G /
  - A mount for the archive whose size corresponds to the lab's current rate of mzXML raw file accumulation is advised.
- Software Requirements:
  - Operating system: RHEL version `9`
  - Database: PostgreSQL version `13`
  - Web server: Apache version `2.4.62`
  - Language: [Python version `3.10`](https://www.python.org/downloads/)
  - Package manager: pip version `25.3`

## Installation

### Create a `tracebase` User Account

Create a `tracebase` user account that belongs to a `tracebase` group that we will use to install TraceBase, e.g. using
`useradd`.

### Environment Setup

Ensure you are using Python 3.10, e.g.:

    python3 --version
    # Python 3.10.11

Create a virtual environment (from a bash shell) in `/usr/local` and activate it, for example:

    sudo mkdir /usr/local/tracebase
    sudo chown tracebase:tracebase /usr/local/tracebase

    python3 -m venv /usr/local/tracebase
    source /usr/local/tracebase/bin/activate

### Installing System Dependencies

#### Apache Installation

We installed apache 2.4.62 on RHEL 9 using roughly these commands:

    sudo dnf install httpd -y
    sudo systemctl start httpd
    sudo systemctl enable httpd
    sudo systemctl status httpd
    sudo firewall-cmd --permanent --add-service=http
    sudo firewall-cmd --permanent --add-service=https
    sudo firewall-cmd --reload

#### Postgres Installation

Install [Postgres](https://www.postgresql.org).

We used the `rhel-9-for-x86_64-appstream-rpms` repository and followed the
[4.1. Installing PostgreSQL](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/configuring_and_using_database_servers/using-postgresql_configuring-and-using-database-servers#installing-postgresql_using-postgresql)
installation instructions.

During installation, use these settings for a postgres user for admin privileges:

    Username: postgres
    Password: ########
    Port: 5432

If you use the same strategy, the `psql` command-line utility will be in the PATH.  Just make sure that the `tracebase`
user has it in their PATH.

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

## Configuration

### Postgres Setup

Set these settings in the `/var/lib/pgsql/data/postgresql.conf` file.  Open it in a text editor and make sure these
settings are uncommented & correct.  E.g.:

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

Each lab must have their own way of submitting large amounts of study data (the study doc, peak annotation files, and
mzXML raw files).  The following `SUBMISSION` environment variables must be defined and a shared drive (not managed by
TraceBase) must be set up for lab members to deposite their submission data.  This is where administrators will go to
retrieve data for loading.  The environment variables are for your own internal documentation for drive access.

- `SUBMISSION_DRIVE_DOC_URL` - A URL to documentation about access to the shared drive where submissions are deposited.
- `SUBMISSION_DRIVE_TYPE` - This is a display name for the drive, e.g. "MS Data Shre", for display of the doc URL.
- `SUBMISSION_DRIVE_FOLDER` - This is a demonstrative path in the shared drive showing where to deposit submissions,
  e.g. `\\gen-iota-cifs\msdata\tracebase-submissions`.

NOTE, If `READONLY` is set to `False`, the `SUBMISSION_*` environment variables are not needed, because it disables the
submission interface.

The submission process is described in
[the user documentation](https://princeton-lsi-researchcomputing.github.io/tracebase/Upload/How%20to%20Build%20a%20Submission/4%20-%20How%20to%20Submit%20Data/),
however since each setup is different, the TraceBase
codebase does not provide a study submission form.  You must create one and save it in this environment variable:

- `SUBMISSION_FORM_URL`

You can do so by creating a copy of our
[example google submission form](https://docs.google.com/forms/d/1XBTUwweS0cEhsBoVxgu7aSAUsypeW4xay93jHGnPznk/copy).

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

The following is an example `/etc/httpd/conf.d/tracebase.conf` file:

    <VirtualHost>
        SSLEngine on
        SSLCertificateFile /etc/pki/tls/certs/example-edu.cer
        SSLCertificateKeyFile /etc/pki/tls/private/example-edu.key
        SSLCertificateChainFile /etc/pki/tls/certs/example-edu.cer
        SSLProtocol all -SSLv2 -SSLv3
        ServerName example.edu
        Timeout 180
        ErrorLog logs/tracebase-error_log
        CustomLog logs/tracebase-access_log common
        WSGIDaemonProcess tracebase processes=2 threads=4 display-name=tracebase python-home=/usr/local/tracebase python-path=/var/www/tracebase
        WSGIProcessGroup tracebase
        WSGIScriptAlias / /var/www/tracebase/TraceBase/wsgi.py
        # If you are using CAS for authentication for a private TraceBase instance
        # <Location />
        #     AuthType CAS
        #     CASScope /
        #     Require group tb
        #     AuthGroupFile /var/www/tracebase/groups.txt
        # </Location>
        Alias /static /var/www/tracebase/static
        Alias /favicon.ico /var/www/tracebase/static/favicon.ico
        <Directory /var/www/tracebase/static>
            Require all granted
        </Directory>
        Alias /archive /tracebase-archive/archive
        <Directory /tracebase-archive/archive>
            Require all granted
            Header set Content-disposition attachment
        </Directory>
    </VirtualHost>

Note that it:

- Creates an alias for the archive, which should be independent of any other tracebase instances (if you intend to run a
  public instance for sharing data).
  - Be sure that the ARCHIVE_DIR variable in `/var/www/tracebase/TraceBase/.env` matches the alias.
- Creates an `alias` to match the `/var/www/tracebase/static` directory.
- Sets the gateway timeout to match what's in the `TraceBase/.env` file.  This allows the software to end gracefully if
  submission processing takes too long.

## Authorization and Security

### User Management

#### Public Users

TraceBase does not provide differential public versus private access.  To "publish" any study data, a separate public
instance of TraceBase must be created that must be separately loaded with the studies that have been selected to be
"public"/published.  To create a public instance, follow these installation instructions, but do not apply any
authentication mechanism.  It is also recommended that you set the `READONLY` environment variable in `TraceBase/.env`
to `True`.

NOTE: Retain copies of submitted study docs and all associated data for this purpose.

#### Regular Users (Lab-Only)

User authentication is left for system administrators to work out.  At Princeton, we use CAS authorization and all
that's required is to add it in a Location tag in `/etc/httpd/conf.d/tracebase.conf` as is seen in ghe example above.
We provide access to specific users by adding their usernames to `/var/www/tracebase/groups.txt`.

#### Admin Users (Curators)

Add superusers for admin access.  This allows select lab users to login to the admin page linked at the top right of
each TraceBase page.  This is a limited interface that is yet to be fully featured and contains currently, only the
ability to edit Compound records.

You can create multiple admin users, each with this command, which will prompt for a username, email, and password:

    python manage.py createsuperuser

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

The TraceBase codebase does not provide a backup mechanism out-of-the-box, but we recommend setting up a backup of the
database and the archive files.  We use a cron job to dump the database and backup that file and the archive directory
regularly.

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

Some of the other supporting data, such as compounds, tissues, and animal treatments tend to vary from lap to lab.  Many
researchers have their prefered compound names, for example, so we do not provide fixtures for this data, but compiling
that data can be a time consuming endeavor.  If you would like to get a jump start on this data resource, you can
download that data from [tracebase.princeton.edu](http://tracebase.princeton.edu), format it for the Study doc format,
and put it in a 3-sheet study doc (Compounds, Tissues, and Treatments) named `underlying_data.xlsx` and load it into
your tracebase instance with:

    python manage.py load_study --invile underlying_data.xlsx
