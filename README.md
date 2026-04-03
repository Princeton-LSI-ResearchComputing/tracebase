# TraceBase

[![Super-Linter](https://github.com/Princeton-LSI-ResearchComputing/tracebase/actions/workflows/superlinter.yml/badge.svg)](https://github.com/marketplace/actions/super-linter)
![TraceBase Tests](https://github.com/Princeton-LSI-ResearchComputing/tracebase/actions/workflows/tracebase-tests.yml/badge.svg)

TraceBase is a data repository and analysis tool for mass-spectrometry data from isotope tracing studies used to
quantify metabolism in vivo.  Lab members submit peak data exported from El-Maven and processed for natural abundance
correction by AccuCor, IsoCor, or Iso-AutoCor along with details about their experiment.  TraceBase computes useful
metrics such as enrichment, normalized labeling, and estimated FCirc (rate of appearance/disappearance), making it
simple to browse, collate, compare, and download the data.

## Directory Structure

- `DataRepo` - Django App Folder for the TraceBase Database Interface: This is the main application package.
- `TraceBase` - Django Project Folder: Contains core configuration files.
- `docs` - Documentation by user type.
- `requirements` - Python dependencies by deployment type.
- `static` - Media and JavaScript files.
- `.github` - GitHub settings.

## Installation

See `INSTALL.md`.

## Testing

In brief, the main test package can be run using:

    python manage.py test

For detailed and comprehensive testing instructions and verification, see:

- The **Verification** section of `INSTALL.md`.
- The **Testing** section of `CONTRIBUTING.md`.

## Usage

- Using the site.
  - See our [online user guide](https://princeton-lsi-researchcomputing.github.io/tracebase/).
  - Mirrored in `/docs/user/`.
- Loading Study data.
  - See `docs/curator/Loading.md`.

## Contributing

See `CONTRIBUTING.md`.  Helpful tips can be found in `docs/contributor/Development_Notes.md`.

## License

See `LICENSE.md`.

## Support

- [GitHub Repository](https://github.com/Princeton-LSI-ResearchComputing/tracebase)
- [Feature & Bug Request](https://princeton-university.atlassian.net/wiki/x/DABDGQ)
- [Feedback](https://docs.google.com/forms/d/e/1FAIpQLSdnYe_gvKdoELXexZ9508xO8o59F1WgXcWBNh-_oxYh9WfHPg/viewform?usp=pp_url&entry.1881422913=README.md)

## Technology

- Python
  - See `requirements/*` for dependencies.
- Django
- Postgres
- JavaScript

## Authors & Acknowledgments

See [The TraceBase Team](https://princeton-lsi-researchcomputing.github.io/tracebase/About/Team/).

## Citing

Citation coming soon.
