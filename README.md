# TraceBase

[![Super-Linter](https://github.com/Princeton-LSI-ResearchComputing/tracebase/actions/workflows/superlinter.yml/badge.svg)](https://github.com/marketplace/actions/super-linter)
![TraceBase Tests](https://github.com/Princeton-LSI-ResearchComputing/tracebase/actions/workflows/tracebase-tests.yml/badge.svg)

TraceBase is a data repository and analysis tool for mass-spectrometry data from isotope tracing studies used to
quantify metabolism in vivo.  Lab members submit peak data exported from El-Maven and processed for natural abundance
correction by AccuCor, IsoCor, or Iso-AutoCor along with details about their experiment.  TraceBase computes useful
metrics such as enrichment, normalized labeling, and estimated FCirc (rate of appearance/disappearance), making it
simple to browse, collate, compare, and download the data.

## Installation Instructions

See `INSTALL.md`.

## Usage

See our [online user guide](https://princeton-lsi-researchcomputing.github.io/tracebase/).

## Contributing Guidelines

See `CONTRIBUTING.md`.

## License Information

See `LICENSE.md`.

## Support

- [GitHub Repository](https://github.com/Princeton-LSI-ResearchComputing/tracebase)
- [Feature & Bug Request](https://princeton-university.atlassian.net/wiki/x/DABDGQ)
- [Feedback](https://docs.google.com/forms/d/e/1FAIpQLSdnYe_gvKdoELXexZ9508xO8o59F1WgXcWBNh-_oxYh9WfHPg/viewform?usp=pp_url&entry.1881422913=README.md)

## Technologies Used

- Python
  - See `requirements/common.txt` for dependencies.
- Django
- Postgres
- JavaScript

## Authors & Acknowledgments

See [The TraceBase Team](https://princeton-lsi-researchcomputing.github.io/tracebase/About/Team/).
<!-- TODO: Add citation

## Citing TraceBase

-->
