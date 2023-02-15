# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.2] - 2023-02-10

### Added

- Pages/views
  - Created CSS file "bootstrap_table_cus1.css" to customize table options with Bootstrap-table plugin.
  - Created JavaScript "setTableHeight.js" to set table height dynamically with Bootstrap-table plugin.
- Documentation now in the repository and hosted on GitHub pages at
  [https://princeton-lsi-researchcomputing.github.io/tracebase/](https://princeton-lsi-researchcomputing.github.io/tracebase/)

### Changed

- Pages/views
  - Made minor changes to the DataFrame for study list and stats (added total infusates)
  - Modified code for customtag filter "obj_hyperlink" to display list with or without line breaks.
  - Improved display of infusates in study and aninal templates.
  - Changed column width for some columns in study and animal tables.
- Models
  - Changed the value validation for PeakDataLabel and TracerLabel count, mass_number, and positions be based on the actual attributes of the compound/element instead of static arbitrary values
- Advanced Search
  - Added time collected to the PeakData and PeakGroups advanced search formats.
- Loading
  - The sample table loader now generates as many actionable errors as possible in 1 run.
  - If a sample table loader load action requires input involved in a previous error, that action is now skipped.
  - Added verbosity controls to the sample table loader.
  - Sample table loader now raises an error on unknown headers.
  - The accucor loader now generates as many actionable errors as possible in 1 run.
  - If an accucor loader load action requires input involved in a previous error, that action is now skipped.
  - Added verbosity controls to the accucor loader.
  - The accucor loader now raises an error on unknown headers.
  - Errors/warnings and raise/print decisions in both the accucor and sample loaders are now based on the validate mode.
  - Moved `validate_researchers` and `UnknownResearcherError` to `models/researcher.py`, one for addressing circular import issues and the other for re-use/encapsulation.
  - Wrote a bunch of exception classes and moved exception verbiage to the exception classes' init methods.
  - Streamlined and organized the methods in accucor_data_loader so that it's more organized and sensical.
  - Created a `buffer_exception` method and implemented it in the sample_table_loader and accucor_data_loader.  Whenever an exception should stop loading (or stop a particular loop/method), it is raised directly and caught by the calling function and buffered if more can be otherwise accomplished.
  - Replaced the assertion using the debug parameter with a raise/catch of the DryRun exception in both sample and accucor loaders.
  - Streamlined the validation view.

## [2.0.1] - 2023-01-05

### Added

- Advanced Search
  - The last/previous serum sample (peak group) status is now searchable.
  - Animal age and sample time collected are now searchable in weeks, days, hours, or minutes using decimal values.
  - Added a status column to the FCirc page that shows warnings/errors about the validity of the FCirc calculations.
  - Added infusate/tracer fields to advanced search field select lists.
  - Added Tracers, Tracer Compounds, Concentrations, and modified the display of Infusates to the advanced search results and download templates.
- Pages/views
  - Added infusate list page.
  - Add Help menu link to Google Drive folder.

### Changed

- Advanced Search
  - Last serum sample (peak group) determination now falls back to previous serum sample if a tracer compound was not picked in the last serum sample's MSRun.
  - Clicking previous/last FCirc checkboxes now repaginates to always show a constant number of rows per page of results.
  - Advanced search results now link to infusate details pages.
  - Changed displayed isotope naming format to look more similar to the loading template format, but with concentrations included.
  - Fixed overlooked issue with labeled element and count leftover from the multi-tracer/label template update.
  - Fixed an overlooked multi-tracer/label issue with the concentrations in the fcirc tsv download template.
- Pages/views
  - Improved infusate and sample column sorting on detail/list view pages.
  - Minor display and sort bugfixes.
  - Improved pagination performance in Infusate and Sample list/detail view pages.
  - Changes to protocol and compound views and almost all templates for list views.
  - Added dynamic table height to some view pages.
  - Split the protocol list views into two: animal treatment and msrun protocol.
  - Minor page width issue fixed to prevent confusing horizontal scrollbars from appearing.
  - Fixed issues with handling null values in Pandas DataFrames.
  - Improved customtag code to allow better display of infusates/tracers/compounds in list pages.
- Data submission/loading
  - Improved protocol loading (for treatments and MSRun protocols).
  - Updated loading template.
- Dependencies
  - Django updated to 3.2.16.

### Removed

- Data submission/loading
  - Upload data validation page temporarily disabled for improvements.
