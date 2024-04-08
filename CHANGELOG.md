<!-- markdownlint-disable no-duplicate-heading -->
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

### Added

- Build a TraceBase Submission Page
  - Ability to extract sample names from AccuCor/IsoCorr files
  - Ability to strip suffixed like "_pos", "_neg", and "_scan1" from peak annotation file sample headers
  - Added ability to "add" files in the form (since selecting new ones replaced previously selected ones in Windows)
  - Downloaded file has instructions for each column attached to headers as comments
  - Automatically populates existing tissues and treatments
  - Ability to add samples iteratively, to an existing sample sheet/study doc
- Lots of groundwork laid for autofilling lots of data and annotations such as data that has errors associated with it

### Removed

- Obscured references to edge-cases where an extra file is needed when submitting data, to simplify and streamline the process.

## [3.0.0-beta] - 2024-03-13

### Fixed

- Fixed PeakData and PeakGroups links from ArchiveFile detail page

### Added

- Added an ArchiveFile class with associated DataType and DataFormat classes
- LCMethod model and tests
- LCMethod views
- Copy records from PeakGroupSet to ArchiveFile, all records are copied as MS Peak Annotation data in AccuCor format as a default

### Changed

- Updated dependencies
- Use human readable string methods for ArchiveFile, DataType, and DataFormat (affects admin interface, other default templates)
- Made auto-update code thread-safe
- Implemented context managers and decorator wrappers to control autoupdate behaviors
- Display ArchiveFile record associated with PeakGroup instead of PeakGroupSet

### Removed

## [2.0.6] - 2023-12-08

### Added

- Added `export_studies` management command. This command exports all of the
  data for the specified studies which consists of the PeakData, PeakGroup, and
  FCirc formats.

## [2.0.5] - 2023-10-10

### Changed

- Updated to use Django 4.2

## [2.0.3] - 2023-07-07

### Added

- Sample table loading now checks in-file sample name uniqueness
- Added the ability for the accucor loading code to deal with infusates with multiple labeled elements when there is only 1 isotopic version of it among the tracers.

### Fixed

- PeakGroups table now displays "None" when erichment fraction or enrichment
  abundance cannot be calculated. Previously was blank. (Issue #611).

### Changed

- Stripped units in the sample table loader now generate an error if the units are deemed to be incorrect.
- Massive refactor to loading scripts
  - As many errors as possible are buffered and reported en masse at the end
  - Data associated with previous errors are now skipped
  - Unknown headers now cause errors
  - Autoupdates are now deferred to the calling script/method
  - All loading code is now wrapped in atomic transactions
  - Units are now stripped from fields with a warning
  - Repeated exceptions are now consolidated into single exceptions
  - Debug mode loading side effects were eliminated
  - --debug was changed to --dry-run for all loaders for consistency
- Massive refactor to the validation interface
  - load_study (called in --validate mode) is now used for validation
  - Isocorr files now have a separate file field
  - A loading yaml is now automatically created
  - Exceptions are now all now presented in chronological order (errors and warnings)
  - Many exceptions are now multi-indented-lines
- Improvements in loading exceptions
  - Many custom exceptions were added (e.g. SheetMergeError)
  - Exception messages improved to include more data (e.g. field and row number references and valid values where appropriate)
  - Cross-file exception groups were created for the same errors coming from multiple files
  - New MultiLoadStatus exception class was created for communication between the loading code and the validation page
  - Some exceptions now suggest resolutions (e.g. e.g. add the iscorr flag)
  - If all samples are missing, a NoSamplesError is now generated for brevity
  - Sample name uniqueness errors now describe suggested resolutions based on the different resolutions (fudge the date versus prefix the name)
- A couple null=True model changes were made where unsearchable empty strings were being stored
- Example data was updated to adhere to new restrictions (e.g. no unknown headers)
- Documentation updates associated with the loading and validation refactor
- Tissue "blank" is now case insensitive
- Max labaeled atoms is now determined using the formula instead of a static value
- Allow data to be loaded from the same MSRun in multiple accucor/isocor files
- Error when attempting to load duplicate or conflicting PeakGroups from accucor/isocor files

### Removed

- Validation database
- All references to the validation database

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
