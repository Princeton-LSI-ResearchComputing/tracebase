<!-- markdownlint-disable no-duplicate-heading -->
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v3.1.5-beta] - 2025-05-15

### Fixed

- Fixed 500 server errors
  - Fixed an internal server error resulting from attempts to access field attributes before checking that the field was not null.
  - Fixed Mzxml download server errors when there were no mzXML files in the search results.
  - Fixed a bug that caused ordering advanced search results by some columns to encounter a 500 server error.
  - Fixed a minor JavaScript bug in the hierarchical formsets that prevented the FCirc comparator select list from having a default selection, which upon submission (if not manually selected), would cause a 500 server error.
  - Fixed a validation/loading bug where when the Label Positions column in the Tracers sheet contained only a single digit, an exception would be raised due to a type error.
  - Gracefully handled a tracer name parsing error that was causing a 500 server error when the label count was missing during validation.
  - Changed 500 error in read-only mode to a 403 forbidden error
- Fixed loading bugs
  - MaintainedModel
    - Fixed a ValueError by adding a try/except block that infers that when there is no existing link from a reverse relation, there is no need to propagate changes through it, so it is skipped.
    - Fixed an issue in get_all_maintained_field_values where it was supplying too many arguments to values_list by breaking it up into a loop to call one at a time.  This was the first time it had ever received multiple values to retrieve (due to recently added maintained fields).
  - Fixed a type-checking error when column headers are custom.
  - Fixed an issue where missing blanks (which should have been ignored) were preventing the MSRunsLoader from proceeding, despite there being no errors.
- Fixed validation page bugs
  - Fixed minor MissingSamples bug that caused the originating column to not be identified, due to an inability to map database foreign key fields from IntegrityErrors to the column in the study doc.
- Added a graceful paginator class to not cause a 500 server error when the requested page number is out of bounds.

### Added

- Added label_combo fields to models: Animal through Tracer.
- Added a number of convenient database utilities for handling database fields, their paths, their relationship type, and general field type (number, string, and foreign key).
- Dependencies
  - Added a package to the dev requirements that allows pylint to be more compatible with django, specifically when running it on the command-line.
  - Added the django debug toolbar as a dev dependency
  - Added an init file for pylint.
    - Upped the pylint dependency due to an exception regarding init files that is gracefully handled in the newer versions and easier to figure out.
- Added groundwork code for improving list view performance, (unfinished).

### Changed

- Load/validation error handling improvements
  - General Exception improvements
    - Removed the database field name from RequiredColumnValue errors, as it is irrelevant/cryptic to the end user.
    - Added summarizable exception classes:
      - UnmatchedBlankMzXML
      - UnmatchedMzXML
      - AssumedMzxmlSampleMatch
    - Added these summarizer exception classes:
      - UnmatchedBlankMzXMLs
      - UnmatchedMzXMLs
      - AssumedMzxmlSampleMatches
    - Also added a summarization class for UnexpectedLabels: AllUnexpectedLabels
    - Exception wording improvements.
  - AggregatedErrors
    - Added the ability to handle errors and warnings separately so that the important issue can be more easily identified.
    - Changed a number of methods to be able to match the error/warning state, and report them separately
  - TableLoader
    - Added the ability to handle errors and warnings separately so that a blocking data issue can be more easily identified.
  - MSRunsLoader
    - Added the ability to distinguish between RecordDoesNotExist and MultipleRecordsReturned exceptions to clarify the reason for a "missing sample"
    - Changed missing sample errors related to unmatched mzXML files into mzXML-specific errors to add clarity to why a sample is "missing".  This adds scan label context to distinguish apparently identical missing sample errors.
    - Improved the ability to identify mzXMLs for likely blank samples.
  - PeakAnnotationsLoader
    - Renamed UnexpectedLabels to UnexpectedLabel
    - Renamed AllUnexpectedLabels to UnexpectedLabels
    - Created AllUnexpectedLabels (to summarize unexpected labels across files).
  - StudyLoader
    - Made UnexpectedSamples a warning if all unexpected samples are skipped.
    - Added a compilation of AllUnexpectedLabels with the load key "ContaminationCheck" to the StudyLoader.
    - Added the ability to catch and report errors from pandas' file reading.
- Database backend improvements
  - Added cache warnings when cache operations fail and fallback to just calling the method each time
    - This happens when the default value for a cached field is None.
    - This allows developers to be able to identify performance issues due to specific data problems.
  - Mitigated concurrent validation database row locking by changing the database isolation level to SERIALIZABLE.  Notes:
    - 2 concurrent validations can still block one another if they are touching the same record.
    - This change makes it less likely to encounter a gateway timeout during validation by limiting the opportunities that 2 validations try to modify the same data to only those existing at the start of a validation (as opposed to them arising concurrently).
    - There is a plan in place to queue all validation jobs to run serially to prevent gateway timeouts that arise from row locking.
  - Made some model record orderings case insensitive.
  - Maintained field improvements (autoupdate behavior changes)
    - Made lazy autoupdates lazier, improving loading performance by not triggering caching updates.
    - Improved the autoupdate message to differentiate between changed and unchanged values.
    - Prevented auto-update propagation if the source of the save trigger was a select.
- Improved security settings.
- Testing improvements
  - Improved the type hinting for the test case class factory
  - Made an assertNotWarns decorator in the tracebase test case classes.
  - Created a simple way to create test models on the fly.
- Loading improvements
  - Improved matching of mzXMLs that start with a number to a sample name.
  - Passed the --debug flag to the peak annotations loader.
  - PeakAnnotationsLoader
    - Made the peak annotation file format detection more robust by replacing static checks into a loop based on existing subclasses.
    - Added the ability to remove the compound version numbers that ELMaven appends to compounds sometimes.

## [v3.1.4-beta] - 2025-02-04

### Fixed

- Fixed an issue where multiple representation errors (attributing the same source peak annotation file) were mistakenly raised due to an unrelated error.
- Eliminated a pandas warning.
- Fixed an issue where error summaries were being added to other error summaries.
- Fixed an uncaught exception in the CompoundsLoader
- Added missing init files among the test directories and fixed stale tests that now get run as a result.
- Fixed some infusate/tracer name delimiters to use variables the user sets instead of static strings.

### Added

- Added a `load_study_set` command.  Compared to the legacy version, the following changes were made:
  - The input file is a listing of study docs.
  - Removed buffering of autoupdates so that each loaded study is complete.
  - Added the ability to skip commented studies.
- A PermissionError is now raised on the build a submission page if the site is READONLY.
- Added a `--debug` option to all load commands to print all exception traces.
- Added option `--exclude-sheets` to `load_study`.
- Added security settings based on `python manage.py check --deploy` output.
- Added methods to `tracebase_test_case` to generate missing test stubs.
- Added a suggestion for `DuplicateValues` exceptions from the animals loader to suggest that study names be delimited on 1 row.

### Changed

- Removed the legacy loading code
  - Removed all references to the legacy loaders
  - Converted tests and test data to the new loading code
  - Removed all orphaned code resulting from the removed loading code
- Updated loading code for the new logic regarding MSRunSample records
  - When there are multiple MSRunSample records a placeholder is used to link PeakGroups.
  - Otherwise, PeakGroup records link to the MSRunSample record containing the raw data.
- Made the check for requiring sequence defaults to be supplied to the MSRunsLoader more robust.
- Bumped django to 4.2.18
- Changes to behavior due to missing C12 PARENT rows in accucor files
  - Changed the check for missing C12 PARENT rows to a warning, since it can be missing due to the MS signal being below the detection threshold.
  - Changed the formula fill down strategy to be more efficient when the C12 PARENT row is missing.
  - Updated the suggested remedy in the warning.
  - Added to fill in missing formulas using a new "BACKFILL" placeholder.
- Made `AggregatedErrorsSet` compatible with `AggregatedErrors` such that they have the same interface so that `load_table` could handle both exception classes.
- Modified archive cleanup message when nothing was there to clean up in the `MSRunsLoader`.
- Organizational improvements to the test code.
- Moved the leaderboard (and other) code into the `Researcher` class as class methods.

## [v3.1.3-beta] - 2025-01-13

### Fixed

- Overlooked errors (related to neighboring loader method calls) are now reported in the validation interface.
- Set the database "isolation_level" to REPEATABLE_READ to prevent random timeouts of the validation interface during an active load.
- Fixed errors about mismatching infusates due to mismatches between tracer concentrations with and without significant figures applied.
- Fixed a bug where formulas in accucor files were not getting filled down during universal format conversion.
- Fixed an edge-case bug that prevented loading of an accucor corrected sheet in csv format.
- Fixed various uncaught exceptions on the submission interface.
- Fixed minor issues with `ArchiveFile` record creation.
- Fixed a bug where the `Peak Group Conflicts` sheet was not being checked for missing required values.

### Added

- Missing PARENT C12 rows in peak annotation files are now detected and reported as errors.
- Added the ability to skip sheet loads using `--exclude-sheets` in `load_study`
- Defaulted the study directory to the enclosing directory of the study doc, and applied that to mzXML and peak annotation files.
- Added explicit support for finding files from the study doc by looking in the study directory, current directory, or by absolute path.
- Added a suggestion to the `UnexpectedLabels` error to make it clearer how to solve the problem.
- Added an environment variable (READONLY) that controls whether the site is public or not (to disable uploading and editing).
- We now allow animals to have no infusate.
- All "None" values now universally display as "None" in the advanced search results.
- Added a check of whether an mzXML has been explicitly skipped when checking if leftover mzXMLs exist.
- Added a check to catch mzXML file related errors and exit quickly (since loading them is slow).
- Added the ability to associate mzXML files containg dashes in their name with corresponding sample headers with underscores.
- Added the ability to associate mzXML filenames, starting with a number, with corresponding sample headers that otherwise uniquely match.
- Added custom errors/warnings related to mzXML file issues.
- Added summarized errors/warnings for errors and warnings that tend to recur.
- Added status prints during slow mzXML loading operations.
- Added a time limit to the computation of advanced search results stats that truncates the stats if it is close to timing out.
- Added the ability to check for existing values in a study doc sheet before autofilling (which was a feature previously limited to the submission start page).
- Added the ability to correctly check matching previously loaded values versus input values where Excel incorrectly inferred the type.
  - Added the selected annotation file column to the unique column constraint in order to issue more precise errors about duplicate differing selections.

### Changed

- Improved validation errors/warnings.
- Updated the CONTRIBUTING.md doc and updated the example datasets to work with version 3.
  - Changed study, animal, and sample names to prepend "demo_" so that nothing conflicts with actual studies.
  - Updated common records to not conflict with published data.
  - Added details about linting tools.
  - Updated the django version referenced in the doc.
  - Added a migration check command in the section that talks about requirements for merging a PR.
- Removed "FIXED" message added to errors if none of the missing records are autofilled.
- Mitigated a cascade of validation errors stemming from a manually mismatched infusate entered into the animals sheet.
- Removed reference to the legacy scripts from load_study and load_samples.

## [v3.1.2-beta] - 2024-11-13

### Fixed

- Advanced search download form errors now gracefully display an error.

### Added

- Added an `mzXMLs` download button to the advanced search results when the results displayed have an MZ file column.
- Added `textlint` to the contributing doc.

### Changed

- Changed the advanced search download button to additionally contain "TSV" to distinguish it from the mzXMLs download button.

## [v3.1.1-beta] - 2024-11-13

### Fixed

- Fixed the ability to load accucor corrected data when unaccompanied by original data (i.e. support for csv/tsv accucor data).
- Fixed a copy/paste bug where RAW data was being loaded as corrected data.
- Fixed an old undiscovered bug in the isoautocorr support was using the "cor_pct" sheet instead of the "cor_abs" sheet.
- Fixed the display of peak annotation filenames in the advanced search results.
- Fixed a 500 server error when trying to sort advanced search results using the labeled element.
- Fixed an isotopeLabel parsing error that was skipping the load of C12 PARENT data when dual labeled data was missing labels of one or more elements.
- Fixed a bug that prevented loading and template creation of peak annotation files in csv/tsv format.
- Fixed a bug that prevented the load of animal data when it was missing the optional age data.
- Fixed a bug that was calling empty rows as duplicate rows.
- Fixed the skipping of empty rows in the peak annotation files loader.

### Added

- New submission start page features
  - Added the ability for the user to specify the mass spec operator, run date, LC protocol, and instrument for each peak annotation file supplied on the submission start page.
    - Added autocomplete to each of the sequence fields.
    - Metadata filled in next to the drop-area gets copied for each dropped file.
    - Added the ability to autofill the sequence metadata into the 'Sequences', 'Peak Annotation Files', and 'Peak Annotation Details' sheets.
  - Created a drop-area for peak annotation files on the submission start page.
  - Added the ability to detect and report multiple compound representations.
  - Added a 'Peak Group Conflicts' sheet to the study doc download, to allow users to select a peak annotation file for each measurement of the same compound for a set of samples, so that there is only one measurement for a compound and sample that is loaded without error.  The sheet is hidden if there are no conflicts.  The sheet includes the following columns:
    - Peak Group Conflict (for the compound [synonym])
    - Selected Peak Annotation File (with a dropdown of the files containing the compound and common samples)
    - Common Sample Count (the number of common samples between the files in the above-described dropdown)
    - Example Samples
    - Common Samples (a hidden column with delimited sample names)
  - Added the ability to disable and enable the submit button to help clarify that files must be added to the form.
- Added features to the loading scripts to account for peak group conflicts.
  - Added the ability to skip peak groups from peak annotation files that were unselected in the 'Peak Group Conflicts' sheet.
  - Added exception classes to explain related errors.
  - Added the ability to delete previously loaded peak groups when new peak annotation files create a conflict.
- Added an --mzxml-dir option to the load_study and load_msruns command-line scripts.
  - The directory is walked to find files with an `mzXML` extension.
  - mzXML files are associated with peak annotation files based on whether the peak annotation file is in a directory on the path to the mzXML file.
  - Added the ability to skip mzXML files containing 0 scans.
  - Made it possible to run `load_msruns` with only the --mzxml-dir option.
- Added the ability to extract compound and sample data even when the peak annotation format could not be precisely determined (between isocorr and isoautocorr).
- Added all mzXML files associated with a sample, along with polarity and scan range, to the advanced search results (Peak Groups and Peak Data formats).
- Added a bulk mzXML download button on the advanced search interface.
  - The download is a streamed ZIP archive.
  - The download includes a metadata file.
  - Files are organized into subdirectories.
- Added the ability to check for missing C12 PARENT rows in peak annotation data (which addressed a bug that was filling in the wrong formula in some accucor files).
- Added the detection (and reporting) of errors in loaders called from StudyLoader in both the validation interface and `load_study`.
- Added support for alternate AccuCor labeled elements (N and D).

### Changed

- Improved the accuracy of some count stats in the loaders.
- Improved various column header comments (making repeated similar comments automated).
- Renamed some columns for clarity.
- Changed the way MSRunSample records are linked to by PeakGroup records to simplify the logic for MSRunSample record decisions based on whether they do or do not contain mzXML file ArchiveFile record links.
  - Now, PeakGroup records are always linked to MSRunSample records *without* mzXML file links if there are multiple mzXML files for a sample and sequence (to pave the way toward the old design of each MSRunSample record linking to a "bucket of files" for any sample/sequence combination).
- Improved peak annotation file format identification.
- Allowed required missing values to be allowed to be missing when the skip column (in the Peak Annotation Details' sheet has a value.
- Removed the polarity column from the advanced search results formats.
- Removed the ability to sort by mzXML filenames.
- Made duplicate sample errors based on peak annotation headers into a warning (to account for sample headers that are the same between multiple peak annotation files).

## [v3.1.0-beta] - 2024-09-12

### Fixed

- Included consideration for the variability of the AccuCor 'adductName' column.
- Fixes for uncaught exceptions about study doc versions and for referenced infusates from the animals sheet/loader.
  - Replaced uncaught study doc version exceptions with graceful exceptions that include a hint of which version the submitted version is closest to.
  - Fixed an issue where finding the infusate using the infusate name fails when the infusate does not yet exist in the database.
- Worked around an IsoCorr quirk that changes dashes in sample name headers to underscores, causing mismatches with the mzXML filenames.
- Improved submission start page performance to prevent timeouts.
- Added missing environment variables to the example env file.
- Ensured that the peakgroup name is composed of the supplied compound synonyms instead of the compound primary name so-as to support qualitative differences in the compounds synonyms describe (e.g. stereoisomers) (which was incorrectly changed before).
- Addressed infusate/tracer consistency check issue associated with spaces between isotopes and added case insensitivity.
- Fixed the clean-up of archive files when the load fails.
- Fixed all of the column overrun issues in the templates.
- Fixed the 200 row download limit issue.
- Clarified confusing "labeled elements" columns in the templates to represent combinations of elements among the tracers.
- Fixed empty MSRun names in the peak group templates.
- Avoided TypeError due to the mzXML file column not having a value.
- Fixed dry-run mode handling WRT child loaders of StudyLoader.
- Fixed handling of defer_rollback mode.
- Fixed omission of MultiplePeakGroupRepresentations and NoTracerLabeledElements errors by adding an override of PeakGroup.save() to check for multiple representations.

### Added

- Additions to the submission interface.
  - Added autofill functionality for all sheets in the study doc.
  - Added static drop-downs for fields/columns that are enumerated.
  - Added formulas to calculated columns (and shaded those columns).
  - The submission (aka "validation") page now wraps calls to the new loading classes.
  - Added the ability to allow csv and tsv files in the submission form.
  - Added backward compatibility support for version 2 study docs.
  - Added compounds with matching formulas to the Compounds autofill.
- Moved/renamed legacy loader scripts and replaced load_study.py with calls to the new loading classes.
- Scan label handling (e.g. "_pos" and "_neg") additions.
  - Added the ability to remove internal scan labels from sample headers to determine sample name.
  - Added dashes as a alternate scan label delimiter.
- Added aggregation of load stats to StudyLoader.

### Changed

- Removed the code field from the Study model and its loader/sheet.
- Changes to the submission interface.
  - Linked to a new google submission form.
  - Added a tab-bar to the submission page and broke up the submission process into 4 steps:
    - Start (to download a template based on autofill from peak annotation files).
    - Fill In (including instructions on filling out the study doc).
    - Validate (to check the study doc for problems and make minor fixes).
    - Submit (to link researchers to the google form that emails us a new submission exists).
  - Removed the original upload page and instead linked the Upload tab directly to the submission start page.
  - Removed the validation disabled view/template.
- Improved the help_text for PeakGroup.name to be more specific about how it differs from the primary compound name.
- Updated the upload instructions in the docs.
- Many exception summarization and streamlining improvements were made.
- Removed cache retrieval print.

## [v3.0.2-beta] - 2024-07-30

### Fixed

- Caught a situation where the last serum sample can be invalid due to manual data manipulation and added an error status on the FCirc advanced search results page explaining the problem and prescribed fix.

### Added

- A number of load scripts and classes were added behind the scenes, but are not yet employed in actual loading or validation.
  - This includes as-yet unused IsoAutocorr support.
- Added the ability to load mzXML files at any point before, during, or after a study load.

### Changed

- MSRunSample records now enforce only a single placeholder record for any sequence and sample combo.

## [v3.0.1-beta] - 2024-04-08

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

## [v3.0.0-beta] - 2024-03-13

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

## [v2.0.6] - 2023-12-08

### Added

- Added `export_studies` management command. This command exports all of the data for the specified studies which consists of the PeakData, PeakGroup, and FCirc formats.

## [2.0.5] - 2023-10-10

### Changed

- Updated to use Django 4.2

## [v2.0.3] - 2023-07-07

### Added

- Sample table loading now checks in-file sample name uniqueness
- Added the ability for the accucor loading code to deal with infusates with multiple labeled elements when there is only 1 isotopic version of it among the tracers.

### Fixed

- PeakGroups table now displays "None" when erichment fraction or enrichment abundance cannot be calculated. Previously was blank. (Issue #611).

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
  - A loading YAML is now automatically created
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

## [v2.0.2] - 2023-02-10

### Added

- Pages/views
  - Created CSS file "bootstrap_table_cus1.css" to customize table options with Bootstrap-table plugin.
  - Created JavaScript "setTableHeight.js" to set table height dynamically with Bootstrap-table plugin.
- Documentation now in the repository and hosted on GitHub pages at [https://princeton-lsi-researchcomputing.github.io/tracebase/](https://princeton-lsi-researchcomputing.github.io/tracebase/)

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

## [v2.0.1] - 2023-01-05

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
