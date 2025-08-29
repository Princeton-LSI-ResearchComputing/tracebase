# Errors and Warnings Reference

The process of building a Study Submission happens on The Upload page.  The Upload page has 2 tabs where you can
encounter errors and/or warnings about your study submission: the **Start** and **Validate** tabs.  This documentation
is intended to serve as a reference to help researchers figure out how to address any errors or warnings (collectively:
_exceptions_) that can arise during the study submission building process.

The TraceBase submission interface was built with researchers in mind, to empower users to be able to fix their data on
their own, thereby speeding up the curation and loading process.  It also is intended to give users a sense of ownership
over their own data.

Not every possible exception you can encounter is here.  The excpetions in this documentation represent the debug work
of curators who have figured out the meaning behind common context-lacking cryptic/technical database exceptions that
have been encountered in past submissions and saved that work in the form of custom exceptions that refer to the precise
corresponding input data's file location.  Cryptic exceptions (for previously unencountered issues) can still arise.  If
you see any, leave it for a curator to figure out so that that work can be saved in a new, more easy to understand
exception, so that future users can benefit from that work, should they encounter the same issue.

## How to Use this Exception Lookup Reference

If a custom exception listed here is encountered and its language is difficult to understand or more context is needed,
look up the exception's name on this page to get hints on what the exception means, the context surrounding it, and
potentially, a more in depth suggestion on how to fix the issues in the submission that lead to the error.  If you have
any suggestions on improving the wording in any exception you encounter in the submission process, please share feedback
using the **Feedback** link at the top of the page where you encountered it.

Most such errors and warnings about your submission data will be encountered on the **Validate** tab, but a small subset
will only ever be seen on the **Start** tab.  That's because it is the only time during the submission build process
where TraceBase evaluates your peak annotation files collectively.

It is important to note that if you discover an overlooked peak annotation file, it should be submitted on the **Start**
page with all other peak annotation files.  That is the only way to identify some issues and it auto-fills multiple
sheets with the data it extracts.  The best strategy to save work already done up to that point (until the ability to
add an existing stay doc on the start page for updating has been added), is to copy it over to a newly generated study
doc template.

## Exceptions

### AnimalWithoutSamples (AnimalsWithoutSamples)

An animal was detected without any samples associated with it in the `Samples` sheet.

If the animal has samples in the `Samples` sheet, it is likely that the load of every sample associated with the
animal encountered a separate error.  Fixing those errors will resolve this warning.

If however, there are no samples associated with the animal in the `Samples` sheet, it is likely that one or more
peak annotation files associated with the animal was omitted when generating the Study Doc on the Upload **Start**
page.  In this case, to address the issue, it is recommended that you generate a new Study Doc from **all** peak
annotation files combined and copy over all of your work from the current file, being careful to account for new
ordered samples rows and all auto-filled sheets, like Peak Annotation File/Details and Compounds.

This is recommended for a number of reasons that are covered elsewhere in the TraceBase documentation, but to
summarize, the Upload **Start** page performs checks that are not performed elsewhere to find conflicting issues
between peak annotation files, and it fills in all inter-sheet references (including hidden sheets and columns and
peak group conflicts) that are laborious and error-prone to attempt manually.

You may alternatively elect to add the forgotten peak annotation files in a separate submission after the current
data has been loaded.  You may keep the animal records and ignore this warning.  The subsequent submission should
include the complete animal record and associated study record.

### AnimalWithoutSerumSamples (AnimalsWithoutSerumSamples)

An animal with a tracer infusion was detected without any serum samples associated with it in the `Samples`
sheet.

Serum samples are necessary in order for TraceBase to report FCirc calculations.

If the animal has serum samples in the `Samples` sheet, it is possible that the load of every serum sample
associated with the animal encountered a separate error.  Fixing those errors will resolve this warning.

If however, there are no serum samples associated with the animal in the `Samples` sheet, it is possible that a
peak annotation file associated with the animal was omitted when generating the Study Doc on the Upload **Start**
page.  In this case, to address the issue, it is recommended that you generate a new Study Doc from **all** peak
annotation files combined and copy over all of your work from the current file, being careful to account for new
ordered samples rows and all auto-filled sheets, like Peak Annotation File/Details and Compounds.

This is recommended for a number of reasons that are covered elsewhere in the TraceBase documentation, but to
summarize, the Upload **Start** page performs checks that are not performed elsewhere to find conflicting issues
between peak annotation files, and it fills in all inter-sheet references (including hidden sheets and columns and
peak group conflicts) that are laborious and error-prone to attempt manually.

You may alternatively elect to add the forgotten peak annotation files in a separate submission after the current
data has been loaded.  You may keep the animal records and ignore this warning.  The subsequent submission should
include the complete animal record and associated study record.

### AssumedMzxmlSampleMatch (AssumedMzxmlSampleMatches)

The sample name embedded in the mzXML filename uniquely but imperfectly matches.

This exception is only ever raised as a warning.  Some peak abundance correction tools have certain character
restrictions applied to sample headers.  Some such restrictions are:

* Sample names cannot start with a number
* No dashes (`-`) allowed
* Length limits

Those restritions are not the same as those of the mass spec instrument software or the tools that generate the
mzXML files from RAW files.  To get around those restrictions when they are encountered, the sample headers are
often modified, but the mzXML filenames remain the original value.

The loading code accommodates these peculiarities in order to be able to dynamically match the mzXML files with the
corresponding peak annotation file sample header.  This warning serves to simply be transparent about the
association being automatically made, in order to catch any potential authentic mismatches.

In every known case, this warning can be safely ignored.

### CompoundDoesNotExist

The compound from the input file does not exist as either a primary compound name or synonym.

There are 2 possible resolutions to this exception.  Both involve updates to the Compounds sheet.

* Add the name as a synonym to an existing matching compound record.
* Add a new row to the compounds sheet.

In either case, if no matching compound exists in the Compounds sheet of the Study Doc, be sure to check TraceBase’s
Compounds page for a matching compound record (missing the current name as a synonym).  The Upload **Start** page
which generates the Study Doc populates the sheet with existing compounds from TraceBase whose formulas exactly
match the formula obtained from the peak annotation file(s).  But the formula derived from a peak annotation file
may represent an ionized version of the compound record in TraceBase and thus, may not have been auto-added^, which
is why the TraceBase site should be consulted.

^ _Note that pre-populating the Compounds sheet with ionization variants is a proposed feature._
_See GitHub issue [#1195](https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/1195)._

### CompoundExistsAsMismatchedSynonym

The compound name already exists as a synonym of a differing compound.

To resolve this issue, either edit the compound in the input file to match and merge it with the existing compound
or remove the synonym from the differing compound record so that peak groups (and tracers) are associated with the
other compound record.

Note that this exception can arise due to either a formula that represents the ionized state of a compound or the
HMDB ID could be inaccurately assigned.

If the compound from the peak annotation file(s) differs from the existing TraceBase compound record (e.g. different
formula or HMDB ID), and the new record represents a distinctly different compound, reach out to the curators.  The
existing compound synonym may already be associated with a different compound in other studies, so either changes
would need to be made to those other studies or the new study would need to be edited to distinguish the different
compounds.  Either way, a curator will need to coordinate the fix to ensure database-wide consistency.

### ConflictingValueError (ConflictingValueErrors)

A conflicting value was encountered between previously loaded data and data being loaded from an input file.

The loading code does not currently support database model record updates, but it does support **adding** data to an
existing (and previously loaded) input file.  Some of those additions can **look** like updates.  Values on a
previously loaded row in delimited columns like the `Synonyms` column in the `Compounds` sheet, can receive
additional delimited values without error.

But when values in a column (outside of columns containing delimited values) change in a file that has been
previously loaded, you will get a `ConflictingValueError` exception.

Note that formatted columns (e.g. an infusate name) may use delimiters, but are not treated as delimited columns.

### DateParseError

Unable to parse date string.  Date string not in the expected format.

To resolve this exception, reformat the date using the format reported in the error.

### DefaultSequenceNotFound

An MS Run Sequence record, expected to exist in the database, could not be found.

Note that each sheet in the study doc is loaded independently, but the order in which they are loaded matters.  For
example, the `Sequences` sheet must be loaded before the `Peak Annotation Files` sheet.  If there was an error when
loading any rows on the `Sequences` sheet, this error would be encountered when attempting to find that sequence
that was just loaded.

Alternatively, this exception could have arisen because a `Sequences` sheet row was edited and values in the
`Default Sequence` column in the `Peak Annotation Files` sheet (or other linking column) was not similarly updated
and became unlinked.

To resolve this exception, either the previous error must be fixed, or the `Default Sequence` column’s value in the
`Peak Annotation Files` sheet must be updated to match a row in the `Sequence Name` column in the `Sequences` sheet.

### DuplicateCompoundIsotopes

Summary of `DuplicateValues` exceptions specific to the peak annotation files.  It does not report the affected
samples because all such errors always affect all samples, as peak annotation files typically have a column for each
sample and a row for each compound’s isotopic state.

This error occurs when a compound’s unique isotopic makeup appears in multiple rows.

### DuplicateHeaders

Duplicate headers encountered in the input file.

No duplicate headers are allowed.

### DuplicatePeakAnnotationFileName

Multiple peak annotation files appear to have the same name.

This exception is raised as an error on the Upload **Start** page only.

To resolve this issue, either resubmit the files to exclude a truly duplicate file or rename one or both of the
files to make their names unique.

TraceBase requires that peak annotation filenames be globally unique to avoid ambiguities when sharing or
referencing data files.

### DuplicatePeakGroupResolutions

A row in the `Peak Group Conflicts` sheet is duplicated, and may contain conflicting resolutions.

A row in the `Peak Group Conflicts` sheet is a duplicate if it contains the same (case insensitive) compound synonym
(or `/`-delimited synonyms in any order) and the same^ samples.

Refer to the documentation of the `MultiplePeakGroupRepresentation` exception for an explanation of multiple peak
group representations and the `Peak Group Conflicts` sheet’s involvement in resolving them.

This exception is a warning when the resolution is the same on each row, but an error if the resolution (i.e. the
selected representation - the peak annotation file) is different on each row.

^ _The **same** samples means **all** samples.  There is assumed to be no partial overlap between sample sets for_
_the same compounds because the automated construction of this file separates them programmatically, so be_
_careful editing the ``Peak Group Conflicts`` sheet, to make sure you do not introduce partial sample overlap_
_between rows._

### DuplicateValues (DuplicateValueErrors)

A duplicate value (or value combination) was found in an input file column (or columns) that requires unique
values (or a unique combination of values with 1 or more other columns).

Fixing this issue typically involves either deleting a duplicate row or editing the duplicate to make it unique.

### DurationError

Invalid time duration value.  Must be a number.

To resolve this exception, edit the value to only be a number (no units symbol).

### EmptyColumns

The data import encountered empty columns that were expected to have data.

If there are sample columns present and all expected samples are accounted for, this will be a warning.  If any of
the expected sample columns are missing, this will be an error.

In the warning case, this issue usually occurs when columns in Excel have been removed (or some unknown file
manipulation has occurred).  Whatever the case may be, the excel reader package that the loading code uses treats
these empty columns as populated and names them with an arbitrary column header that starts with ‘Unnamed: ‘.

In the error case, no sample headers were found.  The file either contains no sample data and should be either
repaired or excluded from loading, meaning that it will need to be removed from the Peak Annotation Details and
Files sheets.

### ExcelSheetNotFound

Expected Excel file sheet not found.  Ensure the correct file was supplied.

### FileFromInputNotFound

A report of filenames obtained from an input file that could not be found.

### InfileDatabaseError

An unexpected internal database error has been encountered when trying to load specific input from an input file.

Exceptions like these are often hard to interpret, but the error was caught so that metadata about the related input
could be provided, such as the file, sheet, row, and column values that were being loaded when the exception
occurred.  However, the cause could be hard to determine if it is related to previously loaded data that did not
report an error.

If the cause of the error is not easily discernible, feel free to leave it for a curator to figure out.

These exceptions, when they occur on a somewhat regular basis, are figured out and the work in figuring out the
cause and likely solution is saved in a custom exception class to make them easier to fix when they crop up again.

### InfusateParsingError

A regular expression or other parsing error was encountered when parsing an Infusate string.  The formatting or
completeness of the string must be manually fixed.  Consult formatting guidelines (check the file’s header
comment).

### InvalidHeaders

Unexpected headers encountered in the input file.

No unexpected headers are allowed.

### InvalidMSRunName

Unable to parse Sequence Name.  Must be 4 comma-delimited values of Operator, LC Protocol, Instrument, and
Date].

### InvalidPeakAnnotationFileFormat

The peak annotation file format code is either unrecognized, or doesn’t appear to match the auto-detected format
of the supplied file.

This exception is raised as an error on the Upload **Start** page only.

To resolve this issue, select the format code using the dropdown menus in the `File Format` column of the
`Peak Annotation Files` sheet in the Study Doc that corresponds to the reported file.

Note that this error is more likely to occur when supplying CSV or TSV versions of peak annotation files.  Automatic
format determination is based on the Excel sheet and/or column names, and there is a lot of overlap in the column
names of the different formats.

### IsotopeParsingError

A regular expression or other parsing error was encountered when parsing an Isotope string.  The formatting or
completeness of the string must be manually fixed.  Consult formatting guidelines (check the file’s header
comment).

### IsotopeStringDupe

The formatted isotope string matches the same labeled element more than once.

Strings defining isotopes are formatted with multiple element symbols paired with mass numbers concatenated
together, followed by their dash-delimited counts in the same relative order.  This error occurs when that isotope
string matches the same element multiple times.

Unfortunately, the only way to address this error would be to edit the peak annotation file to eliminate the
duplicate.

Example:
`C13N15C13-label-2-1-1` would match `C13` twice, resulting in this error.

### MissingC12ParentPeak (MissingC12ParentPeaks)

No C12 PARENT row was found for this compound in the peak annotation file.

This exception occurs (as a warning) in 2 cases:

* The C12 PARENT peak exists, but was not picked in El-Maven.  In this case, the best solution is to redo the peak annotation file starting from re-picked peaks from El-Maven that include the parent peak.  Alternatively, the peak annotation file could be edited to remove all of that compound’s peaks and a subsequent file could be loaded using the complete peak group.
* The C12 PARENT peak was below the detection threshold.  In this case, the warning can be ignored and a 0-count will be assumed.

### MissingCompounds (AllMissingCompounds)

Summary of compounds expected to exist in the database that were not found, while loading a single input file.

### MissingDataAdded

Use this for warnings only, when missing data exceptions are caught, handled to autofill missing data in a
related sheet, and repackaged as a warning to transparently let the user know when repairs have occurred.

Examples:
Novel animal treatment:
A novel animal treatment is entered into the `Treatment` column of the `Animals` sheet in the Study Doc, but
not into the `Treatments` sheet.  The TraceBase Upload **Validate** page will autofill the new treatment
name in a new row added to the `Treatments` sheet of the Study Doc.

```default
Novel tissue:
    A novel tissue is entered into the `Tissue` column of the `Samples` sheet in the Study Doc, but not into the
    `Tissues` sheet.  The TraceBase Upload **Validate** page will autofill the new tissue name in a new row
    added to the `Tissues` sheet of the Study Doc.
```

This warning is an indicator that there is new data to potentially fill in in the mentioned sheet.

### MissingFCircCalculationValue (MissingFCircCalculationValues)

A value, while not required, but necessary for (accurate) FCirc calculations, is missing.

TraceBase does not require values for some database model fields because it supports animals that have not been
infused with tracers, but when an animal does have a tracer infusion, certain values are necessary to accurately
compute FCirc.  If any of those values have not been filled in, and the animal has an infusate, you will see this
exception as a warning.

While your data can be loaded without these values, it is highly recommended that all such values be supplied in
order to show FCirc records with calculated values and without associated errors or warnings.

### MissingSamples (AllMissingSamples)

Summary of samples expected to exist in the database that were not found, while loading a single input file.

### MissingStudies (AllMissingStudies)

Summary of studies expected to exist in the database that were not found, while loading a single input file.

### MissingTissues (AllMissingTissues)

Summary of tissues expected to exist in the database that were not found, while loading a single input file.

### MissingTreatments (AllMissingTreatments)

Summary of treatments expected to exist in the database that were not found, while loading a single input
file.

### MixedPolarityErrors

A mix of positive and negative polarities were found in an mzXML file.

TraceBase does not support mixed polarity mzXML files.

### MultiplePeakAnnotationFileFormats

The peak annotation file format could not be uniquely determined.

This exception is raised as an error on the Upload **Start** page only.

To resolve this issue, select the format code using the dropdown menus in the `File Format` column of the
`Peak Annotation Files` sheet in the Study Doc that corresponds to the reported file.

Note that this error is more likely to occur when supplying CSV or TSV versions of peak annotation files.  Automatic
format determination is based on the Excel sheet and/or column names, and there is a lot of overlap in the column
names of the different formats.

### MultiplePeakGroupRepresentation (MultiplePeakGroupRepresentations, AllMultiplePeakGroupRepresentations)

A peak group for a measured compound was picked multiple times and abundance corrected for 1 or more samples.

TraceBase requires that the single best representation of a peak group compound be loaded for any one sample.

Certain compounds can show up in both positive and negative scans, or in abutting scan ranges of the same polarity.
While neither representation may be perfect, this simple requirement prevents inaccuracies or mistakes when using
the data from TraceBase.

Be wary however that your compound names in your peak annotation files are consistently named, because different
synonyms are not detected as multiple peak group representations.  This was a design decision to support succinct
compound records while also supporting stereo-isomers.  (Note: This distinction may go away in a future version of
TraceBase, where synonyms are treated as the same compound in the context of peak groups.)

Multiple peak group representations are only detected and reported on the Upload **Start** page, when the peak
annotation files have their compounds and samples extracted.  The multiple representations are recorded in an
otherwise hidden sheet in the Study Doc named `Peak Group Conflicts`.

For every row in the conflicts sheet, a peak annotation file drop-down is supplied to pick the file from which the
peak group should be loaded.  The same peak group compound from the other file(s) will be skipped.

If you forgot a peak annotation file when generating your study doc template, start over with a complete generated
Study Doc in order to catch these issues.  But note, previously loaded Peak Group records are included in the
detection of multiple representations, so alternatively, you may choose to add forgotten peak annotation files in a
separate submission, keeping in mind that if you select the new peak annotation file as the peak group
representation to load, the old previously loaded peak group will be deleted.

Note that while manual editing of this sheet is discouraged, you can manually edit it as long as you preserve the
hidden column.  There is a hidden sample column containing delimited sample names.  This column is required to
accurately update all multiple representations.

### MultipleRecordsReturned

The record search was expected to match exactly 1 record, but multiple records were found.

This issue can arise for various reasons, but usually are related to conflicting sample naming conventions in the MS
instrument that produces the RAW (and by implication, the mzXML) filenames, the abundance correction software that
produces the peak annotation files (e.g. AccuCor), and TraceBase’s unique biological sample naming constraints
related to scan labels.

TraceBase’s attempts to map differing sample names in differing contexts to a single biological Sample record can
yield this exception in one case when there exists a biological sample duplicate in the Study Doc’s Samples sheet.
This can happen due to the retention of scan labels in sample names when populating that sheet.  So one possible
resolution may be to merge duplicate sample records in the Samples sheet that happen to be different scans of the
same biological sample.

Another possibility could be misidentified “scan labels” that for example do not refer to polarity that were
manually fixed, but which cause issues when trying to map mzXML filenames or peak annotation file sample headers to
those Sample records.

Each issue should be handled on a case-by-case basis.

### MultipleStudyDocVersions

The study doc version could not be automatically narrowed down to a single matching version.

This exception is accompanied by version determination metadata intended to highlight the supplied versus expected
sheet and column names.  Note however, that the only column names that will be reported as missing are required
columns.  Missing optional columns may be reported as “unknown”.

TraceBase is backward compatible with older versions of the Study Doc and the Upload **Validate** page automatically
detects the version based on sheet and column names.

This exception could arise if various sheets have been removed, leaving sheets whose required column names do not
differ between versions.  There is currently no fix for this issue on the Upload **Validate** page and validation
must happen on the command-line where a version number can be supplied.  In this case, it is recommended that you
skip validation and if you think the data is complete, move on to the **Submit** step.

### MzXMLSkipRowError (AllMzXMLSkipRowErrors, AllMzXMLSkipRowErrors)

Could not determine which mzXML file loads to skip.

When the mzXML file paths are not supplied and the number of mzXML files of the same name exceed number of skipped
sample data headers in the peak annotation files (i.e. some of the same-named files are to be loaded and others are
to be skipped), it may be impossible to tell which ones are which.

The loading code can infer which are which if the files are divided into directories with their peak annotation
files in which they were used, but if that cannot be figured out, this error will be raised.

This can be resolved either by accounting for all mzXML files in the `Peak Annotation Details` sheet with their
paths or by organizing the mzXML files with the peak annotation files they were used to produce.

### MzxmlColocatedWithMultipleAnnot

mzXML files are in a directory that has multiple peak annotation files somewhere along its path.

This exception has to do with determining which MS Run `Sequence` produced the mzXML file, which is dynamically
determined when there are multiple sequences containing the same sample names.

mzXML files are assigned an MS Run Sequence based on either the value in the `Sequence` column in the
`Peak Annotation Details` sheet or (if that’s empty), the `Default Sequence` defined in the `Peak Annotation Files`
<!-- textlint-disable terminology -->
sheet and the `Peak Annotation File Name` column in the `Peak Annotation Details` sheet.
<!-- textlint-enable terminology -->

If the default is used, this exception is a warning.  However, if there is no default and no `Sequence` in the
`Peak Annotation Details` sheet’s `Sequence` column, the association is inferred by the directory structure.  By
travelling up the path from the mzXML file to the study directory, the first peak annotation file encountered is the
one that is associated with the mzXML file.  The simplest case is when the mzXML file is in the same directory as a
single peak annotation file.

The loading code only raises this as an error when the mzXML filename matches headers in multiple peak annotation
files from different sequences and the specific one in which it was used was not explicitly assigned and it could
not be inferred from the directory structure.

The easiest fix is to put peak annotation files in a directory along with only the mzXML files that were used in its
production.  The more laborious (but more versatile) solution is to add the file path of every mzXML reported in the
error to the `Peak Annotation Details` sheet along with the `Sequence`.

### MzxmlNotColocatedWithAnnot

mzXML files are not in a directory under an unambiguously associated peak annotation file in which they were
used.

This exception has to do with determining which MS Run `Sequence` produced the mzXML file, which is dynamically
determined when there are multiple sequences containing the same sample names.

mzXML files are assigned an MS Run Sequence based on either the value in the `Sequence` column in the
`Peak Annotation Details` sheet or (if that’s empty), the `Default Sequence` defined in the `Peak Annotation Files`
<!-- textlint-disable terminology -->
sheet and the `Peak Annotation File Name` column in the `Peak Annotation Details` sheet.
<!-- textlint-enable terminology -->

If the default is used, this exception is a warning.  However, if there is no default and no `Sequence` in the
`Peak Annotation Details` sheet’s `Sequence` column, the association is inferred by the directory structure.  By
travelling up the path from the mzXML file to the study directory, the first peak annotation file encountered is the
one that is associated with the mzXML file.  The simplest case is when the mzXML file is in the same directory as a
single peak annotation file.

The loading code only raises this as an error when the mzXML filename matches headers in multiple peak annotation
files from different sequences and the specific one in which it was used was not explicitly assigned and it could
not be inferred from the directory structure.

The easiest fix is to put peak annotation files in a directory along with only the mzXML files that were used in its
production.  The more laborious (but more versatile) solution is to add the file path of every mzXML reported in the
error to the `Peak Annotation Details` sheet along with the `Sequence`.

### MzxmlParseError

The structure of the mzXML file is not as expected.  An expected XML element or element attribute was not found.

This could be due to an mzXML version change or a malformed or truncated file.

TraceBase supports mzXML version 3.2.

### MzxmlSampleHeaderMismatch

The mzXML filename does not match the sample header in the peak annotation file.

<!-- textlint-disable terminology -->
This situation can arise either if the filename has been (knowingly) manually modified or when the `mzXML File Name`
<!-- textlint-enable terminology -->
entered into the `Peak Annotation Details` sheet was mistakenly associated with the wrong `Sample Data Header`.

This exception is only ever raised as a warning and is not inspected by curators, so confirm the association and
either make a correction or ignore, if the association is correct.

### MzxmlSequenceUnknown (AllMzxmlSequenceUnknown)

Unable to reliably match an mzXML file with an MSRunSequence.

This exception is raised as a warning when the number of mzXML files with the same name are not all accounted for in
the Peak Annotation Details sheet of the Study Doc.  I.e. there are more mzXML files than peak annotation files with
sample headers of this name.

There are a number of ways this can happen:
- The extra files are empty (and are reported in `NoScans` warnings).
- A peak annotation file for the extras has not been included in the load.
- The sample was re-analyzed in a subsequence MS Run because there was a problem with the first run.
- There are 2 different biological samples with the same name and one is not included in the current submission.

The first case is handled automatically and can be safely ignored.  In fact, if it is any other case, an error would
be raised after this warning, so in any case, this can be ignored, but if subsequent error does occur, this warning
provides information that can help figure out the problem.

In all of the other cases, there are 2 ways to resolve the warning:

* Add rows to the Peak Annotation Details sheet that account for all the files (adding ‘skip’ to the `Skip` column for any files that should be ignored).  This is the preferred solution.
<!-- textlint-disable terminology -->
* Add the relative path from the study folder to the specific mzXML file in the existing `mzXML File Name` column (not including the study folder name).
<!-- textlint-enable terminology -->

Despite the ‘required columns’ highlighted in blue indicating that ‘Sample Name’ and ‘Sample Data Header’ are
<!-- textlint-disable terminology -->
required, when there is no associated peak annotation file, the `mzXML File Name`, `Sequence`, and `Skip` columns
<!-- textlint-enable terminology -->
are all that’s required.  This is a special case.

If all of the files are for the same MS Run, nothing further is needed.  But if they are from different MS Runs, the
<!-- textlint-disable terminology -->
`mzXML File Name` column must contain the relative path from the study folder to the mzXML file (not including the
<!-- textlint-enable terminology -->
study folder name).

### NewResearcher (NewResearchers, AllNewResearchers)

When an as-yet unencountered researcher name is encountered, this exception is raised as a warning to ensure it
is not a spelling variant of an existing researcher name.

### NoSamples

None of the samples in the indicated file, required to exist in the database, were found.

Each sheet in an excel file is loaded independently and the loads proceed in the order of those dependencies.

Errors like this usually only happen when related dependent data failed to load (due to some other error) and is
evidenced by the fact that the indicated columns/rows have values.  Fixing errors that appear above this will fix
this error.

For example, an Animal record must be loaded and exist in the database before a Sample record (which links to an
Animal record) can be loaded.  If the loading of the Animal record encountered an error, anytime a Sample record
that links to that animal is loaded, this error will occur.

The loading code tries to avoid these “redundant” errors, but it also tries to gather as many errors as possible to
reduce repeated validate/edit iterations.

### NoScans (AllNoScans)

An mzXML file was encountered that contains no scan data.

This exception is raised as a warning and can be safely ignored.  Empty mzXML files are produced as a side-effect of
the way they are produced.  Such files could be excluded from a study submission, but are hard to distinguish
without looking inside the same-named files.  It is recommended that the files be left as-is.

### NoTracerLabeledElements (NoTracerLabeledElementsError)

A compound in a peak annotation file was encountered that does not contain any of the labeled elements from any
of the tracers.

The purpose of a peak group (which the loading code populates) is to group a compound’s peaks that result from
various isotopic states (the incorporation of labeled elements from the tracer compounds).  If the formula of the
measured compound does not contain any of the elements that are labeled in the tracers, this suggests a potential
problem, such as the animal’s infusate from the Animals sheet was incorrectly selected or omits a tracer with labels
that are in this compound.

Resolutions to this issue can involve either updating the associated animal’s infusate/tracers to include a tracer
with the labeled elements it shares with this compound or simply ignoring this warning noting that the compound will
not be loaded as a peak group.^

^ _TraceBase was not designed to support non-isotopic mass spectrometry data.  Adding support for non-isotopic_
_data is a planned feature.  See GitHub issue_
_[#1192](https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/1192)._

### NoTracers

An operation that requires an animal to have been infused with tracers encountered an animal that was not infused
with tracers, such as FCirc calculations.

This error occurs when an animal is associated with an infusate record, but that infusate is not linked to any
tracers.  This is likely because an error occurred during infusate/tracer loading and arises when validating a serum
sample.

### ObservedIsotopeParsingError

A regular expression or other parsing error was encountered when parsing an Isotope observation string.  The
formatting or completeness of the string must be manually fixed.  Consult formatting guidelines (check the file’s
header comment).

### ObservedIsotopeUnbalancedError

The number of elements, mass numbers, and counts parsed from the isotope string differ.  A single (fully labeled)
isotope must include each value in the order of mass number, element symbol, and count.  E.g. `13C5` means that
there are 5 heavy carbons of mass number 13 in a compound.

Examples:
- `13C` would cause this error because there is no count.
- `C5` would cause this error because there is no mass number.
- `135` would cause this error because there is no element and there’s no way to tell where the count begins.

### ParsingError

Superclass of infusate, tracer, and isotope parsing errors.

### PossibleDuplicateSample (PossibleDuplicateSamples)

Multiple peak annotation files have an identical sample header, but are associated with distinctly different
TraceBase biological Sample records.

This exception is always raised as a warning as a check to ensure that the distinction is intentional, and not just
a copy/paste error.

If there do exist different biological samples that happen to have the exact same name, this warning can be safely
ignored.  If they are the same biological sample, the `Sample Name` column in the `Peak Annotation Details` sheet
must be updated.  You may also need to delete or update the associated row in the `Samples` sheet, if no other
verified rows in the `Peak Annotation Details` sheet refers to it.

### ProhibitedCompoundName (ProhibitedCompoundNames)

The compound name or synonym contains disallowed characters that were replaced with similar allowed characters.

This exception is always raised as a warning.

Disallowed characters are either compound name/synonym delimiters or Peak Group name delimiters that are used during
loading.

While the offending characters are automatically replaced, you may elect to use an alternate character.  If you go
with the automatic replacement, nothing further needs to be done, but if you edit the values in the Study Doc, but
be sure to make the edit everywhere, including the Compounds sheet, the Tracers/Infusates sheet (and in the Infusate
column in the Animals sheet), the Peak Group Conflicts sheet.  Also, all peak annotation files will need to be
updated as well.

### RecordDoesNotExist

The expected record from the indicated database model was not found.

### ReplacingPeakGroupRepresentation

A previously loaded peak group from a previous submission (for a measured compound was picked multiple times and
abundance corrected for 1 or more samples) will be replaced with a new representation from a new peak annotation
file that includes this compound for the same 1 or more samples.

Refer to the documentation of the `MultiplePeakGroupRepresentation` exception for an explanation of multiple peak
group representations and TraceBase’s requirements related to them.

This exception is always raised as a warning, to be transparent about the replacement of previously loaded Peak
Group records.  By selecting the new peak annotation file as the peak group representation to load in the
`Peak Group Conflicts` sheet, this warning informs you that an old previously loaded peak group will be deleted.

This exception is expected when a selection has been made that supercedes a selection made in a previous load
relating to the same samples and compound.

### RequiredColumnValue (RequiredColumnValues)

A value, required to exist in the input table, was not supplied.

### RequiredHeadersError

Supplies a list of missing required column headers in the input file.

### RequiredValueError (RequiredValueErrors)

A value, required to exist in the database, was found to be missing.

Each sheet in an excel file is loaded independently and the loads proceed in the order of those dependencies.

Errors like this usually only happen when related dependent data failed to load (due to some other error) and is
evidenced by the fact that the indicated columns/rows have values.  Fixing errors that appear above this will fix
this error.

For example, an Animal record must be loaded and exist in the database before a Sample record (which links to an
Animal record) can be loaded.  If the loading of the Animal record encountered an error, anytime a Sample record
that links to that animal is loaded, this error will occur.

The loading code tries to avoid these “redundant” errors, but it also tries to gather as many errors as possible to
reduce repeated validate/edit iterations.

### SheetMergeError

### SynonymExistsAsMismatchedCompound

The compound synonym already exists as the primary name of a differing compound.

To resolve this issue, either edit the new compound containing the conflicting synonym in the input file to match
and merge it with the existing compound or remove the new compound record so that peak groups (and tracers) are
associated with the other compound record.

Note that this exception can arise due to either a formula that represents the ionized state of a compound or the
HMDB ID could be inaccurately assigned.

If the compound from the peak annotation file(s) differs from the existing TraceBase compound record (e.g. different
formula or HMDB ID), and the new record represents a distinctly different compound, reach out to the curators.  The
existing compound name may already be associated with a different compound in other studies, so either changes would
need to be made to those other studies or the new study would need to be edited to distinguish the different
compounds.  Either way, a curator will need to coordinate the fix to ensure database-wide consistency.

### TracerCompoundNameInconsistent

The compound name used in the tracer name is not the primary compound name.

TraceBase requires that tracer names use the primary compound name so that searches yield complete and consistent
results.  It automatically changes the tracer name to use the primary compound and raises this exception as a
warning, to be transparent about the modification of the user-entered compound name.

If the established primary compound name is problematic, reach out to a TraceBase curator to propose a change of a
compound’s primary name.  Note that such a change will affect all studies that use this tracer (if any).

### TracerGroupsInconsistent

An infusate is either a duplicate or exists with a conflicting tracer group name.

A duplicate infusate can trigger this exception due to concentration value precision.  In other words, it’s not
technically a true duplicate, but is treated as such due to the fact that concentration values may exceed a
precision threshold.  Excel and the underlying Postgres database have slightly different levels of precision.
TraceBase saves what you enter, but when it is entered into the database, the precision may change and end up
matching another record.  It’s also important to note that while TraceBase saves the value you enter, it searches
for infusates using significant figures, which can also lead to a duplicate exception.  See the tracer column
headers in the `Infusates` sheet in the Study Doc for details of what significant figures are used.

The resolution in the duplicate case is to use existing records whose concentration values insignificantly differ.

The other reason this exception may be raised could be due to nomenclature control over the `Tracer Group Name`,
which must be the same across all infusates that that include the same tracer compounds, regardless of concentration
and isotopic inclusion.

If the tracer group name differs, you must use the pre-existing group name already in TraceBase.  If the group name
is problematic, reach out to a TraceBase curator to fix it.

### TracerParsingError

A regular expression or other parsing error was encountered when parsing a Tracer string.  The formatting or
completeness of the string must be manually fixed.  Consult formatting guidelines (check the file’s header
comment).

### UnexpectedInput

The value in the indicated column is optional, but is required to be supplied **with** another neighboring
column value, that was found to be absent.

This exception can be resolved either by supplying the neighboring column’s value or by removing this column’s
value.

Example:
If an infusion rate is supplied, but there was no infusate supplied, the infusion rate will cause an
UnexpectedInput exception, because an infusion rate without an infusate makes no sense.

### UnexpectedLabel (UnexpectedLabels, AllUnexpectedLabels)

An isotope label, e.g. nitrogen (`N`) was detected in a measured compound, but that labeled element was not in
any of the tracers.  This is reported as a warning to suggest that there could be contamination or the wrong
infusate was selected for an animal, but this is often the result of naturally occurring isotopes and can be
ignored.

### UnexpectedSamples

Sample headers found in a peak annotations file were not in the Study Doc’s Peak Annotation Details sheet.

This could either be due to a sample header omission in the Peak Annotation Details sheet or due to the wrong peak
annotation file being associated with one or more sample headers in the Peak Annotation Details sheet.

### UnknownHeader

A column header was encountered that is not a part of the file specification.

### UnknownHeaders

A list of column headers encountered that are not a part of the file specification.

### UnknownPeakAnnotationFileFormat

The peak annotation file format is unrecognized.

This exception is raised as an error on the Upload **Start** page only.

To resolve this issue, select the format code using the dropdown menus in the `File Format` column of the
`Peak Annotation Files` sheet in the Study Doc that corresponds to the reported file.  If none of the supported
formats in the dropdown match the file format, reach out to the TraceBase team to request adding support for the new
format.  In the meantime, it is recommended that you use one of the supported natural abundance correction tools to
regenerate the file in a TraceBase-compatible format.

### UnknownStudyDocVersion

The study doc version could not be automatically determined.

This exception is accompanied by version determination metadata intended to highlight the supplied versus expected
sheet and column names.  Note however, that the only column names that will be reported as missing are required
columns.  Missing optional columns may be reported as “unknown”.

TraceBase is backward compatible with older versions of the Study Doc and the Upload **Validate** page automatically
detects the version based on sheet and column names.

This exception could arise if the sheet names and/or column names were modified.  Try generating a new study doc
from the Upload **Start** page and compare the sheet and column names to ensure they were not inadvertently altered.
If there are differences, fix them so that the version can be identified by the Upload **Validate** interface.

### UnmatchedBlankMzXML (UnmatchedBlankMzXMLs)

This exception is the same as `UnmatchedMzXML`, but is a warning because the files have “blank” in their sample
names and are assumed to have been intentionally excluded.

### UnskippedBlanks

A sample, slated for loading, appears to be a blank.  Loading of blank samples should be skipped.

Blank samples should be entirely excluded from the Samples sheet, but listed in the `Peak Annotation Details` sheet
with a non-empty value in the `Skip` column.  This tells the peak annotations loader that loads the peak annotations
file to ignore the sample column with this sample name.

Blank samples are automatically skipped in the Upload **Start** page’s Study Doc download, based on the sample name
containing “blank” in its name.

## Module contents

### CompoundExistsAsMismatchedSynonym

The compound name already exists as a synonym of a differing compound.

To resolve this issue, either edit the compound in the input file to match and merge it with the existing compound
or remove the synonym from the differing compound record so that peak groups (and tracers) are associated with the
other compound record.

Note that this exception can arise due to either a formula that represents the ionized state of a compound or the
HMDB ID could be inaccurately assigned.

If the compound from the peak annotation file(s) differs from the existing TraceBase compound record (e.g. different
formula or HMDB ID), and the new record represents a distinctly different compound, reach out to the curators.  The
existing compound synonym may already be associated with a different compound in other studies, so either changes
would need to be made to those other studies or the new study would need to be edited to distinguish the different
compounds.  Either way, a curator will need to coordinate the fix to ensure database-wide consistency.

### ConflictingValueError (ConflictingValueErrors)

A conflicting value was encountered between previously loaded data and data being loaded from an input file.

The loading code does not currently support database model record updates, but it does support **adding** data to an
existing (and previously loaded) input file.  Some of those additions can **look** like updates.  Values on a
previously loaded row in delimited columns like the `Synonyms` column in the `Compounds` sheet, can receive
additional delimited values without error.

But when values in a column (outside of columns containing delimited values) change in a file that has been
previously loaded, you will get a `ConflictingValueError` exception.

Note that formatted columns (e.g. an infusate name) may use delimiters, but are not treated as delimited columns.

### DuplicateValues (DuplicateValueErrors)

A duplicate value (or value combination) was found in an input file column (or columns) that requires unique
values (or a unique combination of values with 1 or more other columns).

Fixing this issue typically involves either deleting a duplicate row or editing the duplicate to make it unique.

### IsotopeParsingError

A regular expression or other parsing error was encountered when parsing an Isotope string.  The formatting or
completeness of the string must be manually fixed.  Consult formatting guidelines (check the file’s header
comment).

### ObservedIsotopeParsingError

A regular expression or other parsing error was encountered when parsing an Isotope observation string.  The
formatting or completeness of the string must be manually fixed.  Consult formatting guidelines (check the file’s
header comment).

### QuerysetToPandasDataFrame

convert several querysets to Pandas DataFrames, then create additional
DataFrames for study or animal based summary data

### RequiredValueError (RequiredValueErrors)

A value, required to exist in the database, was found to be missing.

Each sheet in an excel file is loaded independently and the loads proceed in the order of those dependencies.

Errors like this usually only happen when related dependent data failed to load (due to some other error) and is
evidenced by the fact that the indicated columns/rows have values.  Fixing errors that appear above this will fix
this error.

For example, an Animal record must be loaded and exist in the database before a Sample record (which links to an
Animal record) can be loaded.  If the loading of the Animal record encountered an error, anytime a Sample record
that links to that animal is loaded, this error will occur.

The loading code tries to avoid these “redundant” errors, but it also tries to gather as many errors as possible to
reduce repeated validate/edit iterations.

### SynonymExistsAsMismatchedCompound

The compound synonym already exists as the primary name of a differing compound.

To resolve this issue, either edit the new compound containing the conflicting synonym in the input file to match
and merge it with the existing compound or remove the new compound record so that peak groups (and tracers) are
associated with the other compound record.

Note that this exception can arise due to either a formula that represents the ionized state of a compound or the
HMDB ID could be inaccurately assigned.

If the compound from the peak annotation file(s) differs from the existing TraceBase compound record (e.g. different
formula or HMDB ID), and the new record represents a distinctly different compound, reach out to the curators.  The
existing compound name may already be associated with a different compound in other studies, so either changes would
need to be made to those other studies or the new study would need to be edited to distinguish the different
compounds.  Either way, a curator will need to coordinate the fix to ensure database-wide consistency.

### UnknownHeaders

A list of column headers encountered that are not a part of the file specification.

## About This Document

This document is partially auto-generated using docstrings from the traceBase codebase.  The content under
**Errors and Warnings (Exceptions) Reference** is completely auto-generated, with minor edits.  Consult the
`TraceBaseDocs/README.md` file before making any updates.
