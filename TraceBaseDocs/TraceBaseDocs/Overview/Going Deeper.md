# Going Deeper

TraceBase was built to solve a practical problem, inherent to how the science of metabolomics is performed.  Each
experiment is designed around a specific question or questions about a metabolite.  However, when you analyze samples,
the instruments used yield information that is much more than sufficient to answer one question, so much of the data
generated goes unscrutinized/uninspected/unused.  This is valuable data that can potentially be applied to other
scientific questions, but if those questions arise much later, perhaps _years_ later, if may be difficult to track down
that data and all associated metadata.  It could be spread across computers, external hard drives, lab notebooks, or
even only exist in one resercher's memory.  And that researcher and those data sources may have moved on, thus tracking
down that potentially useful data can be an arduous and time consuming task.

Tracebase can serve as a repository to store all of that unused data in one central location, to be searched and
analyzed.  It even enables the ability to answer broader holistic scientific questions that could not be asked when the
data is not all in one accessible location or whose calculations were performed using different methods.

Such repositories, historically speaking, have difficult requirements that tend to be a barrier to data submission.  The
data can be complex and accuracy is dependent on the integrity of the relationships in the data.  It can also be
difficult to compile multiple files in a way that is consistent from file to file.  E.g. Typos in controlled terms (e.g.
tissue or compound names), or even access to acceptable variants of those terms can be time consuming to get right.

TraceBase endeavors to smooth all of that out, which brings us to the guiding general requirements for the development
of TraceBase.

## Goals

TraceBase was developed with a primary focus the following development goals/features that were determined to be
critical to mission success:

* Submission
* Search
* Accuracy
* Comparability

And more generally, we expect TraceBase to grow and evolve with ever increasing data, feature requests, data types, and
changes to existing data.  And we are aware that the curation of complex data can be a slow and complex process.  In
order for people to install and maintain a TraceBase instance for themselves, overhead needed to be simple and
minimized.  Thus, this project was also tackled with these core principles in mind:

* Minimize overhead
  * Empower researchers to solve their own submission issues (e.g. the validation page) so that they don't have to wait
    through a time consuming curation process that involves multiple back and forths while working through issues.  At
    the same time, this reduces the maintenance efforts necessary to keep TraceBase working.
  * Automate the upkeep of calculated values when data changes
* Adaptability
  * TraceBase was made to be largely data-independent.  E.g. Add a field to a table in the database and features like
    the list views, the loading scripts, or the advanced search either automatically adapt or only require relatively
    simple configuration changes.
  * If data in a study is changed, removed, or added, key changes are monitored to trigger automatic updates of
    derived/calculated values.
* Scalability
  * From a development standpoint, this is similar to the adaptability goal.
  * From a database size standpoint, this means ensuring that performance doesn't wane as data is added.

## Under the hood

Given a development plan, with the goals outlined above, TraceBase contains a number of hidden features that end users
don't usually see, but bring enormous value to the TraceBase codebase in terms of maintenance and future development
efforts.

### Search Strategy

The advanced search code was written to work on any database table field, and it pulls in fields from multiple related
tables into a single hybrid representation (e.g. the
[PeakGroups](../Download/About%20the%20Data/Data%20Types/PeakGroups.md),
[PeakData](../Download/About%20the%20Data/Data%20Types/PeakData.md), and
[FCirc](../Download/About%20the%20Data/Data%20Types/FCirc.md)).  Each hybrid data "format" is configurable and the
search code is independent of the configuration, thus adding or changing fields/columns is a straightforward edit.
Creation of new formats is also relatively simple.

Thus the Advanced Search interface is scalable and adaptable.

### Caching Strategy

Caching of various expensive-to-calculate values in TraceBase increases performance.  Typical caching strategies
periodically refresh cached values based on a simple time schedule, but during times of low traffic, the periodic
rebuilding of those values is susceptible to significantly impacting the performance of a large streamed search results
download, for example.

Additionally, if new data is loaded or changed, cached values can be incorrect until affected cached values are rebuilt,
so a user may be presented with that inaccurate data.

To solve both of these issues, TraceBase employs a caching strategy that sets monitors on specific data that, if it's
changed, will trigger an immediate cache update of the affected values.  That means that as long as a study doesn't
change, performance will always be optimal, and the values always accurate.

### Dynamically Maintained Database Fields

While the caching strategy above assures data accuracy and consistent performance, it has one big drawback: cached
values are not searchable, which is for example why you cannot search based on calculated FCirc values.  It also has a
little overhead of generating those cached values after a load.

A strategy newer than the caching strategy that solves accuracy, performance, _and_ searchability, is maintained
database table fields, and that work was done with a mind toward both scalability and adaptability.  It works much in
the same way that the caching strategy does, as it triggers updates based on monitored database records, but comes with
extra features:

* Multiple configurable modes that govern when to perform the updates.
* A mass autoupdate feature integrated with the loading scripts (to eliminate all associated overhead tasks).
* An extremely small code usage footprint that can
  * Apply field maintenance to a field in a table with as few as 2 lines of code (when given a method to generate a
    field value), making it highly robust to change.
  * Apply mass autoupdate functionality to load methods with a single line of code.
* Restricts maintained fields from manual updates to ensure accuracy and proper loading code development.

### Validation Interface

As the most visible under-the-hood feature, the data submission interface's validation page uses the exact same code
that is used to load the database on the back end.  A suite of errors and warnings were developed using a strategy, in
conjunction with the loading code, to gather as many issues as is possible in a single load attempt to reduce the number
of iterations necessary to correct all data issues and arrive at a study submission as fast as possible.  A number of
components and fine-grained strategies coalesced to contribute to this overall strategy.

#### Consolidated Input

To empower users and expedite the submission building process, eliminating the time consuming back and forth with a
curator when the researcher needs new/novel standardized data, all metadata including nomenclature-controlled data can
be loaded using a single excel spreadsheet.  It is a one-stop shop for all metadata in TraceBase.

#### Standardized Loading

All loading code inherits base functionality that handles the minutiae of loading data so that individually developed
loading scripts can focus on the data.  Every loading script is uncommitted/reversible using a strategy called atomic
transactions.  This is what allows the loading code to be able to be re-used for the validation page, so that the errors
and warnings that the researcher sees while validating their data is exactly what the curator sees when they attempt to
load the submission into the database.

Also, each loader defines the structure of the associated sheet in the Study Doc (xlsx file).  It uses the database
table definitions to populate the column headers and is thus able to provide tips and information in the spreadsheet
itself, while at the same time, making it robust to database changes.

#### Customized Errors and Warnings and Handling

Database errors can be cryptic and hard to debug, even for developers, so particular attention was given to creating
helpful errors for common or not-so-common but difficult-to-solve errors.  Anytime an error was encountered, what was
learned from the debug process was saved in a custom error that explains what the issue is and suggests a likely fix.

Since repeated similar errors can hamper the process of fixing a submission, similar errors are collected and summarized
in a master error.

Lastly, errors and warnings are aggregated and organized into categorical groups to smooth out the submission building
process, including the collation and summarization of similar errors from multiple load scripts.

All of these error features come together to expedite the submission building process, ensuring that data is loaded as
quickly as possible and that researchers, who are the closest to the data, can fix issues on their own.

#### Template Creation and Autofill

The most time consuming task in building a submission is manual data entry.  To boost the study submission building
process, a custom template can be build by simply submitting all of the peak annotation files, along with optional mass
spec metadata associated with each file.  Samples and compounds will be parsed from those files and as much metadata as
is possible will be included in a downloaded template.  Standardized data from the database will be included as well.

What's more is that every time you validate a study doc, any partially entered data will be propagated to other sheets
as well.  For example, if you enter a novel tissue in the samples sheet or a tracer compound to the tracers sheet that
do not exist in the corresponding tissues or compounds sheet, that tissue name and compound name will be added to the
Tissues and Compounds sheets in the resulting download.

### Universal Peak Annotation Format

TraceBase currently supports the following peak annotation file types (in excel, tsv, and csv format):

* [AccuCor](https://doi.org/10.1021/acs.analchem.7b00396)
* [IsoCor](https://doi.org/10.1093/bioinformatics/btz209)
* [Iso-AutoCor](https://github.com/xxing9703/Iso-Autocorr)

However, TraceBase also supports its own internal format called UniCorr, which contains only the common portions of each
of the above formats that are relevant to TraceBase.  Each of those formats is automatically converted to the UniCorr
format so that the same loading code is used for every peak annotation data source.  Thus, to support a new data type,
all that needs to be developed is a converter to the UniCorr format.

This makes TraceBase more robust to change, and scalable, as support for new data types/formats are requested.
