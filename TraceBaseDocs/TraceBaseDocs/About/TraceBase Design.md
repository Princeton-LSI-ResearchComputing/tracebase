# TraceBase Design

TraceBase was built to solve a practical problem, inherent to how the science of metabolomics is performed.  Each
experiment is designed around a specific question or questions about specific metabolites.  However, when you process
samples through liquid chromatography and mass spectrometry, the instruments yield information on much more than just
the metabolites of interest.  Much of the data often goes unscrutinized/uninspected/unused, because it may not relate
directly to the question the experimentalist is currently trying to answer.  This is valuable data that can potentially
be applied to other scientific questions about the same metabolites, but if those questions arise much later, perhaps
_years_ later, it may be difficult to track down that data and all associated metadata.  It could be spread across
computers, external hard drives, lab notebooks, or even only exist in one resercher's memory.  And that researcher and
those data sources may have moved on, thus tracking down that potentially useful data can be arduous and time consuming.

Tracebase can serve as a central repository to store all of that unused data in one central location, to be searched and
analyzed.  It even enables the ability to answer broader holistic scientific questions that could not be asked when the
data is not all in one accessible location, or when the calculations used in the various analyses were performed using
different methods.

Such repositories, historically speaking, have exacting submission requirements that tend to be a barrier to data
submission.  The data can be complex and challenging.  TraceBase endeavors to smooth all of that out, which brings us to
the guiding general requirements for the development of TraceBase.

## <a name="Goals"></a>Goals

### End User Goals

TraceBase was developed with a primary focus the following development goals/features that were determined to be
critical to mission success:

* Ease of Data Submission
* Robust Search Capabilities
* Data Accuracy
* Data Inter-comparability

### Maintenance Goals

And more generally, we expect TraceBase to grow and evolve with ever increasing data, feature requests, data types, and
changes to existing data.  And we are aware that the curation of complex data can be a slow and difficult task.  In
order for people to install and maintain their own instance of TraceBase for themselves, overhead needed to be simple
and minimized.  Thus, this project was also tackled with these core principles in mind:

* Minimize Overhead
    * Empower researchers to solve their own submission issues (e.g. the validation page) so that they don't have to wait
      through a time consuming curation process that involves a slow correspondence between the curator and the researcher
      as they work through issues in the data.  By enabling researchers to solve these problems on their own through
      thoughtfully constructed and streamlined error messages, this reduces the maintenance efforts necessary to keep
      TraceBase working, shortens the time between submission and load, and reduces the overhead for maintainers to
      service study submissions.
    * Automate the upkeep of calculated values when data changes.
* Adaptability
    * TraceBase was made to be largely data-independent.  E.g. Add a field to a table in the database and features like
      the list views, the loading scripts, or the advanced search either automatically adapt or only require relatively
      simple configuration changes.
    * If data in a study is changed, removed, or added, key changes are monitored to trigger automatic updates of
      derived/calculated values.
* Scalability
    * From a development standpoint, this is similar to the adaptability goal.
    * From a database size standpoint, this means ensuring that performance doesn't wane as data is added.
