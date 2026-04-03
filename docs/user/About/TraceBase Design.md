# TraceBase Design

The study of metabolism often includes experiments where an animal is infused with a heavy isotope of a nutrient, and
biological samples are collected and analyzed using a mass spectrometer.  Historically, these costly and time-consuming
experiments were only analyzed with a specific experimental question in mind, and much of the data available went
unused.  TraceBase was designed to capture, organize, and share data generated in these isotope tracing experiments.  It
is ideal for both the researcher who has completed all of their experiments, as well as the researcher whose work is in
progress.

## Key Motivations

* Reduce the barrier for scientific data submission.
* Maintain data integrity and calculation accuracy or curated data, despite changes.
* Enable holistic analysis across many experiments.
* Leverage unanalyzed peak data produced in every experiment by making raw data accessible.

### Reducing the Submission Barrier

Metabolomics studies involve lots of complex data, making the organizational burden, high.   As such, metabolomics
repositories often have exacting submission requirements that create large barriers to data submission.  TraceBase was
developed around flexible submission requirements and a guided collaborative curation process, while providing tools to
empower users to prepare data and easily diagnose problems and fix them iteratively/incrementally.  In designing this
system, the following guiding principles were used:

* Incomplete submissions are OK and can be added to later.
* Imperfect data can be submitted and fixed as you go via the curation process.
* Data must be private to the laboratory (encouraging submission prior to publication).
* Focused design on infusions of isotope tracers into animals.

### Ensure Data Integrity and Accuracy

TraceBase monitors the data in its studies in such way that ensures accurate calculations, despite changes to the data
as a study is compiled and/or added-to.  If data changes can affect how other data calculations are made, calculations
are re-done.  For example, if a later serum sample is added, it represents a better steady state, so normalized labeling
calculations are updated.

### Enable Holistic Cross-Experiment Analyses

TraceBase breaks down common calculations (like FCirc) by labeled element, so that when tracers in different experiments
have different numbers of labeled elements, their results can still be compared.  This ensures consistent analysis of
single experiments and enables holistic analysis across many experiments.

### Leverage Unanalyzed Raw Data

Since LCMS data is expensive and produces more data than is necessary to answer the immediate scientific question at
hand, much of its output goes unused, but if made accessible, can be applied to new scientific questions.  By linking
this raw data to its related experimental data, it can be searched, downloaded, and used in new studies.

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

<!-- markdownlint-disable MD007 -->
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
<!-- markdownlint-enable MD007 -->
