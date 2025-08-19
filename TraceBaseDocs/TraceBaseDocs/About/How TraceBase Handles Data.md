# How TraceBase Handles Data

TraceBase consists of three basic types of information:

* Unique
* Standardized
* Calculated

![TraceBase data organization diagram](../Attachments/Structure%20of%20Tracebase%20Sketch.png)

**Unique** information consists of identifying sample information.  Individual samples are associated with an Animal,
which is itself part of one or more studies.  A Study is a collection of Animals as defined by the researcher who
uploaded them.  This information is provided by the Researcher in the Samples sheet of the
[Study Doc](../Upload/Study%20Doc.md).  Although samples are organized in this way, data from different studies can be
searched, browsed, or downloaded together in TraceBase.

**Standardized** data refers to Sample and Compound attributes that are kept consistent across datasets in TraceBase.
Examples of these consistent data include compound names, tissue names, researcher names, and key animal attributes
including diet, age, sex, and infusion information.  This also includes protocols for Animal Treatments and mass
spectrometry (MS).  This ensures that data can be compiled, compared, and searched across different studies.
Consistency is ensured by developers that review data submitted for upload.  Standardized data can be added or modified
during a study submission to TraceBase by researchers, but is subject to curator review to ensure consistency and a
standard nomenclature throughout the database.

**Calculated** data (or "derived data") is data that can be affected by changes to or additions of records in the
database.  For example, a researcher discovers previously overlooked serum sample data that they submit after an already
completed (and loaded) submission.  When that new data is loaded, it has the potential to change the results of numerous
calculated values.  TraceBase dynamically maintains these calculated values for constant accuracy.  Calculated data can
be found in three types of output: [PeakData](../Download/About%20the%20Data/Data%20Types/PeakData.md),
[PeakGroups](../Download/About%20the%20Data/Data%20Types/PeakGroups.md), and
[FCirc](../Download/About%20the%20Data/Data%20Types/FCirc.md).  Some calculations rely on other calculated data (e.g.
[Normalized Labeling](../Values/Normalized%20Labeling.md) of a measured compound in a PeakGroup from one sample uses the
[Enrichment Fraction](../Values/Enrichment%20Fraction.md) of the tracer compound in the last serum sample from the same
animal).  Calculated values are generally comparable across experiments.
