# PeakGroups

Individual peaks in the mass spec data are grouped for every detected compound.  Since an animal is infused with a
compound containing one or more radio labeled elements, and the animal's biochemistry has metabolized that compound to
produce various other compounds, those isotopes end up in other compounds with mutiple different states of incorporation
of the labeled elements.  As such, any measured compound has multiple peaks associated with it.  Those individual peaks
(which are represented in the [[PeakData]] format) are grouped by compound to produce the PeakGroups output to summarize
or combine all isotopomers for a given compound.

TraceBase makes it possible for all PeakGroups to be directly compared to one other, even if the tracer(s) contain
multiple different labeled elements, thus the data in a PeakGroups file is element-specific, so the PeakGroups file
contains a row for each compound and labeled element combination.

Some key values reported in PeakGroups are:

* [Total Abundance](../../../Values/Total%20Abundance.md)
* [Enrichment Fraction](../../../Values/Enrichment%20Fraction.md)
* [Enrichment Abundance](../../../Values/Enrichment%20Abundance.md)
* [Normalized Labeling](../../../Values/Normalized%20Labeling.md)

Downloaded PeakGroups data has a [standard format](../Format%20of%20Downloaded%20Data.md).
