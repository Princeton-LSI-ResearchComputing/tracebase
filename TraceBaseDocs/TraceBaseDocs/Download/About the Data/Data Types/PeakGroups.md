# PeakGroups

PeakGroups report the abundance and enrichment summated from each isotope associated with a measured compound (as listed
in [[PeakData]]).  Thus, they report values for the entire "group" of peaks for each compound.

PeakGroup values are almost always reported in a single row for each observed compound.  Multiple PeakGroups rows are
generated when the animal was infused with a tracer(s) that contains multiple labeled elements.  For example, a mouse
could be infused with a single amino acid labeled with both 13C and 15N.  In this case, TraceBase reports a separate
PeakGroup for each labeled element.  This ensures that PeakGroups are always comparable between experiments, even when
comparing a mouse infused with 13C-15N tracers to a mouse given only 13C.  Any analysis that relies on considering both
labeled elements simultaneously can be performed on [[PeakData]].

See this [example](../../../Values/Enrichment%20Example.md) for a measured compound with multiple labeled elements.

Some key values reported in PeakGroups are:

* [Total Abundance](../../../Values/Total%20Abundance.md)
* [Enrichment Fraction](../../../Values/Enrichment%20Fraction.md)
* [Enrichment Abundance](../../../Values/Enrichment%20Abundance.md)
* [Normalized Labeling](../../../Values/Normalized%20Labeling.md)

Downloaded PeakGroups data has a [standard format](../Format%20of%20Downloaded%20Data.md).
