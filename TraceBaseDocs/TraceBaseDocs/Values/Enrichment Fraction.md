# Enrichment Fraction

[_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L53-L150)

Found in the [PeakGroups](../Download/About%20the%20Data/Data%20Types/PeakGroups.md) data format.

A weighted average of the fraction of labeled atoms across all peaks in a PeakGroup detected in a particular sample.
E.g. The fraction of labeled carbons in lysine detected in a spleen sample.

`enrichment_fraction = ∑_peak(fraction * labeled_count) / element_count`

Where:

* `∑_peak` stands for the sum across all peaks in a PeakGroup for a particular compound.
* `fraction`: See [Fraction](Fraction.md)
* `labeled_count` is the number of labeled atoms (i.e. isotopomers) of a particular element in any one peak.
* `element_count` is the total number of atoms of a particular element in any one compound (labeled or not).

See the **Enrichment Fraction** column of the PeakGroup table in this [[Enrichment Example]].
