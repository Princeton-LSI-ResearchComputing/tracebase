# Enrichment Fraction

[_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L53-L150)

Found in the [PeakGroups](../Download/About%20the%20Data/Data%20Types/PeakGroups.md) data format.

A weighted average of the fraction of labeled atoms (e.g. Carbons, Nitrogens, Oxygens, etc.) across all peaks in a PeakGroup detected in a particular sample.
E.g. The fraction of labeled carbons in lysine detected in a spleen sample.

$$
L_{avg} = \frac{\sum_i^M i\, L_{i} }{M}
$$

Where:

* $i$ is the number of labeled atoms (i.e. massisomers) of a particular element in any one peak.
* $L_i$ is the fractional abundance of the massisomer peak see [Fraction](Fraction.md)
* $M$ is the total number of atoms of a particular element in any one compound (labeled or not).

See the **Enrichment Fraction** column of the PeakGroup table in this [Enrichment Example](Enrichment%20Example.md).
