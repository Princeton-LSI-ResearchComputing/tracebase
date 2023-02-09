# Normalized Labeling

Found in [PeakGroups](../Types%20of%20Data%20Output/PeakGroups.md)

[GitHub Link](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/c8ef01327429b31a25c9824050487ecb641f491c/DataRepo/models/peak_group_label.py#L171-L202)

The enrichment in this compound normalized to the enrichment in the tracer
compound(s) from the final serum timepoint for that animal.  This is calculated
for each labeled element separately.

`Normalized Labeling = ThisPeakGroup.enrichment_fraction / SerumTracerPeakGroup.enrichment_fraction`

Example:  normalized labeling in alanine (C3H7NO2) and glutamate (C5H9NO4)
measured in the final serum sample and one tissue (quadricep) sample from an
infusion of \[13C3-15N]-alanine:

Tissue | Measured Compound(s) | Labeled Element | Total Abundance | Enrichment Fraction | Enrichment Abundance | Normalized Labeling
-- | -- | -- | -- | -- | -- | --
serum_plasma_tail | alanine | C | 1721209 | 0.275916 | 474909.5 | 1
serum_plasma_tail | alanine | N | 1721209 | 0.344919 | 593677.6 | 1
serum_plasma_tail | glutamate | C | 242215.7 | 0.018 | 4359.787 | 0.065236
serum_plasma_tail | glutamate | N | 242215.7 | 0.053935 | 13063.83 | 0.156369
quadricep | alanine | C | 97694902 | 0.006183 | 604078.4 | 0.02241
quadricep | alanine | N | 97694902 | 0.019993 | 1953218 | 0.057964
quadricep | glutamate | C | 14251992 | 0.007268 | 103580.9 | 0.026341
quadricep | glutamate | N | 14251992 | 0.087257 | 1243586 | 0.252978

In some cases, a labeled element present in a tracer is not found in a measured
compound, and Normalized Labeling is not reported.  For example, lactate
(C3H6O3) can be measured in samples from an animal infused with N-labeled
glutamine (C5H10N2O3), but no nitrogen enrichment or normalized labeling is
reported for lactate.

For infusates with multiple tracers, SerumTracerPeakGroup.enrichment_fraction =
average of enrichment in each serum tracer group.

The `SerumTracerPeakGroup` depends on some edge cases:

- if two serum samples were collected on the same date and same minutes after
  the start of infusion, (ie they are biological replicates), the specific
  value selected is arbitrary.
- if the same `SerumTracerPeakGroup` is repeated in multiple MSruns (appears in
  multiple accucor files), the most recent uploaded data is selected.
