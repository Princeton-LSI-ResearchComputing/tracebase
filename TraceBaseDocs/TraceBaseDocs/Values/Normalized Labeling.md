# Normalized Labeling

[_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L170-L202)

Found in the [PeakGroups](../Download/About%20the%20Data/Data%20Types/PeakGroups.md) data format.

Normalized Labeling is the enrichment of a label in a compound normalized to the enrichment of that label among the
tracer compound(s) from the final serum timepoint for an animal.  This is calculated for each labeled element
separately.

`normalized_labeling = enrichment_fraction / serum_tracers_enrichment_fraction`

See:

* [[Enrichment Fraction]]
* [Serum Tracers Enrichment Fraction](FCirc%20Rates.md#serum_tracers_enrichment_fraction)

## Example

normalized labeling in alanine (C3H7NO2) and glutamate (C5H9NO4) measured in the final serum sample and one tissue
(quadricep) sample from an infusion of \[13C3-15N]-alanine:

Tissue | Measured<br>Compound(s) | Labeled<br>Element | Total<br>Abundance | Enrichment<br>Fraction | Enrichment<br>Abundance | Normalized<br>Labeling
-- | -- | -- | -- | -- | -- | --
serum_plasma_tail | alanine | C | 1721209 | 0.275916 | 474909.5 | 1
serum_plasma_tail | alanine | N | 1721209 | 0.344919 | 593677.6 | 1
serum_plasma_tail | glutamate | C | 242215.7 | 0.018 | 4359.787 | 0.065236
serum_plasma_tail | glutamate | N | 242215.7 | 0.053935 | 13063.83 | 0.156369
quadricep | alanine | C | 97694902 | 0.006183 | 604078.4 | 0.02241
quadricep | alanine | N | 97694902 | 0.019993 | 1953218 | 0.057964
quadricep | glutamate | C | 14251992 | 0.007268 | 103580.9 | 0.026341
quadricep | glutamate | N | 14251992 | 0.087257 | 1243586 | 0.252978

In some cases, a labeled element present in a tracer is not found in a measured compound, and Normalized Labeling is not
reported.  For example, lactate (C3H6O3) can be measured in samples from an animal infused with N-labeled glutamine
(C5H10N2O3), but no nitrogen enrichment or normalized labeling is reported for lactate.

For infusates with multiple tracers, the serum tracers enrichment fraction is the average of enrichment in each serum
tracer group.

The selection of the serum tracer peak groups to use, when multiple instances have been picked, depends on the
following:

* If multiple serum samples were collected on the same date and same minutes after the start of infusion, (i.e. they are
  biological replicates), the specific tracer peak group selected for each tracer is arbitrary.
* If a serum tracer peak group is present in multiple MSruns (appears in multiple AccuCor files), the most recently
  uploaded data is selected.
