# Total Abundance

[_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group.py#L78-L90)

Found in the [PeakGroups](../Download/About%20the%20Data/Data%20Types/PeakGroups.md) data format.

Total ion counts for this compound.  AccuCor provides this in the tab "pool size".  Sum of the `corrected_abundance` of
all peaks (i.e. PeakData) for this PeakGroup representing the compound.

`total_abundance = ∑_peak(corrected_abundance)`

Where:

* `∑_peak` stands for the sum across all peaks in a PeakGroup for a particular compound.
* See [[Corrected Abundance]]
