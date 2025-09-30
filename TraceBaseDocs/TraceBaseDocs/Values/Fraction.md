# Fraction

[_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_data.py#L52-L69)

Found in the [PeakData](../Download/About%20the%20Data/Data%20Types/PeakData.md) data format.

The corrected abundance of the labeled element in this PeakData as a fraction of the total abundance of this isotopomer
in a PeakGroup for a particular compound.  AccuCor calculates this as "Normalized", but Tracebase calculates this value
as `fraction` to avoid confusion with other variables like `normalized_labeling`.

`fraction = corrected_abundance / total_abundance`

See:

* [Corrected Abundance](Corrected%20Abundance.md)
* [Total Abundance](Total%20Abundance.md)

## Example

Alanine (C3H7NO2) measured with 13C and 15N labeling:

Labeled Element:Count | Corrected Abundance | Fraction
-- | -- | --
C:0; N:0 | 1090437 | 0.633529
C:0; N:1 | 154827.9 | 0.089953
C:1; N:0 | 0 | 0
C:1; N:1 | 0 | 0
C:2; N:0 | 3105.043 | 0.001804
C:2; N:1 | 0 | 0
C:3; N:0 | 33989.78 | 0.019748
C:3; N:1 | 438849.7 | 0.254966
