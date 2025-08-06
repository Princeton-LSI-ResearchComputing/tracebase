# Fraction

Found in [PeakData](../Download/About%20the%20Data/Data%20Types/PeakData.md)

[GitHub Link](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/86fee46e86add535348a2d717324c3465b8d5d9b/DataRepo/models/peak_data.py#L44-L58)

The corrected abundance of the labeled element in this PeakData as a fraction
of the total abundance of this isotopomer in this PeakGroup. Accucor calculates
this as "Normalized", but TraceBase renames it to "fraction" to avoid confusion
with other variables like "normalized labeling".

`fraction = peak_corrected_abundance / peak_group_total_abundance`

Example:  alanine (C3H7NO2) measured with 13C and 15N labeling:

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
