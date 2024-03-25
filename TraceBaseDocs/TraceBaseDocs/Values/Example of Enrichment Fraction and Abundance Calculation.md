# Example of Enrichment Fraction and Abundance Calculation

This is an example for a measured compound alanine (C3H7NO2) measured with 13C
and 15N labeling.

## PeakData

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

## PeakGroup

Sample | Tissue | Measured Compound | Labeled Element | Total Abundance | Enrichment Fraction | Enrichment Abundance
-- | -- | -- | -- | -- | -- | --
sampleid | serum_plasma_tail | alanine | C | 1721209.0 | 0.2759 | 474909.4989
sampleid | serum_plasma_tail | alanine | N | 1721209.0 | 0.3449 | 593677.6307

Note that PeakGroup data is split into a separate row for each element.
