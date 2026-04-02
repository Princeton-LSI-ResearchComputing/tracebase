# Calculated Values Glossary

Terms and calculations in TraceBase are based on Bartman, TeSlaa and Rabinowitz
["Quantitative flux analysis in mammals"](https://doi.org/10.1038/s42255-021-00419-2).

The following list of calculated values includes a short definition, pseudocode of calcuations, examples, and a GitHub
link to the code where the term is calculated.

They are organized according to the type of Output table where they are found.

<!-- markdownlint-disable MD007 -->
## [PeakData](../Download/About%20the%20Data/Data%20Types/PeakData.md)
* [[Corrected Abundance]]
* [[Fraction]]

## [PeakGroups](../Download/About%20the%20Data/Data%20Types/PeakGroups.md)
* [[Total Abundance]]
* [[Normalized Labeling]]
    * [Serum Tracers Enrichment Fraction](Normalized%20Labeling.md#serum_tracers_enrichment_fraction)
* Enrichment of a Label in a Measured Compound in a PeakGroup
    * [[Enrichment Fraction]]
    * [[Enrichment Abundance]]
    * [[Enrichment Example]]

## [FCirc](../Download/About%20the%20Data/Data%20Types/FCirc.md)
* Intact FCirc
    * Per Gram
        * [Intact Weight Normalized Rate of Disappearance (`Rd_intact_g`)](FCirc%20Rates.md#Rd_intact_g)
        * [Intact Weight Normalized Rate of Appearance (`Ra_intact_g`)](FCirc%20Rates.md#Ra_intact_g)
    * Per Animal
        * [Intact Animal Normalized Rate of Disappearance (`Rd_intact`)](FCirc%20Rates.md#Rd_intact)
        * [Intact Animal Normalized Rate of Appearance (`Ra_intact`)](FCirc%20Rates.md#Ra_intact)
* Average FCirc
    * Per Gram
        * [Average Weight Normalized Rate of Disappearance (`Rd_avg_g`)](FCirc%20Rates.md#Rd_avg_g)
        * [Average Weight Normalized Rate of Appearance (`Ra_avg_g`)](FCirc%20Rates.md#Ra_avg_g)
    * Per Animal
        * [Average Animal Normalized Rate of Disappearance (`Rd_avg`)](FCirc%20Rates.md#Rd_avg)
        * [Average Animal Normalized Rate of Appearance (`Ra_avg`)](FCirc%20Rates.md#Ra_avg)
<!-- markdownlint-enable MD007 -->
