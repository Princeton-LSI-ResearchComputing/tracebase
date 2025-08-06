# FCirc

FCirc data has a row for every combination of serum sample, tracer, and labeled element.

See [For Multiple Tracers or Labeled Elements](#MultipleTracersElements).

## Definition

Circulatory Flux (FCirc) is defined as the steady-state flux of metabolite between tissues and bloodstream(see
[Bartman, TeSlaa, and Rabinowitz](https://doi.org/10.1038/s42255-021-00419-2)).

Typically, FCirc is measured by infusing a labeled tracer to a steady state and measuring its dilution in the animal.
The labeling of the tracer compound in serum and the known rate of tracer infusion are used to calculate FCirc.  There
are key assumptions of steady state and minimal perturbation in typical calculations of FCirc - see the above reference
for details.

## Types of FCirc

A researcher may wish to calculate multiple forms of FCirc depending on their experimental goals.  These forms are based
on whether the tracer infusion is included in the flux (Ra vs Rd, or "rates of appearance versus disappearance" of the
fully labeled tracer) and whether the intact or atom-averaged flux is preferred.  TraceBase calculates all of four of
these and automates the standard calculations for FCirc without making any assumptions.

![Types of FCirc](../../../Attachments/types_of_fcirc.png)
(from Bartman, TeSlaa, and Rabinowitz)

It's important to note however that serum samples are collected at multiple points before sacrifice and TraceBase
calculates FCirc values for **every** serum/plasma sample, regardless of stead-state.  So be aware that the serum sample
with the most accurate FCirc calculations will be derived from the **last** collected serum sample.

The FCirc view in the Advanced Search can be filtered for the "last serum sample" manually, as a row in the search or
using the checkbox shortcut above the results:

![Filter out "previous" serum samples](../../../Attachments/last_serum_sample.png)

It's also important to note that TraceBase will report an Ra value in an infusion that was perturbative.  This can still
be a useful and valid measurement, but it should not be interpreted as the normal circulatory flux (FCirc).

Finally, FCirc can either be normalized to animal body weight (reported as nmol/minute/gram body weight) or not
(nmol/minute/animal).  TraceBase calculates all of these so that the researcher can select the appropriate measurement.

## <a name="MultipleTracersElements"></a>For Multiple Tracers or Labeled Elements

An animal can be infused with multiple tracers and any tracer can have multiple different labeled elements.

FCirc values are calculated separately for each labeled element in each tracer compound.  This was a specific design
choice made to enable comparison of tracer infusions across different studies.  For example, FCirc-intact for carbon can
be directly compared between an animal given U13C-alanine infusion and another animal given U13C-15N-alanine infusion.

* Note that for "intact" FCirc values, this is different than "any transformation".  (e.g. the FCirc-intact for
  U13C-15N-alanine infusion counts U13C-alanine and U13C-15N-alanine as un-transformed).

## Calculations

Each FCirc calculation is performed per labeled element, and requires the following values/metadata to have been
supplied by the researcher:

* Animal body weight
* Serum sample collection time
* MS Run Date
  * Note: reruns take precedence over previous runs
  * If this date is not provided, and the last serum sample was run multiple times, an arbitrary run is selected
* At least 1 serum sample, from which:
  * A peak group has been supplied for every tracer the animal was infused with
  * Note: Additionally, the **intact** FCirc calculations require the detection of a fully labeled tracer in its
    PeakGroup.

### Serum tracers enrichment fraction

The rates of appearance and disappearance of the average labeled state of any measured compound rely on the calculation
of a weighted average of the enrichment fraction of labeled atoms among of all of the infused tracers in the animal's
final serum sample.  E.g. The fraction of labeled carbons among all the final serum sample's tracer compounds.

This calculation is performed for a single labeled element in the following manner:

First, the label enrichment is summed for all of the tracers in the last serum sample:

`serum_tracers_enrichment_sum = Sum for every tracer peak observation (fraction * labeled_count)`

Then the serum tracers' enrichment fraction is calculated with:

`serum_tracers_enrichment_fraction = serum_tracers_enrichment_sum / total_atom_count`

Where:

* `labeled_count` refers to the number of labeled elements in a single tracer observation (/peak).
* `total_atom_count` is the number of occurrences the element summed across all tracers' formulas (labeled or not).
* `fraction`: See [Fraction](../../../Values/Fraction.md)

### Per gram rates of appearance/disappearance

* `Rd_intact_g = infusion_rate * tracer_concentration / fraction`

* `Ra_intact_g = Rd_intact_g - infusion_rate * tracer_concentration`

* `Rd_avg_g = infusion_rate * tracer_concentration / serum_tracers_enrichment_fraction`

* `Ra_avg_g = Rd_avg_g - infusion_rate * tracer_concentration`

### Per animal rates of appearance/disappearance

* `Rd_intact` = `Rd_intact_g * animal_body_weight`

* `Ra_intact` = `Ra_intact_g * animal_body_weight`

* `Rd_avg` = `Rd_avg_g * animal_body_weight`

* `Ra_avg` = `Ra_avg_g * animal_body_weight`
