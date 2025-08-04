# FCirc

FCirc data has a row for every combination of serum sample, tracer, and labeled element.

See [For Multiple Tracers or Labeled Elements](FCirc.md#For%20Multiple%20Tracers%20or%20Labeled%20Elements).

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

![Types of FCirc](../Attachments/types_of_fcirc.png)
(from Bartman, TeSlaa, and Rabinowitz)

It's important to note however that serum samples are collected at multiple points before sacrifice and TraceBase
calculates FCirc values for **every** serum/plasma sample, regardless of stead-state.  So be aware that the serum sample
with the most accurate FCirc calculations will be derived from the **last** collected serum sample.

The FCirc view in the Advanced Search can be filtered for the "last serum sample" manually, as a row in the search or
using the checkbox shortcut above the results:

![Filter out "previous" serum samples](../Attachments/last_serum_sample.png)

It's also important to note that TraceBase will report an Ra value in an infusion that was perturbative.  This can still
be a useful and valid measurement, but it should not be interpreted as the normal circulatory flux (FCirc).

Finally, FCirc can either be normalized to animal body weight (reported as nmol/minute/gram body weight) or not
(nmol/minute/animal).  TraceBase calculates all of these so that the researcher can select the appropriate measurement.

## For Multiple Tracers or Labeled Elements

An animal can be infused with multiple tracers and any tracer can have multiple different labeled elements.

FCirc values are calculated separately for each labeled element in each tracer compound.  This was a specific design
choice made to enable comparison of tracer infusions across different studies.  For example, FCirc-intact for carbon can
be directly compared between an animal given U13C-alanine infusion and another animal given U13C-15N-alanine infusion.

* Note that for "intact" FCirc values, this is different than "any transformation".  (e.g. the FCirc-intact for
  U13C-15N-alanine infusion counts U13C-alanine and U13C-15N-alanine as un-transformed).

## Calculations

[GitHub Link](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/1a7e9f9a05b01e00fdb83b4e1e97ef54c6588302/DataRepo/models/peak_group_label.py#L464-L477)

*for each element:*

* `Rd_g` = `rate_disappearance_intact_per_gram = infusion_rate *
  tracer_concentration / fraction`

* `Ra_g` = `rate_appearance_intact_per_gram
  = rate_disappearance_intact_per_gram - infusion_rate * tracer_concentration`

* `Rd_avg_g` = `rate_disappearance_average_per_gram = tracer_concentration *
  infusion_rate * tracer_concentration /
  enrichment_fraction_of_labeled_element`

* `Ra_avg_g` = `rate_appearance_average_per_gram
  = rate_disappearance_average_per_gram - infusion_rate * tracer_concentration
  / enrichment_fraction_of_labeled_element`

* `Rd` = `rate_disappearance_intact_per_gram * animal_body_weight`

* `Ra` = `rate_appearance_intact_per_gram * animal_body_weight`

* `Rd_avg` = `rate_disappearance_average_per_gram * animal_body_weight`

* `Ra_avg` = `rate_appearance_average_per_gram * animal_body_weight`
