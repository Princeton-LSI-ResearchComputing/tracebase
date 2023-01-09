# Fcirc

## Definition
Circulatory Flux (Fcirc) is defined as the steady-state flux of metabolite between tissues and bloodstream(see [Bartman, TeSlaa, and Rabinowitz](https://doi.org/10.1038/s42255-021-00419-2).  Typically, Fcirc is measured by infusing a labeled tracer to a steady state and measuring its dilution in the animal.  The labeling of the tracer compound in serum and the known rate of tracer infusion are used to calculate Fcirc.  There are key assumptions of steady state and minimal perturbation in typical calculations of Fcirc - see the above reference for details.

## Types of Fcirc
A researcher may wish to calculate multiple forms of Fcirc depending on their experimental goals.  These forms are based on whether the tracer infusion is included in the flux (Ra vs Rd) and whether the intact or atom-averaged flux is preferred.  TraceBase calculates all of four of these.

![image](https://user-images.githubusercontent.com/34348153/205693110-8e852f8f-0c27-456e-a42c-e705e42ff72a.png)
(from Bartman, TeSlaa, and Rabinowitz)

TraceBase automates the standard calculations for Fcirc without making any assumptions.  Note that **TraceBase attempts to calculate Fcirc for every serum/plasma sample, so it will sometimes estimate Fcirc that is invalid (ie before steady state)**.  In other cases, TraceBase will report an Ra value in an infusion that was perturbative.  This can still be a useful and valid measurement, but it should not be interpreted as the normal circulatory flux (Fcirc).

Finally, Fcirc can either be normalized to animal body weight (reported as nmol/minute/gram body weight) or not (nmol/minute/mouse).  TraceBase calculates all of these so that the researcher can select the appropriate measurement.

## For Multiple Tracers or Labeled Elements

If multiple tracers are infused in one animal, Fcirc values are calculated for each tracer compound.

If multiple elements are labeled in a given tracer, Fcirc values are calculated separately for each element.  This was a specific design choice made to enable comparison of tracer infusions across different studies.  For example, Fcirc-intact for carbon can be directly compared between an animal given U13C-alanine infusion and another animal given U13C-15N-alanine infusion.

 - Note that for “intact” Fcirc values, this is different than “any transformation”.  (e.g. the Fcirc-intact for U13C-15N-alanine infusion counts U13C-alanine and U13C-15N-alanine as un-transformed).

## Calculations

[Github Link](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/1a7e9f9a05b01e00fdb83b4e1e97ef54c6588302/DataRepo/models/peak_group_label.py#L464-L477)

*for each element:*

* Rd_g = `rate_disappearance_intact_per_gram = infusion_rate * tracer_concentration / fraction`

* Ra_g = `rate_appearance_intact_per_gram = rate_disappearance_intact_per_gram - infusion_rate * tracer_concentration`

* Rd_avg_g = `rate_disappearance_average_per_gram = tracer_concentration * infusion_rate * tracer_concentration / enrichment_fraction_of_labeled_element`

* Ra_avg_g = `rate_appearance_average_per_gram = rate_disappearance_average_per_gram - infusion_rate * tracer_concentration / enrichment_fraction_of_labeled_element`

* Rd = `rate_disappearance_intact_per_gram * animal_body_weight`

* Ra = `rate_appearance_intact_per_gram * animal_body_weight`

* Rd_avg = `rate_disappearance_average_per_gram * animal_body_weight`

* Ra_avg = `rate_appearance_average_per_gram * animal_body_weight`

