# FCirc Rates

Each FCirc calculation is performed per labeled element, and requires the following values/metadata to have been
supplied by the researcher:

<!-- markdownlint-disable MD007 -->
* Animal body weight
* Serum sample collection time
* MS Run Date
    * Note: reruns take precedence over previous runs
    * If this date is not provided, and the last serum sample was run multiple times, an arbitrary run is selected
* At least 1 serum sample, from which:
    * A peak group has been supplied for every tracer the animal was infused with
    * Note: Additionally, the **intact** FCirc calculations require the detection of a fully labeled tracer in its
      PeakGroup.
<!-- markdownlint-enable MD007 -->

## <a name="serum_tracers_enrichment_fraction"></a>Serum Tracers Enrichment Fraction

[_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/animal_label.py#L89-L171)

The rates of appearance and disappearance of the average labeled state of any measured compound rely on the calculation
of a weighted average of the enrichment fraction of labeled atoms among of all of the infused tracers in the animal's
final serum sample.  E.g. The fraction of labeled carbons among all the final serum sample's tracer compounds.

This calculation is performed for a single labeled element in the following manner:

The label enrichment is summed for all of the tracers in the last serum sample, and is divided by the total count of the
element among all the tracers' formulas (labeled or not).

`serum_tracers_enrichment_fraction = ∑_tracer_peak(fraction * labeled_count) / element_count`

Where:

* `∑_tracer_peak` stands for the sum across all tracer peaks/observations.
* `labeled_count` refers to the number of labeled elements in a single tracer observation (/peak).
* `element_count` is the number of occurrences the element summed across all tracers' formulas (labeled or not).
* `fraction`: See [[Fraction]]

## Rates of Appearance/Disappearance (`Ra`/`Rd`)

### Intact

#### Per Gram

* <a name="Rd_intact_g"></a>`Rd_intact_g = infusion_rate * tracer_concentration / fraction` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L455-L477))

* <a name="Ra_intact_g"></a>`Ra_intact_g = Rd_intact_g - infusion_rate * tracer_concentration` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L481-L494))

#### Per Animal

* <a name="Rd_intact"></a>`Rd_intact = Rd_intact_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L501-L509))

* <a name="Ra_intact"></a>`Ra_intact = Ra_intact_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L513-L523))

### Average

#### Per Gram

* <a name="Rd_avg_g"></a>`Rd_avg_g = infusion_rate * tracer_concentration / serum_tracers_enrichment_fraction` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L527-L552))

* <a name="Ra_avg_g"></a>`Ra_avg_g = Rd_avg_g - infusion_rate * tracer_concentration` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L556-L579))

#### Per Animal

* <a name="Rd_avg"></a>`Rd_avg = Rd_avg_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L583-L595))

* <a name="Ra_avg"></a>`Ra_avg = Ra_avg_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L599-L611))
