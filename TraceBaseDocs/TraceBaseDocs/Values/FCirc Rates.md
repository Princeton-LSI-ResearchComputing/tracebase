# FCirc Rates

Each FCirc calculation is performed per labeled element, and requires the following values/metadata to have been
supplied by the researcher:

<!-- markdownlint-disable MD007 -->
* Animal Body [Weight](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#weight)
* [Infusion Rate](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#infusionrate)
* At least 1 serum sample, with:
    * [tissue](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#tissue) name
      containing "serum".
    * [collection time](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#coltim)
    * A peak group for every tracer the animal was infused with
    * Note: The **intact** FCirc calculations require the detection of a fully labeled tracer in each of the infused
      tracers' PeakGroups.
* MS [Run Date](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#rundate)
    * Note: reruns take precedence over previous runs
    * If this date is not provided, and the last serum sample was run multiple times, an arbitrary run is selected
<!-- markdownlint-enable MD007 -->

## Rates of Appearance/Disappearance (`Ra`/`Rd`)

For more information of the variables in the equations below:

* [`animal_body_weight`](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#weight)
* [`enrichment_fraction`](Enrichment%20Fraction.md)
* [`fraction`](Fraction.md)
* [`infusion_rate`](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#infusionrate)
* [`tracer_concentration`](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#conc)

### Intact

#### Per Gram

* <a name="Rd_intact_g"></a>`Rd_intact_g = infusion_rate * tracer_concentration / fraction` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L455-L477))

* <a name="Ra_intact_g"></a>`Ra_intact_g = Rd_intact_g - infusion_rate * tracer_concentration` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L481-L494))

#### Per Animal

* <a name="Rd_intact"></a>`Rd_intact = Rd_intact_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L501-L509))

* <a name="Ra_intact"></a>`Ra_intact = Ra_intact_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L513-L523))

### Average

#### Per Gram

* <a name="Rd_avg_g"></a>`Rd_avg_g = infusion_rate * tracer_concentration / enrichment_fraction` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L527-L552))

* <a name="Ra_avg_g"></a>`Ra_avg_g = Rd_avg_g - infusion_rate * tracer_concentration` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L556-L579))

#### Per Animal

* <a name="Rd_avg"></a>`Rd_avg = Rd_avg_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L583-L595))

* <a name="Ra_avg"></a>`Ra_avg = Ra_avg_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L599-L611))
