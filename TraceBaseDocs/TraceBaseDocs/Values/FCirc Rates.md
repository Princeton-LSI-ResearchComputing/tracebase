# Circulatory fluxes (FCirc)

Each FCirc calculation is performed per labeled element, and requires the following values/metadata to have been
supplied by the researcher:

<!-- markdownlint-disable MD007 -->
* Animal Body [Weight](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#weight)
* [Infusion Rate](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#infusionrate)
* MS [Run Date](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#rundate)
    * Note: reruns take precedence over previous runs
    * If this date is not provided, and the last serum sample was run multiple times, an arbitrary run is selected
* At least 1 serum sample, with:
    * [tissue](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#tissue) name
      containing "serum".
    * [collection time](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#coltim)
    * A peak group for every tracer the animal was infused with
    * Note: The **intact** FCirc calculations require the detection of a fully labeled tracer in each of the infused
      tracers' PeakGroups.
<!-- markdownlint-enable MD007 -->

## Rates of Appearance/Disappearance (`Ra`/`Rd`)

TraceBase compute four distinct values for circulatory fluxes i) intact rate of disappearance $R_{d,intact}$, ii) intact rate of appearance $R_{d,intact}$, 
iii) averaged rate of disappearance $R_{d,avg}$ and iv) averaged rate of appearance $R_{a,avg}$. Rates of disappearance measure the overall rate of appearance for a metabolic compound,
whereas rate of appearances are corrected for the tracer infusion rate. 

Intact denotes that the rate was computed using the labeling fraction of the infused massisomer. For example, if uniformly labeled glucose (U13-glucose) is infused, 
the intact rates are computed using the M+6 (C_labled = 6) labeling fraction of circulating glucose.
Thus, the intact rate of appearance $R_{a,intact}$, 
and the intact rate of disappearance $R_{d,intact}$ are computed as:

$$
R_{a,intact} = \frac{1}{ L_{intact}} T 
$$
R_{a,intact} = \frac{(1-L_{intact})} { L_{intact} } T
$$

where $L_{intact}$ ([`fraction`](Fraction.md)) is the labeling fraction of the infused massisomer. $T$ is the molar infusion rate, computed from the weight normalized infusion rate 
$I$ ([`infusion_rate`](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#infusionrate)) 
and the tracer concentration $C$ ([`tracer_concentration`](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#conc)):

$$
T = I \times C
$$

Average denotes that the rate was computed using an averaged labeling fraction across all massisomers of the infused tracer compound $L_{avg}$ ([`enrichment_fraction`](Enrichment%20Fraction.md)).
The average labeling fraction, also termed enrichment fraction, is computed from the weighted average of all labeling fractions. We note that averaged labeling fractions are computed for either nitrogen or carbon.

$$
L_{avg} = \frac{\sum_i^M i\, L_{i} }{M}
$$

where $M$ is either the number of nitrogens and number of carbons, $L_i$ is the labelling fraction for a mass isomer that contains $i$ heavy nitrogens or carbons. Using the averaged labeling fraction,
averaged Rates are computed as follows:


$$
R_{a,intact} = \frac{1}{ L_{avg}} T
$$

$$
R_{a,intact} = \frac{(1-L_{avg})} { L_{avg} } T
$$


By default, circulatory fluxes are based on the animal weight normalized in fusion rate $I$ resulting in units nmol/min/gBW. In some cases,e.g.  when body weight primarily varies due to fat accumulation,
it is advantageous compare circular fluxes on a per animal instead of per weight basis. For this case TraceBase offers per animal values for each of the above defined rates. 
Fhese values are computed from the weight normalized rates, by multiplying with the animal weight $W$ ([`animal_body_weight`](../Upload/How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md#weight)):

$$
R_{animal} = R \times W
$$


Links to the source code of these calculations can be found here:

* <a name="Rd_intact_g"></a>`Rd_intact_g = infusion_rate * tracer_concentration / fraction` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L455-L477))
* <a name="Ra_intact_g"></a>`Ra_intact_g = Rd_intact_g - infusion_rate * tracer_concentration` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L481-L494))


* <a name="Rd_avg_g"></a>`Rd_avg_g = infusion_rate * tracer_concentration / enrichment_fraction` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L527-L552))
* <a name="Ra_avg_g"></a>`Ra_avg_g = Rd_avg_g - infusion_rate * tracer_concentration` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L556-L579))

* <a name="Rd_avg"></a>`Rd_avg = Rd_avg_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L583-L595))
* <a name="Ra_avg"></a>`Ra_avg = Ra_avg_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L599-L611))

* <a name="Rd_intact"></a>`Rd_intact = Rd_intact_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L501-L509))
* <a name="Ra_intact"></a>`Ra_intact = Ra_intact_g * animal_body_weight` ([_source_](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/241e47de6a06df543ad73c6ceb82d758ce373cbe/DataRepo/models/peak_group_label.py#L513-L523))


