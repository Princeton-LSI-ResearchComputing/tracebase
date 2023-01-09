# Enrichment Abundance

Found in [PeakGroups](../Types%20of%20Data%20Output/PeakGroups.md)

[Github Link](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/c8ef01327429b31a25c9824050487ecb641f491c/DataRepo/models/peak_group_label.py#L155-L167)

The abundance (ion counts) of labeled atoms in this compound, on a per-element basis.

For each element: `enrichment_abundance = peak_group.total_abundance * enrichment_fraction_of_labeled_element`
