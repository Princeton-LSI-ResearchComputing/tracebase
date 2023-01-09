# Enrichment
Found in [PeakGroups](../Types%20of%20Data%20Output/PeakGroups.md)

[Github Link](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/c8ef01327429b31a25c9824050487ecb641f491c/DataRepo/models/peak_group_label.py#L54-L151)

A weighted average of the fraction of labeled atoms for this PeakGroup (e.g. the fraction of carbons that are labeled in this PeakGroup compound). Calculated on a per-element basis (e.g. "C" enrichment and "N" enrichment are independent of each other).

For each element:
`Sum of all (PeakData.fraction * PeakData.labeled_count) / PeakGroup.Compound.num_atoms(PeakData.labeled_element)`
