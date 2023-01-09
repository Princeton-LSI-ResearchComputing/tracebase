# Total Abundance

Found in [PeakGroups](../Types%20of%20Data%20Output/PeakGroups.md)

[Github Link](https://github.com/Princeton-LSI-ResearchComputing/tracebase/blob/1a7e9f9a05b01e00fdb83b4e1e97ef54c6588302/DataRepo/models/peak_group.py#L53-L65)

Total ion counts for this compound. Accucor provides this in the tab "pool size". Sum of the corrected_abundance of all PeakData for this PeakGroup.

`total_abundance = Sum(corrected_abundance)`