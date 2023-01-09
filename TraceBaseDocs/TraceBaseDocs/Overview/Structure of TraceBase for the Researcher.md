TraceBase consists of three basic types of information: Unique, Standardized, and Calculated.

![image](https://user-images.githubusercontent.com/34348153/202543023-444340ea-0968-457e-9520-8aa4e5b1871e.png)



Unique information consists of identifying sample information.  Individual samples are associated with an Animal, which is itself part of a Study.  A Study is the original collection of Animals as defined by the researcher who uploaded them.  This information is provided by the Researcher in upon upload in the [Sample Information Sheet](../Upload/Sample%20Information%20Sheet.md).  Although samples are organized in this way, data from different studies can be searched, browsed, or downloaded together in TraceBase.

Standardized data refers to Sample and Compound attributes that are kept consistent across datasets in TraceBase.  Examples of these consistent data include compound names, tissue names, researcher names, and key animal attributes including diet, age, sex, and infusion information.  This ensures that data can be compiled and compared across different studies.  Consistency is ensured by developers that review data submitted for upload.

Calculated data are found in three types of output: [PeakData](../Types%20of%20Data%20Output/PeakData.md), [PeakGroups](../Types%20of%20Data%20Output/PeakGroups.md), and [Fcirc](../Types%20of%20Data%20Output/Fcirc.md).  Some calculations rely on other sample attributes (ie [Normalized Labeling](../Values/Normalized%20Labeling.md) uses the [Enrichment](../Values/Enrichment.md) of the tracer compound in the final serum sample for that animal).  Calculated values are generally comparable across experiments.
