# Format of Downloaded Data

Downloaded data (PeakGroups, PeakData, and Fcirc) are formatted as TSV (Tab Separated Values).  The organization of downloaded data is always the same, regardless of any column filtering that may have been applied in the Advanced Search page.  This makes downloaded data amenable to other analysis tools outside of TraceBase.

Three header rows are always included in any downloaded TSV data (first character is '#')
1. Download Time
2. Advanced Search Query used to generate the downloaded data
3. Blank row

The fourth row lists column names.

The remaining rows are data, where one row = 1 observation.

Note that PeakGroup and Fcirc data for samples from infusions with multiple labeled elements may be split into a separate row for each element.
