# How do I analyze data downloaded from TraceBase?

Downloaded data is formatted as TSV (tab separated values). [Format of Downloaded Data](Download/Format%20of%20Downloaded%20Data.md)
- PeakData
- PeakGroups
- Fcirc

## Analyze in Excel

TSV can be opened in Excel in multiple ways:
- Open a blank workbook, drag and drop the TSV file into the workbook.
- Excel > Open > Browse > select tsv file (you may need to enable "all files") > follow import wizard using "Delimited" settings

Excel PivotTables are a powerful tool to quickly browse TraceBase data.  To create a PivotTable, follow these recommended steps:
1. Select all > copy and paste to a second worksheet
2. In the new worksheet, delete the header rows so that only column names remain
3. Select all > Insert > PivotChart > OK
   1. This generates a new worksheet with the PivotTable and PivotChart

Select the fields you are interested in, drag and drop them into the 'Filters', 'Legend', 'Axis', or 'Values' areas.

Notice how Excel PivotTables can be used to quickly reorganize data in a format amenable to copying and pasting into a GraphPad table.

## Analyze in R
Downloaded data is formatted in a 'tidy' way where each observation is in one row.


## Analyze in Python
