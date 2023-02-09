# Downloaded Data Formats

## Downloaded Data

Downloaded Data ([PeakData](PeakData.md), [PeakGroups](PeakGroups.md),
[Fcirc](Fcirc.md)) are formatted as TSV (Tab Separated Values).  The
organization of downloaded data is always the same, regardless of any column
filtering that may have been applied in the Advanced Search page.  This makes
downloaded data amenable to other analysis tools outside of TraceBase.

Each row in the TSV of downloaded data includes:

> \# Download Time
> \# Advanced Search Query used to generate the downloaded data
> \# Blank row
> Column Names
> data, where one row = 1 observation

Note that [PeakGroups](PeakGroups.md) and [Fcirc](Fcirc.md) data for samples
from infusions with multiple labeled elements may be split into a separate row
for each element.

In the [Fcirc](Fcirc.md) view, each row is split by serum sample, tracer, and
element.  See [For Multiple Tracers or Labeled
Elements](Fcirc.md#For%20Multiple%20Tracers%20or%20Labeled%20Elements).

## Other downloaded tables

Summary tables found while browsing TraceBase can be downloaded (ie Studies,
Animals, Tissues, Compounds, etc).  These are downloaded in the format selected
by the user (csv, txt, MS-Excel) and the columns included are dependent on what
the user has selected.
