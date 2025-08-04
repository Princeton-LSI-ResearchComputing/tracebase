# Format of Downloaded Data

Downloaded data is available in 2 formats:

1. Table data in TSV format (Tab Separated Values)
2. Compressed ZIP format containing mzXML files

The data contained in the downloads are available in 5 basic varieties:

* From the **Download menu** at the top of every page, and from the **TSV download button in the advanced search**:
  * [PeakData](PeakData.md) (TSV)
  * [PeakGroups](PeakGroups.md) (TSV)
  * [FCirc](FCirc.md) (TSV)
* From the **mzXMLs download button in the advanced search when performing PeakGroups and PeakData searches only**:
  * mzXMLs (ZIP of mzXML files and TSV containing metadata - See the **ZIP** heading, below)
* From summary tables, accessed via the links in the left sidebar^

See [How to Download](How%20to%20Download.md) for more information on accessing these downloads.

## TSV

The column inclusion of every TSV download is always the same, regardless of any column visibility settings that may
have been applied on the Advanced Search page.  This has a couple advantages:

1. It makes downloaded data amenable to repeatable application of analysis tools outside of TraceBase.
2. It makes it possible to share data between researchers in a consistent fashion.

Three header rows containing download-related metadata are always included in any downloaded TSV data and are preceded
by the comment character `#`.

> \# Download Time
> \# Advanced Search Query used to generate the downloaded data
> \# Blank row
>
> Column Names
> data, where one row = 1 observation

## ZIP

NOTE: mzXML files are optional when loading data into TraceBase, so not all rows in the PeakGroups and PeakData searches
have them.  Thus, mzXML downloads can contain a subset of the displayed rows: It includes only those rows that have
mzXML files available.

The mzXMLs download button on the advanced search downloads a ZIP archive containing a TSV file listing the mzXML files
with associated metadata and mzXML files organized into the following directory structure:

* Mass Spec Run Date
  * Mass Spec Operator Name
    * Instrument Name
      * Liquid Chromatography Protocol Name
        * Polarity
          * Scan Range
            * *.mzXML
* mzXML metadata in TSV format

_Note that mzXML files are optional when submitting data to TraceBase, so not all rows in the PeakGroups and PeakData_
_searches have them.  Thus, mzXML downloads can contain a subset of the displayed rows: The ZIP file will include only_
_those rows that have mzXML files available._

### mzXML

The mzXML format is an external format described in [this paper](https://doi.org/10.1038/nbt1031).
