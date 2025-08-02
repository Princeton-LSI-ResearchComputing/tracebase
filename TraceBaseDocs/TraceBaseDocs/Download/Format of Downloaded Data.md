# Format of Downloaded Data

Downloaded data is available in 2 formats:

1. Table data in TSV format (Tab Separated Values)
2. mzXML files in Zip format

A TSV download is available for PeakGroups, PeakData, and Fcirc data.

A Zip download is available for PeakGroups and PeakData and contains mzXML files (if available).  Note that the mzXML
download will contain the subset of the displayed rows that have mzXML files available.

PeakGroups (for samples from infusions with multiple labeled elements) are divided into a row for each labeled element.

FCirc data has a row for every combination of serum sample, tracer, and labeled element.

## TSV

TSV files can be obtained from the Download menu in the top bar or from the TSV download button in the advanced search.

The column inclusion of every TSV download is always the same, regardless of any column visibility settings that may
have been applied on the Advanced Search page.  This has a couple advantages:

1. It makes downloaded data amenable to repeatable application of analysis tools outside of TraceBase.
2. It makes it possible to share data between researchers in a consistent fashion.

Three header rows containing download metadata are always included in any downloaded TSV data and are preceded by the
comment character '#'.

1. Download Time
2. Advanced Search Query (in JSON format) used to generate the downloaded data
3. Blank line

The first row after he header is the tab-delimited column names, followed by data rows.

## Zip

The mzXMLs download button on the advanced search downloads a Zip archive containing a TSV file listing the mzXML files
with associated metadata and mzXML files organized into the following directory structure:

- Mass Spec Run Date
  - Mass Spec Operator Name
    - Instrument Name
      - Liquid Chromatography Protocol Name
        - Polarity
          - Scan Range
            - *.mzXML
