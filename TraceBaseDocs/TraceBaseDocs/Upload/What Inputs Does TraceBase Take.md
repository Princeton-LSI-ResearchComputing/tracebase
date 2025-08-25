<!-- markdownlint-disable MD007 -->
# What Inputs Does TraceBase Take

TraceBase takes 3 kinds of input data:

* Raw Mass Spectrometry Peak Data
    * `RAW` files from the Mass Spec Instrument
    * `mzXML` files
* Corrected Mass Spectrometry Peak Data
    * We refer to these inputs as `peak annotation files`
    * Used initially to extract experimental metadata
    * Included in the final submission with associated raw data
* Experimental Metadata
    * All experimental metadata (e.g. sample details, compounds, protocols, etc) is entered into a single Excel file
    * We refer to this input file as the `Study Doc`

The experimental metadata includes some nomenclature-controlled metadata to make sure all data is inter-comparable and
reliably searchable across studies.  See [How TraceBase Handles Data](../About/How%20TraceBase%20Handles%20Data.md) for
more information on how TraceBase treats both input and output data.

## Raw Mass Spectrometry Peak Data

TraceBase currently supports 2 forms or raw Mass Spec files"

* `RAW` files produced by a Mass Spec instrument
* `mzXML` files containing a portion of the RAW data that are typically used in peak correction

TraceBase archives each of these file types to make them searchable in order to find unanalyzed peak data for future
studies (as opposed to the curated corrected data that TraceBase provides calculated values for, like
[FCirc Rates](../Values/FCirc%20Rates.md)).

A collection of either form of raw file is too large and impractical to provide via upload on the web, so study
submissions are delivered for loading using a shared drive, as described on the Upload **Submit** page.

The `mzXML` files are parsed by TraceBase to extract:

* Polarity
* Scan Range

## Corrected Mass Spectrometry Peak Data

TraceBase supports the output from 3 popular tools for natural abundance correction:

* [AccuCor](https://doi.org/10.1021/acs.analchem.7b00396) _([GitHub](https://github.com/lparsons/accucor))_
* [IsoCor](https://doi.org/10.1093/bioinformatics/btz209) _([GitHub](https://github.com/xxing9703/isocorr13C15N))_^
* [Iso-AutoCor](https://github.com/xxing9703/Iso-Autocorr)

Therefore, a typical workflow to put data in TraceBase could start with:

1. Create `mzXML` files from the Mass Spec instrument's `RAW` files using conversion tools like
    * ProteoWizard's MSConvert
    * OpenMS FileConverter
    * custom scripts
2. Select peaks from a set of `mzXML` files using peak-picking tools like
    * [Maven](http://maven.princeton.edu/index.php)
    * [El-Maven](https://www.elucidata.io/el-maven)
3. Create a `peak annotation file` using one of the natural abundance correction tools mentioned above

Tracebase can take any of the 3 `peak annotation file` types (AccuCor, IsoCor, or Iso-AutoCor) in any one of 3 formats:

* Microsoft Excel Spreadsheet
* Corrected data sheet only in one of 2 plain text formats (e.g. using NotePad or TextEdit):
    * CSV (comma separated values)
    * TSV (tab separated values)

Peak annotation files are included in 2 steps during the upload process:

* They are used on the Upload **Start** page to stub out a `Study Doc` template to get a jump start on experimental
  metadata entry.
* They are included in the final submission, organized into folders on a shared drive with their associated raw files.

^ The GitHub version of IsoCor is called _isocorr13C15N_.  These two terms are used interchangeably on TraceBase.

## Experimental Metadata

To empower the user and engender a sense of ownership and control over your data, all experimental metadata (even the
nomenclature-controlled metadata that subject to curator oversight), is localized in a single Excel document that you
build during the submission process.

TraceBase is designed to match an LCMS sample name to a biological sample.  In a submission, the sample names and the
compounds are extracted and processed from the `peak annotation files`.  It's not possible to extract all other metadata
automatically, thus much of this data entry is manual, but the data is organized around how researchers tend to store
this kind of data.

### <a name="metadata_recommendations"></a>Recommended Practices for Organizing Data

Some researchers keep a single list of Animal IDs for all experiments.  This works well for TraceBase, because each
Animal ID is unique.  Other researchers keep a list of animal IDs for each experiment (aka "Study").  In this example,
the study should have a unique identifier and that should be combined with the animal identifier (e.g.
`study001_mouse001`).  This also works well for TraceBase.

If the Animal ID or Sample ID is not unique, upload is more difficult but still achievable.  Feel free to enter what
you have and TraceBase curators can help make your data compatible.

Note that these labeling schemes can be applied to your general organization of data outside of tracebase:
* For a new experiment, create a new identifier counting up from `study001`
    * A related follow up experiment may have an extension, e.g. `study001b`, `study001c`
    * Any text could be used in place of "study"
* Within each experiment, count animal identifiers `study001_m01`, `study001_m02`...
* When labeling sample files, provide full animal identifier, tissue, and sometimes time collected:
    * E.g. `study001_m01_quad`, `study001_m01_tailserum_000`, `study001_m01_tailserum_120`
* When working with samples in the lab, it is not feasible to label every tube with the full identifier, but shorthand
  can be used for intermediate tubes if everything is from the same study:
    * E.g. for tissue extraction from `study001`, label working tubes `Q_1`, `Q_2`....  Label final tubes `001_m1_Q`
* Data and any other information related to your experiments can be organized in folders labeled for each study
  `study001_my first infusion`, `study001b_fixing my first infusion`
<!-- markdownlint-enable MD007 -->
