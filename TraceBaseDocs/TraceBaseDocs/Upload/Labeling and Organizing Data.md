# Labeling and Organizing Data

## Key Information for TraceBase

TraceBase is designed to match an LCMS sample name found in an input file
(Accucor or IsoCorr) to a biological sample (specific tissue from a specific
animal, listed in the [Study Doc](Study%20Doc.md)).

Most importantly, the Sample ID and Animal ID should be unique for a given Study.

If the Animal ID or Sample ID is not unique, upload is still possible.  Feel
free to upload what you have and developers are happy to help.

## Recommended Best Practices

To make upload easy, you could include this key information in the sample name
of your input Accucor or IsoCorr file:

- Animal ID
- Tissue
- Time Collected

The following workflow for labeling / organizing everything will work well with
TraceBase (and might be useful generally).

### Scheme for Unique Experiment, Animal, and Sample IDs

For a new experiment, create a new identifier counting up from "study001", and
sometimes include a short identifier or name for the experiment. (e.g.
"study001_hyperthyroid glucose infusion").  Now you can reference this
experiment ID ("study001") elsewhere.

- A different experiment may have a new identifier (e.g. "study002_hypothyroid
  glucose infusion").
- A related follow up experiment may have an extension to the identifier, e.g.
  "study001b_hyperthyroid lactate infusion".
- Any text could be used in place of "study".

Within each experiment, animal ID counts up from 001 "study001_m01, study001_m02...".

- A unique sample file can then be created by adding to this animal ID:
  - provide full animal identifier, tissue, and sometimes time collected:
  - E.g. "study001_m01_quad, study001_m01_tailserum_000,
    study001_m01_tailserum_120"
- Some researchers keep a single list of Animal IDs for all experiments (e.g.
  0001, 0002, etc)  This works well for TraceBase, because each Animal ID is
  unique.

### Apply this scheme when performing LCMS

When loading samples to run LCMS, apply this labeling scheme:

- Create a folder for each sequence that includes the date of LCMS and
  experiment identifier.
- Label samples according to biological entity (e.g. "study001_plasma_0")
  - If running the same samples multiple times (e.g. positive mode / negative
    mode), save each result in a different destination folder.
- When analyzed in Maven/El-Maven and processed for isotope correction, the
  resulting sample names will be easy to label for upload to TraceBase.

LCMS data for a single biological entity can be generated from multiple LCMS
experiments (e.g. "positive" vs "negative" mass spectrometry, alternative
extraction methods, etc). There are two general methods for handling this:

- Option 1:  include method information in the sample name (e.g. append
  "hilicPos")
- Option 2: leave sample name the same for every biological entity, and
  generate separate Accucor / Isocorr files for each type of method.
  - This requires saving results into separate folders on the LCMS.
  - This option is easiest to implement for upload to TraceBase because it
    keeps your Sample Information Sheet simple.

### Apply this scheme everywhere

These labeling schemes can be applied to your general organization of data
outside of TraceBase.

Data and any other information related to your experiments can be organized
into one folder labeled for each study "study001_my first infusion",
"study001b_fixing my first infusion".  Put everything related to the experiment
in this folder, for example:

- mouse information sheets
- sample information sheets (even a copy of the TraceBase [Study Doc](Study%20Doc.md))
- LCMS data (mzxml)
- accucor / isocorr files
- Maven project files
- R / python scripts for analyzing this data

In your lab notebook, create page(s) for each experiment ID.

When working with samples in the lab, it is not feasible to label every tube
with the full identifier, but shorthand can be used for intermediate tubes if
everything is from the same study:

- E.g. when extracting tissue from only study001, label working tubes with
  minimal info "Q1, Q2".
- Label final tubes with as much information as possible (e.g. "001_M1_Q").
- Label boxes stored in freezers with study identifier, a relevant date, and
  your initials
  - (e.g. "study001_glucose infusion", "study001b_lactate infusion"...)
