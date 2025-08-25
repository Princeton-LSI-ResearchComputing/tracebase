# How to Name Your Studies, Animals, and Samples

## Recommended Sample Naming Best Practices

To make upload easy, we recommend including this key information in your sample names:

* Animal Name
* Tissue
* Time Collected

You might also consider including your initials or an abbreviation of your study name, to ensure uniqueness.

Here's why:

> When naming your samples for the mass spectrometer run, making them unique for TraceBase can smooth out your
> submission experience.
>
> The sample names in the RAW files that come off the Mass Spec instrument should map to a unique biological sample in
> TraceBase.  Mass Spec runs are often configured for samples from a single animal, so it makes sense that the sample
> names entered often embed multiple bits of data to reference a specific tissue plus sometimes the animal name, or even
> a researcher's initials.
>
> When multiple `mzXML` files are generated from one sample's raw file, to obtain the positive or negative scan data, or
> a particular m/z (mass/charge) range, the resultant files adopt the same name, and when those files are collated
> together, sometimes the polarity or scan range (e.g. `_neg`, `_pos`, or `_scan2`), or some combination, is added so
> that the files can exist in the same directory.
>
> All of this information gets incorporated into the sample headers of the peak annotation files.  TraceBase is designed
> to extract the biological sample names from the LCMS sample name found in a peak annotation file (e.g.  _AccuCor_),
> thus it attempts to identify and remove the scan labels so that the samples listed in each row of the `Samples` sheet
> of the [Study Doc](How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md) represent
> unique biological samples.
>
> Thus, most importantly, since samples from multiple animals are collected into a single study, the sample names (and
> Animal names for that matter) should be unique across that entire study.  If the Animal name or Sample name is not
> unique, upload is still possible, but the names will need to be modified to be unique.
>
> And since TraceBase is designed for cross-study analyses, it enforces that all animal and sample names be globally
> unique, thus a simple sample name like `M1_spleen` is likely going to need to be modified to avoid colliding with
> existing TraceBase samples.

## Suggested Scheme for Unique Study, Animal, and Sample Names

For a new study/experiment, create a new identifier counting up from "study001", and sometimes include a short
identifier or name for the experiment. (e.g.  "study001_hyperthyroid_glucose_infusion").  Now you can reference this
experiment ID ("study001") elsewhere.

* A different experiment may have a new identifier (e.g. "study002_hypothyroid glucose infusion").
* A related follow up experiment may have an extension to the identifier, e.g.  "study001b_hyperthyroid lactate
  infusion".
* Any text could be used in place of "study".

Within each experiment, animal name counts up from 001 "study001_m01, study001_m02...".

* A unique sample file can then be created by adding to this animal name:
    * provide full animal identifier, tissue, and sometimes time collected:
    * E.g. "study001_m01_quad, study001_m01_tailserum_000, study001_m01_tailserum_120"
* Some researchers keep a single list of Animal IDs for all experiments (e.g.  0001, 0002, etc)  This works well for
  TraceBase, because each Animal name is unique.

## Apply this scheme when performing LCMS

When loading samples to run LCMS, apply this labeling scheme:

* Create a folder for each sequence that includes the date of LCMS and experiment identifier.
* Label samples according to biological entity (e.g. "study001_plasma_0")
    * If running the same samples multiple times (e.g. positive mode / negative mode), save each result in a different
      destination folder.
* When analyzed in Maven/El-Maven and processed for isotope correction, the resulting sample names will be easy to label
  for upload to TraceBase.

LCMS data for a single biological entity can be generated from multiple LCMS experiments (e.g. "positive" vs "negative"
mass spectrometry, alternative extraction methods, etc). There are two general methods for handling this:

* Option 1:  include method information in the sample name (e.g. append "hilicPos")
* Option 2: leave sample name the same for every biological entity, and generate separate Accucor / Isocorr files for
  each type of method.
    * This requires saving results into separate folders on the LCMS.
    * This option is easiest to implement for upload to TraceBase because it keeps your Sample Information Sheet simple.

## Apply this scheme everywhere

These labeling schemes can be applied to your general organization of data outside of TraceBase.

Data and any other information related to your experiments can be organized into one folder labeled for each study
"study001_my first infusion", "study001b_fixing my first infusion".  Put everything related to the experiment in this
folder, for example:

* mouse information sheets
* [Study Doc](How%20to%20Build%20a%20Submission/2%20-%20How%20to%20Fill%20In%20the%20Study%20Doc.md)
* LCMS data (mzxml)
* accucor / isocorr files
* Maven project files
* R / python scripts for analyzing this data

In your lab notebook, create page(s) for each experiment name.

When working with samples in the lab, it is not feasible to label every tube with the full identifier, but shorthand can
be used for intermediate tubes if everything is from the same study:

* E.g. when extracting tissue from only study001, label working tubes with minimal info "Q1, Q2".
* Label final tubes with as much information as possible (e.g. "001_M1_Q").
* Label boxes stored in freezers with study identifier, a relevant date, and your initials
    * (e.g. "study001_glucose infusion", "study001b_lactate infusion"...)

If you are unsure of how to label something, write whatever you think fits and submit anyway.  Indicate what you are
unsure about in the submission form or on the information sheet.  The developer team will check the data and help you
label it properly before adding to the live data on TraceBase.

The key steps to remember for this sheet are to match sample names and animal IDs:
![Diagram showing how the AccuCor files, Samples Table, and Animals Table must match](../Attachments/Sample%20Information%20Sheet%20Sketch.png)
