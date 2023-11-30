# Sample Information Sheet

The Researcher uploading data completes a Sample Information Sheet to identify
the samples to be included in TraceBase.  Definitions for each term in this
worksheet are described below.

See also [Labeling and Organizing Data](Labeling%20and%20Organizing%20Data.md)

If you are unsure of how to label something, write whatever you think fits and
submit anyway.  Indicate what you are unsure about in the submission form or on
the information sheet.  The developer team will check the data and help you
label it properly before adding to the live data on TraceBase.

The key steps to remember for this sheet are to match sample names and animal
IDs: ![Diagram showing how the identifiers between the AccuCor files, Samples
Table, and Animals Table must
match](../Attachments/Sample%20Information%20Sheet%20Sketch.png)

## Animals Tab

### Animal ID

A unique identifier for the animal.

### Age

Age in weeks when the infusion started

### Sex

"male" or "female"

### Animal Genotype

Most specific genotype possible.

Check existing data on TraceBase for common genotypes.  If necessary, indicate
genotype as "unknown" (e.g. if the animal is a mixed background wildtype).

### Animal Body Weight

Weight in grams at time of infusion

### Infusate

A description of the infusion solution (aka cocktail) given to this animal.

Individual tracers are formatted: `compound_name-[weight element count,weight element count]`

* Examples:
  * valine-\[13C5,15N1]
  * leucine-\[13C6,15N1]
  * isoleucine-\[13C6,15N1]

* If a tracer is not fully labeled for a given element, the positions of the
  that element must be included: `compound_name-[position,position-weight
  element count]`
  * Example: L-Leucine-\[1,2-13C2]

Mixtures of compounds are formatted: `tracer_mix_name {tracer; tracer}`

* `tracer_mix_name` is optional
* Example: BCAAs {isoleucine-\[13C6,15N1];leucine-\[13C6,15N1];valine-\[13C5,15N1]}

### Tracer Concentrations

Concentration(s) of infusate in this infusion solution (in mM)

For multiple tracer compounds, list concentrations in the same order as in
"infusate" separated by semicolons

* Example: `5; 6; 9` to indicate 5 mM, 6 mM, 9 mM

### Infusion Rate

Volume of infusate solution infused (microliters (ul) per minute per gram of
mouse body weight)

### Diet

Description of animal diet used.  Include the manufacturer identification and
short description where possible.  Check data on TraceBase for commonly used
diets.

### Feeding Status

`fasted`, `fed`, or `refed`

Indicate the length of fasting/feeding in `Study Description`

### Animal Treatment

Short, unique identifier for animal treatment protocol.  Details are provided
in the `Treatment Description` field on the `Treatments` sheet

* Example:  "T3 in drinking water"

Default animal treatment is `no treatment`.

Note that diets and feeding status are indicated elsewhere, and are not
indicated here.

These protocol are designed to be uniquely named so that lab members can share
between their studies.

### Study Name

An identifier for the "experiment" or collection of animals that this animal
belongs to.

### Study Description

A long form description of the study.

Describe here experimental design, citations if the data is published, or any
other relevant information that a researcher might need to consider when
looking at the data from this study.

## Samples Tab

### Sample Name

Unique identifier for the biological sample. Generally, the sample names should
match the sample headers in the AccuCor/IsoCorr files, but if a sample tube was
injected multiple times, they can differ.  In that case, an
[LCMS-metadata file](https://docs.google.com/spreadsheets/d/1rfKOGqms8LPeqORO5gyTXLXDU2lvz-CG2aCEwmu8xHw/copy)
should be included in your submission to match the sample names in the database
with the headers in the AccuCor/IsoCorr files.

### Date collected

Date sample was collected (YYYY-MM-DD).

### Researcher Name

Researcher primarily responsible for collection of this sample (FIRST LAST).  Secondary people (PI, collaborator, etc) can be mentioned in the study description.

### Tissue

Type of tissue, matching the reference list of tissues.

* See `Tissues` tab for reference list of tissues, or view a list of [Tissues
  on TraceBase](https://tracebase.princeton.edu/DataRepo/tissues/).

* If you have a tissue that is not listed in `Tissues` tab, add your new tissue
  to the `Tissues` tab.

### Collection Time

Minutes after the start of the infusion that the tissue was collected.

Collection Time for samples collected before the infusion should be <= 0.

### Animal ID

Matches to the Animal ID that this sample was collected from.

## Treatments Tab

### Animal Treatment

* Short, unique identifier for animal treatment protocol.
* Matches to the same value in "Samples" sheet.

### Treatment Description

* A thorough description of an animal treatment protocol. This will be useful
  for searching and filtering, so use standard terms and be as complete as
      possible.

* Any difference in `Treatment` should be indicated by a new `Animal Treatment`

  * Example:  different doses of drug

## Tissues Tab

### TraceBase Tissue Name

* Short identifier used by TraceBase
* Use the most specific identifier applicable to your samples
* If your data contains a tissue not already listed here, create a new
  TraceBase Tissue Name and Description here.

### Description

* Long form description of TraceBase tissue
