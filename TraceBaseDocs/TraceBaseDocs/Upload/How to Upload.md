# Uploading Data to TraceBase

## How "ready" does my data have to be to upload to TraceBase?

Every data submission to TraceBase (sample metadata, peak annotation files (AccuCor/IsoCor/Iso-AutoCor), and RAW/mzXML
files) is described/organized in a submission template we refer to as a **Study Doc** (an Excel Spreadsheet).  You can
create a study doc that contains the samples/animals associated with as few as one peak annotation file, an entire MS
Run, a whole Study, or even multiple studies.  We recommend that as soon as you have a peak annotation file, you draft a
submission to TraceBase.

The submission process uses the peak annotation files to automate the entry of a large portion of the metadata when you
download the template, such as sample names and compounds, but some manual metadata entry (for example, describing the
animals and samples) is required.  The required columns are highlighted in blue in the downloaded Study Doc^.  See the
[[#instructions]] below for details.

> ^ _Note that in order for FCirc calculations to be displayed on TraceBase, some optional columns described at the top_
> _of the [FCirc Rates](../Values/FCirc%20Rates.md) are required._

The upload process ensures that the data integrity is preserved from study to study and from sample to sample.  For
example, the process ensures:

* Samples are labeled accurately.
* Animal, Sample, and Study names are unique.
* Consistent nomenclature is used.

Your data is initially uploaded to a private folder, where a curator checks the data to ensure it is formatted correctly
before it is loaded.  When all checks have passed, the curator adds the data to TraceBase.  This means it is OK (and
expected) for your data to be imperfectly labeled when you initially submit for upload, however the process provided
empowers each user to be able to solve problems on their own.  As the author of the data, you are the most knowledgeable
person to fix issues that come up.  However, you can choose to engage as much as you want in the validation of your
data.

## <a name="instructions"></a>Step-by-step Instructions

The submission process is designed to be as self-explanatory as possible.

To get started, click the upload button in the menu bar at the top of the page.

![Upload in the menu bar](../Attachments/uploadenabled.png)

> _Note: upload on public instances of TraceBase can be disabled, in which case, there would be no upload option:_
> ![No upload in the menu bar](../Attachments/uploaddisabled.png)

There are 4 tabs on the Upload Page that proceed from left to right:

![Upload tabs](../Attachments/upload_tabs_.png)

Upon finishing any one page, you can leave and come back.  Your work is saved in the downloaded Study Doc.

### 1. Start - Create a Study Doc

The start page is designed to create a submission template (a.k.a. a "Study doc") from scratch using your peak
annotation files.  The simplest usage is to drop all of your peak annotation files (e.g. "AccuCor") in one drag
operation onto the drag and drop area and click "Download Template".  The tool will automatically extract your samples
and compounds and use them to pre-fill the template sheets.

However, you can save yourself some time by filling in the requested metadata associated with each peak annotation file:

* MS Operator (the name of researcher who ran the Mass Spec)
* Instrument (e.g. Exploris480)
* LC Protocol (e.g. polar-HILIC-25-min)
* Run Date (in YYYY-MM-DD format)

Each field has a drop-down, but any value can be entered.

The fields next to the drop area will apply to every file dropped (to avoid repeated manual entry), but you can edit
those associated with each individual file after dropping in the rows that appear below for each dropped file:

![peak annot drop example](../Attachments/drag_annots_forms.png)

In this example, the operator, instrument, and LC protocol were filled in before dropping and the date was left empty
because each was run on a different date, to be filled in after dropping in the forms that appear below the drag and
drop area.

2. Fill in the template with data about your samples
    * Follow the directions under the "Fill In" tab to flesh out your study doc
      with all the details of your study.

3. Check your Study doc for errors
    * Under the "Validate" tab, submit your filled-in study doc to download a
      version containing some minor repairs and an error report.
    * You can either proceed to submission or try and fix any of the reported
      errors and re-validate.

4. Submit your data
    * Under the "Submit" tab, follow the directions on where to deposit your
      data.
    * Click the "Submission Form" button and fill out the resulting form to let
      the curators know that your submission is done.

What happens next?

We will review your study to ensure that everything will load smoothly. If
there are any issues, we will contact you to sort everything out. The study
data will be loaded into TraceBase and we'll send you an email with a link to
view the data to make sure everything looks correct and you can start browsing
your data.

[Contact us](https://forms.gle/LNk4kk6RJKZWM6za9) anytime if you have any
questions, concerns, or comments. If, after your submission, you need to
follow-up or would like to check-in on the upload status, [let us
know](https://forms.gle/LNk4kk6RJKZWM6za9).

## FAQ

Do my compound names need to match TraceBase compound names?

* No.  TraceBase maintains a list of primary compound names associated with
  synonyms.  If you upload data with a new compound name, we will contact you
  to resolve the difference.  If it is a new compound, then your name becomes
  the primary compound name.  If your name matches an existing compound in
  TraceBase, then your name is added as a synonym, and your next upload will
  not have any issues.

I have a new Tissue.  How do I upload?

* In the sample information workbook, on the Tissues tab, add your new Tissue
  name to the list.  Label your samples with this new Tissue name.  When you
  submit the google form, tell the developer you are adding a new Tissue.

Can I upload multiple data files at once?

* Yes!  Upload as many data files as you want.  Ideally, use only one Sample
  Information Sheet.

My sample names in one Accucor/Isocorr file are not unique (ie `Mouse1_Q`,
`Mouse2_Q`, `Mouse3_Q` in one data file for one experiment, and `Mouse1_Q`,
`Mouse2_Q`, `Mouse3_Q` in a second data file for a second experiment).  Can I
upload these together?

* Yes, but we will work with you to ensure data is uploaded correctly.
  Ideally, every sample name in the data file should correspond to one unique
  biological sample in this `Study`.  Upload what you have, then contact us and
  we can help you upload these data.

I modified my data files before uploading them (ie added rows for sample
information).  Can I upload these files?

* Yes.  We will help you come up with an easy solution for uploading modified
  files.  Just upload what you have and we will contact you to confirm our
  solution is OK.

Can I upload some data now, and upload more data from the same samples later?

* Yes.  TraceBase will add new data to existing samples.  If the same compound
  is uploaded a second time, TraceBase will use the latest upload.

What kind of data can be uploaded?

* See [[Input Data for TraceBase Upload]]
