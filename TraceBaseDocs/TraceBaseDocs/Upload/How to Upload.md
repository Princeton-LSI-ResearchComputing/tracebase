# Uploading Data to TraceBase

To get started, click the upload button in the menu bar at the top of the page.

![image](https://user-images.githubusercontent.com/34348153/202543782-fba4123b-50ed-4a35-9c7c-b3c52183d086.png)

The upload process process ensures that samples are labeled accurately and that
new data is consistent with existing data in TraceBase.  Your data is initially
uploaded to a private folder, then a developer checks the data to ensure it is
formatted correctly, and when ready the developer adds the data to TraceBase.
This means it is OK (and expected) for your data to be imperfectly labeled when
you initially submit for upload.

## Step-by-step instructions

1. Create a Sample Sheet (see also [How to label sample
   information](How%20to%20label%20Mass%20Spec%20Run%20Information.md))
[Copy](https://docs.google.com/spreadsheets/d/1To3495KxJkAtnAD9KVdzc162zKbNiYuPEVyPseQPjMQ/copy?copyComments=true)
the TraceBase Animal and Sample Table Template.
   1. Fill in the `Animals` sheet. Give each mouse in your study a unique ID.
   2. Fill in the `Samples` sheet.
      - Ensure that each `Sample Name` uniquely represents a true biological
        sample. If a sample name does not match the sample headers in your
        AccuCor/Isocorr files (e.g. a sample tube was injected multiple times),
        you must add those non-matching samples to an
        [LCMS-metadata file](https://docs.google.com/spreadsheets/d/1rfKOGqms8LPeqORO5gyTXLXDU2lvz-CG2aCEwmu8xHw/copy)
      - If you have a Tissue that is not included in the existing list, please
        label it as you would like to in the `Tissues` sheet and inidcate
        that you are submitting a new Tissue Type in the "Special Instructions"
        section of the form.
   3. Fill in Treatments sheet (optional) with any treatments not captured by
      other columns (ie `Diet`).  Use `no treatment` if applicable. Provide a
      list of treatments used in the study, along with a description of each.

      Note: In general, you should NOT edit your AccuCor files. It's simplest
      and often best to resolve issues by editing your animal and sample table
      file.

2. Upload your data on Google Drive
   1. Create a folder for your study in the [TraceBase Study Submission
      folder](https://drive.google.com/drive/folders/1cBy3eezfr_0vmaz8RGodga6A8n3hkm1m?usp=sharing)
      on Google Drive
   2. Create a folder with your name (if it doesn't already exist).
   3. Create a new folder for this study.
   4. Add the Animal and Sample spread sheet, AccuCor/IsoCorr files, and the
      conditionally required LCMS-metadata file to the study folder

3. Submit the [TraceBase Study Submission form](https://forms.gle/vEfJEfhPbCbkybpE7)
   Complete the following:
   1. Study Name
   2. Link to Google Drive folder with study submission.
   3. Mass Spectrometry Details (see [[How to label Mass Spec Run
      Information]]).  Note, the LCMS-metadata file can be used to denote
      multiple different operators, dates, methods, etc. The values entered into
      the submission form are used as defaults for any samples that are not
      included in the file.

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
