To get started, click the upload button in the menu bar at the top of the page.

![image](https://user-images.githubusercontent.com/34348153/202543782-fba4123b-50ed-4a35-9c7c-b3c52183d086.png)

The upload process process ensures that samples are labeled accurately and that new data is consistent with existing data in TraceBase.  Your data is initially uploaded to a private folder, then a developer checks the data to ensure it is formatted correctly, and when ready the developer adds the data to TraceBase.  This means it is ok (and expected) for your data to be imperfectly labeled when you initially submit for upload.

# Step-by-step instructions
1. Create a Sample Sheet (see also [How to label sample information](https://docs.google.com/document/d/15C1Qp_l-QQ7eCK8Iu2wWFS8f2eUb7I11DQKz-R3mHus/edit?usp=sharing))
[Copy](https://docs.google.com/spreadsheets/d/1To3495KxJkAtnAD9KVdzc162zKbNiYuPEVyPseQPjMQ/copy?copyComments=true) the TraceBase Animal and Sample Table Template.
   1. Fill in the `Animals` sheet. Give each mouse in your study a unique id.
   2. Fill in the `Samples `sheet. Ensure that each `Sample Name` matches the sample names in your AccuCor/Isocorr files. If you have a Tissue that is not included in the existing list, please label it as you would like to in the `Tissues` sheet and tell the developer you have a new Tissue.
   3. Fill in Treatments sheet (optional) with any treatments not captured by other columns (ie `Diet`).  Use `no treatment` if applicable.
Provide a list of treatments used in the study, along with a description of each.
2. Optional: Validate your data
   1. Download your Animal and Sample Table in Excel format.
   2. [Validate](https://tracebase-dev.princeton.edu/DataRepo/validate) your sample table and AccuCor/IsoCorr files
   3. If there are any errors reported, fix them in the Google spreadsheet and return to step 1.

Note: In general, you should NOT edit your AccuCor files. It's simplest and often best to resolve issues by editing your animal and sample table file.

3. Upload your data on Google Drive
   1. Create a folder for your study in the[ TraceBase Study Submission folder](https://drive.google.com/drive/folders/1cBy3eezfr_0vmaz8RGodga6A8n3hkm1m?usp=sharing) on Google Drive
   2. Create a folder with your name (if it doesn't already exist).
   3. Create a new folder for this study.
   4. Copy the Animal and Sample spread sheet and any AccuCor files that should be included in the study
4. Submit the[ TraceBase Study Submission form](https://forms.gle/vEfJEfhPbCbkybpE7).  Complete the following:
   1. Study Name
   2. Link to Google Drive folder with study submission.
   3. Mass Spectrometry Details (see [How to label Mass Spec Run Information](https://docs.google.com/document/d/1Lm4br-jCB2QwbgPyzJvvgLgaO-t7Cwcq1xHyBIOlTog/edit?usp=sharing))

What happens next?

We will review your study to ensure that everything will load smoothly. If there are any issues, we will contact you to sort everything out.
The study data will be loaded into TraceBase and we'll send you an email with a link to view the data to make sure everything looks correct and you can start browsing your data.

[Contact us](https://forms.gle/LNk4kk6RJKZWM6za9) anytime if you have any questions, concerns, or comments If, after your submission, you need to follow-up or would like to check-in on the upload status,[ let us know](https://forms.gle/LNk4kk6RJKZWM6za9).

# FAQ

Do my compound names need to match TraceBase compound names?
* No.  TraceBase maintains a list of primary compound names associated with synonyms.  If you upload data with a new compound name, we will contact you to resolve the difference.  If it is a new compound, then your name becomes the primary compound name.  If your name matches an existing compound in TraceBase, then your name is added as a synonym, and your next upload will not have any issues.

I have a new Tissue.  How do I upload?
* In the sample information workbook, on the Tissues tab, add your new Tissue name to the list.  Label your samples with this new Tissue name.  When you submit the google form, tell the developer you are adding a new Tissue.

Can I upload multiple data files at once?
* Yes!  Upload as many data files as you want.  Ideally, use only one Sample Information Sheet.

My sample names in one Accucor/Isocorr file are not unique (ie `Mouse1_Q`, `Mouse2_Q`, `Mouse3_Q` in one data file for one experiment, and `Mouse1_Q`, `Mouse2_Q`, `Mouse3_Q` in a second data file for a second experiment).  Can I upload these together?
* Not yet.  Every sample name in the data file should correspond to one unique biological sample in this `Study`.  Contact us and we can help you upload these data.

I modified my data files before uploading them (ie added rows for sample information).  Can I upload these files?
* Yes.  We will help you come up with an easy solution for uploading modified files.  Just upload what you have and we will contact you to confirm our solution is ok.

Can I upload some data now, and upload more data from the same samples later?
* Yes.  TraceBase will add new data to existing samples.  If the same compound is uploaded a second time, TraceBase will keep the latest upload.

What kind of data can be uploaded?
 - [[Input Data for TraceBase Upload]]



