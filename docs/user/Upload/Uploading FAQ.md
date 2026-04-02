# Uploading FAQ

## How "ready" does my data have to be to upload to TraceBase?

Every data submission to TraceBase (sample metadata, `peak annotation files` (`AccuCor`/`IsoCor`/`Iso-AutoCor`), and
`RAW`/`mzXML` files) is described/organized in a submission template we refer to as a **`Study Doc`** (an Excel
Spreadsheet).  You can create a study doc that contains the samples/animals associated with as few as one peak
annotation file, an entire MS Run, a whole Study, or even multiple studies.  We recommend that as soon as you have a
peak annotation file, you draft a submission to TraceBase.

The submission process uses the `peak annotation files` to automate the entry of a large portion of the metadata when
you download the template, such as sample names and compounds, but some manual metadata entry (for example, describing
the animals and samples) is required.  The required^ columns are highlighted in blue in the downloaded Study Doc.  See
**[[How to Upload Data to TraceBase]]** for details.

> ^ _Note that in order for FCirc calculations to be displayed on TraceBase, some optional columns described at the top_
> _of the [FCirc Rates](../Values/FCirc%20Rates.md) are required._

The upload process ensures that the data integrity is preserved from study to study and from sample to sample.  For
example, the process ensures:

* Samples are labeled accurately
* Animal, Sample, and Study names are unique
* Consistent nomenclature is used

Your data is initially uploaded to a private folder, where a curator checks the data to ensure it is formatted correctly
before it is loaded.  When all checks have passed, the curator adds the data to TraceBase.  This means it is OK (and
expected) for your data to be imperfectly labeled when you initially submit for upload, however the process provided
empowers each user to be able to solve problems on their own.  As the author of the data, you are the most knowledgeable
person to fix issues that come up.  However, you can choose to engage as much as you want in the validation of your
data.

## Do my compound names need to match TraceBase compound names?

No.  TraceBase maintains a list of primary compound names associated with synonyms.  If you upload data with a new
compound name, we will contact you to resolve the difference.  If it is a new compound, then your name becomes the
primary compound name.  If your name matches an existing compound in TraceBase, then your name is added as a synonym,
and your next upload will not have any issues.

Ideally, every new compound will have an [HMDB](https://hmdb.ca) ID associated with it.  If HMDB does not have a record
for your compound, enter a fake HMDB ID in the form `FakeHMDB0000` in order to validate associated data (because it's a
required value), and add a compound synonym with the compound's [PubChem](https://pubchem.ncbi.nlm.nih.gov/) ID in the
form `PubChem0000000`.^

Note however that currently, tracer/infusate names are always converted to TraceBase's primary compound name for
consistent/uniform search results and that PeakGroup names always use whatever synonym is present in the peak annotation
files.  _The original design thinking was that this would support distinct stereo-isomers, but this may change in the_
_future._

^ _Support for PubChem is a planned feature that will make either an HMDB or PubChem ID required, eliminating the need_
_for the "fake" HMDB ID._

## I have a new Tissue.  How do I upload?

In the sample information workbook, on the Tissues tab, add your new Tissue name to the list.  This will update the
tissue dropdown in the tissue column of the Samples sheet, allowing you to select your new Tissue name.  When you submit
the google form, tell the developer you are adding a new Tissue.

## Can I upload multiple data files at once?

Yes!  Upload as many data files as you want.  Ideally, use only one Study Doc.  This will allow the software to catch
multiple representations of the same compound picked for the same sample(s) in multiple peak annotation files (e.g. the
same compound picked in positive versus negative mode).  TraceBase allows only one representation of a compound in a
sample.

## My sample names in one Accucor/Isocorr file are not unique.  Can I upload these together?

E.g. Samples `Mouse1_Q`, `Mouse2_Q`, `Mouse3_Q` are in one data file for one experiment, and `Mouse1_Q`, `Mouse2_Q`,
`Mouse3_Q` in a second data file for a second experiment.

Yes, but this may require some special attention.  Ideally, every sample name in the data file should correspond to one
unique biological sample in a `Study`.  If that's not the case, and the 2 files containing this name collision are
uploaded together on the **Start** page, TraceBase will assume they are the same biological samples and create a single
sample row for each in the Samples sheet.  This can be fixed manually, but in this case, it is far easier to create
separate Study Docs to avoid the errors.

If you generate a single Study Doc with this name conflict, you may (or may not) see any errors, despite the existing
problem (missing distinct samples).  The errors you might see are `MultipleRepresentation` errors on the **Start** page
and a `Peak Group Conflicts` sheet in the downloaded Study Doc.  Whether this happens or not depends on the compounds in
the peak annotation files.  If 2 of the same-named different samples analyze the same compounds (i.e. you picked the
same peaks), since TraceBase thinks there was a single sample, it assumes that you picked peaks for the same compound
twice.  Only 1 such compound representation is allowed per sample, so TraceBase issues the error and prompts you to pick
one of the 2 compound representations in the `Peak Group Conflicts` sheet.  But since the samples **should** be
different biological samples, picking a representative compound will only make the problem worse.

Let us know when you have this issue and a curator can make the sample name modification for you after your submission
is received.

## I added or edited sample rows manually.  Can I upload these files?

Yes.  Any previously unloaded (i.e. new) samples can be manually edited.

> Note that corresponding edits should be made in related sheets.  For example, if you add or edit a sample row, rows in
> the `Peak Annotation Details` sheet must also added/edited.
>
> If you want to add lots of samples from a peak annotation files (e.g. _Accucor_) that was missed when the template was
> generated, the easiest solution is likely creating a new template and copying over your work.  The benefit of this is
> that all of the extracted data is filled in in multiple sheets, e.g. novel compounds.

If you run into errors in the validate step associated with the added samples, let us know.  We will help you to load
your modified files.  Just send us what you have and we will contact you to confirm our solution is OK.

## Can I upload some data now, and upload more data from the same samples later?

Yes.  TraceBase will add new data to existing samples.  If the same compound is uploaded a second time, TraceBase will
use the latest upload.  The same is true of all data in the Study Doc.

Edited rows (outside of compound synonyms) that were already loaded are a different story and require special curator
attention.  Let us know if you need to modify any previously loaded data and a TraceBase curator will make the update.

## What kind of data can be uploaded?

See [[What Inputs Does TraceBase Take]]
