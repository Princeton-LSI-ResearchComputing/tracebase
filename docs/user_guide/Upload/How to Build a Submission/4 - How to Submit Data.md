# How to Submit Data

The Upload **Submit** page instructs researchers on where to put their data and how to npotify curators that data is
ready to be checked and loaded by a TraceBase curator.  It also describes a recommended directory structure for their
submission.

Submissions include a mix of the following types of files:

* Study Doc (an Excel file)
* Peak annotation files
* `mzXML` and `RAW` files

## General File Guidelines

* The name of the Study Doc should reflect the name of the study, but there are no strict guidelines.
* It is suggested that all file and directory names should avoid spaces, when possible.
* Refrain from editing the content of the Peak Annotation Files

Other files are allowed, but are not used in the load.

## Associating `mzXML`/`RAW` Files with Peak Annotation Files

**You can arrange the study directory however you want**, but where possible, it is best to **colocate**
`peak annotation files` (e.g. _AccoCor_ files) with the raw files that were used to generate them, ideally with one
`peak annotation file` per directory.  This will make resolving which same-named `mzXML` files go with which
`peak annotation files` easier.

`Peak annotation files` are automatically associated only with the `mzXML`/`RAW` files in the **immediate** directory
(not those in subdirectories), however there are exceptions.

If a `peak annotation file` was generated with a mix of `mzXML` files, some (but not all) of which were used to generate
other `peak annotation files`, place the `peak annotation files` in a common parent directory, under which multiple
subdirectories contain all of its `mzXML`/`RAW` files, even though it may include unassociated raw files.  Arranging
them this way will usually avoid the ambiguities created by multiple `mzXML` files with the same name, but only having 1
file of that name under the `peak annotation file`'s current directory.

If 2 `mzXML` files of the same name exist under a parent directory that contains a `peak annotation file` in which one
of the 2 was used, a curator will populate the `mzXML file names` column in the `Peak Annotation Details` sheet of the
`Study Doc` for you, to contain the relative path of the one that was used to generate the `peak annotation file`, but
they may need to reach out to you to know which same-named `mzXML` goes with which `peak annotation file`.
