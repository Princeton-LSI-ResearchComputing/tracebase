# Example Data for TraceBase

This directory contains example data for TraceBase that can be used for
previewing the software.  For test data to use for development, see ../tests/.

## Datasets

### Studies

These directories are organized so that they can be loaded using the
`legacy_load_study` management command.

- `protocols` - A "study" with just protocols that are used in the example
  datasets
- `tissues` - A "study" with just tissues that are used in the example datasets
- `obob_fasted_ace_glycerol_3hb_citrate_eaa_fa_multiple_tracers` - An example
  study that includes animals infused with multiple tracer compounds
- `obob_fasted_glc_lac_gln_ala_multiple_labels` - An example study that
  includes tracers that are labeled with multiple elements (*e.g.* Carbon and
  Nitrogen labeling)
- `AsaelR_13C-Valine+PI3Ki_flank-KPC_2021-12_isocorr_CN-corrected` - A study
  containing multiple tracers and labels in the form of isocorr-generated data

### Other data

Other data in this directory, not yet to organized in "study" form.

- `compounds` - A list of compounds used in the example datasets
