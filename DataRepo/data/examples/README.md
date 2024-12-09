# Example Data for TraceBase

This directory contains example data for TraceBase that can be used for previewing the software.

For test data to use for development, see ../tests/.

## Datasets

### Studies

These directories are organized so that they can be loaded using the `load_study` management command.

- `compounds_tissues_treatments_lcprotocols` - A loadable "study" with underlying data required by the other studies, including compounds, tissues, animal treatments, and liquid chromatography protocols that are used in the example datasets
- `13C_Valine_and_PI3Ki_in_flank_KPC_mice` - A study with one tracer compound containing a single labeled element and isocorr-generated data.
- `obob_fasted_ace_glycerol_3hb_citrate_eaa_fa_multiple_tracers` - An example study that includes animals infused with multiple tracer compounds
- `obob_fasted_glc_lac_gln_ala_multiple_labels` - An example study that includes tracers that are labeled with multiple elements (*e.g.* Carbon and Nitrogen labeling)
