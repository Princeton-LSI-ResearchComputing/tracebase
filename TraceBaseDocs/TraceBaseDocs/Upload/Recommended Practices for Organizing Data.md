# Recommended Practices for Organizing Data

TraceBase is designed to match an LCMS sample name to a biological sample (specific tissue from an animal).  If the LCMS sample file is named to uniquely identify a biological sample, upload to TraceBase is very easy.  To accomplish this, the following key information should be included in the sample name:
- Animal ID
- Tissue
- Time Collected (e.g. if there are multiple serum samples from one animal, the time collected distinguishes these)

Some researchers keep a single list of Animal IDs for all experiments.  This works well for TraceBase, because each Animal ID is unique.  Other researchers keep a list of animal IDs for each experiment (aka "Study").  In this example, the study should have a unique identifier and that should be combined with the animal identifier (e.g. "study001_mouse001").  This also works well for TraceBase.

If the Animal ID or Sample ID is not unique, upload is more difficult but still achievable.  Feel free to upload what you have and developers can help make your data compatible.

Note that these labeling schemes can be applied to your general organization of data outside of tracebase:
- For a new experiment, create a new identifier counting up from "study001"
  - A related follow up experiment may have an extension, e.g. "study001b, study001c"
  - Any text could be used in place of "study"
- Within each experiment, count animal identifiers "study001_m01, study001_m02..."
- When labeling sample files, provide full animal identifier, tissue, and sometimes time collected:
  - E.g. "study001_m01_quad, study001_m01_tailserum_000, study001_m01_tailserum_120"
- When working with samples in the lab, it is not feasible to label every tube with the full identifier, but shorthand can be used for intermediate tubes if everything is from the same study:
  - E.g. for tissue extraction from study001, label working tubes "Q_1, Q_2...".  Label final tubes "001_m1_Q"
- Data and any other information related to your experiments can be organized in folders labeled for each study "study001_my first infusion", "study001b_fixing my first infusion"
