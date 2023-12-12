# How to label Mass Spec Run Information

Information about the Mass Spectrometry used is collected in a google form when
data is submitted.  Note, an LCMS-metadata file can be used to denote multiple
different values for the options below. If such a file is provided, the values
entered into the form are used as defaults for any samples that are not included
in the file.

A description of each field is listed here:

## Mass Spec Run Date

- Date that the sequence was ran on on the machine

## Mass Spec Instrument

- The instrument that was used for the run
- Possible values:
  - HILIC
  - QE
  - QE2
  - QTOF

## Mass Spec Operator

- Researcher who prepared samples for MS and ran them on the machine

## LC Method

- Name of the Liquid Chromatography method used
- Possible values:
  - unknown
  - polar-HILIC-25-min
  - polar-reversed-phase-ion-pairing-25-min
  - polar-reversed-phase-25-min
  - lipid-reversed-phase-25-min
- The submission form only takes predefined values. If the method is not
  available among the options, select "Other" and enter a description that
  includes the name and run length. Example:
  - lipid-reversed-phase-25-min: This method involves the analysis of lipids
    using reversed-phase chromatography on C18 columns, coupled with high
    resolution mass spectrometry. It is mainly used for the analysis of lipids.

## Polarity

- Ion mode used for the Mass Spectrometer
  - Either  "negative" or "positive"
