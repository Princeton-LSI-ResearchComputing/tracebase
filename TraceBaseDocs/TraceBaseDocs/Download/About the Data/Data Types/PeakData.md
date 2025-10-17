# PeakData

PeakData is a table of TraceBase output analagous to an AccuCor input file labeled with sample information.  It contains
sample metadata and two basic values:

* [Corrected Abundance](../../../Values/Corrected%20Abundance.md) - the abundance of each observed isotopomer for each
  compound (corrected for natural isotope abundance). (ion counts)
* [Fraction](../../../Values/Fraction.md) - the simple fraction of this isotopomer vs all others for this observed
  compound.

Downloaded PeakData has a [standard format](../Format%20of%20Downloaded%20Data.md).

PeakData is used to construct [[PeakGroups]].
