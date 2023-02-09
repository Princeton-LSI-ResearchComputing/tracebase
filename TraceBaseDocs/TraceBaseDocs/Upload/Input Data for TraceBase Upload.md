# Input Data for TraceBase

TraceBase accepts mass spectrometry data that has been corrected for the
natural abundance of isotopomers.

Specifically, tracebase supports the output from two popular tools for natural
abundance correction: [Accucor](https://github.com/lparsons/accucor)
([https://doi.org/10.1021/acs.analchem.7b00396](https://doi.org/10.1021/acs.analchem.7b00396))
and [isocorr13C15N](https://github.com/xxing9703/isocorr13C15N).

![Diagram showing the file types that can be uploaded to TraceBase](../Attachments/Input%20Formats%20Sketch.png)

Therefore, a typical workflow to put data on TraceBase could be:

1. Select peaks from a set of `.mzXML` files using
   [Maven](http://maven.princeton.edu/index.php) or
   [El-Maven](https://www.elucidata.io/el-maven).
2. Correct for natural abundance using Accucor or isocorr13C15N.
3. Upload data to TraceBase.

TraceBase uses the [Sample Information Sheet](Sample%20Information%20Sheet.md)
to label and organize this data, and produces three types of [output
formats](../Types%20of%20Data%20Output/Downloaded%20Data%20Formats.md).
