# Input Data for TraceBase

TraceBase accepts mass spectrometry data that has been corrected for the natural abundance of isotopomers.

Specifically, tracebase supports the output from two popular tools for natural abundance correction: [Accucor](https://github.com/lparsons/accucor) ([https://doi.org/10.1021/acs.analchem.7b00396](https://doi.org/10.1021/acs.analchem.7b00396)) and [isocorr13C15N](https://github.com/xxing9703/isocorr13C15N).

Therefore, a typical workflow to put data on TraceBase could be:
1. Select peaks from a set of `.mzXML` files using [Maven](http://maven.princeton.edu/index.php) or [El-Maven](https://www.elucidata.io/el-maven).
2. Correct for natural abundance using Accucor or isocorr13C15N.
3. Upload data to TraceBase.







