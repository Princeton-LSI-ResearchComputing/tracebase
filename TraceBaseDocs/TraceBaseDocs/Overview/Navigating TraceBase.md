# Navigating TraceBase

TraceBase can be explored in two general ways:

- Samples First:  Browse the list summaries in the sidebar
- Data First:  Advanced Search

Both of these methods filter all the data in TraceBase down to what you’re looking for.

## Browse

General exploration and browsing can help you explore the different types of data and studies available on TraceBase.
For example, it is easy to check infusion rates and FCirc for a tracer without downloading anything from TraceBase.

To begin browsing, select either one of the boxes from the homepage or a table from the sidebar.

![TraceBase Homepage](../Attachments/Snapshot%20Homepage.png)

Each page contains a table where each row represents a unique database record.  E.g. Each row on the Samples page is a
unique biological sample.  In the example below, "Studies" was selected.

![Studies table](../Attachments/Snapshot%20Studies%20Table.png)

Each table can be viewed fullscreen, searched, downloaded, and columns can be added or removed (note: many tables have
some columns hidden by default).  Some items within a table include links to see more information.  Clicking the first
study for example, opens a new page with details for that specific study showing its subset of samples:

![Study record](../Attachments/Study%20Record%20Screenshot.png)

After browsing to a study's sample subset, the actual data can be interrogated by clicking links to an advanced search
specifically for that study's data:

* [PeakGroups](../Download/About%20the%20Data/Data%20Types/PeakGroups.md)
* [PeakData](../Download/About%20the%20Data/Data%20Types/PeakData.md)
* [Fcirc](../Download/About%20the%20Data/Data%20Types/FCirc.md)

## Advanced Search

The advanced search page provides a method to rapidly drill down to exactly what you’re looking for, and the adbility to
export what you have found as a downloaded data file, containing metadata about the search that pulled up that data.

![Advanced search form](../Attachments/Snapshot%20Advanced%20Search.png)

First, select an output format ([[PeakGroups]], [[PeakData]], or [[Fcirc]]).

Next, filter the sample information.  Note this is filtering from all the data in TraceBase.  In this example, PeakGroup
data is filtered for all infusates including "glucose" and tissue names containing "serum".  This produces a result with
697 PeakGroups.

Complex search conditions are accomplished by adding rows to the search form using the green buttons associated with
each row.

* `+` adds a row
* `++` adds a row group that allows you to choose whether any or all conditions in the group are required to match
* `-` removes a row or row group

Each row has a column (i.e. database field) select list, a comparator select list (based on the column's data type, e.g.
"is" or "contains"), and a search term input.  Based on some columns' data type, a fourth "units" select list may
appear.

Clicking the Search button generates a table with a small set of columns.  To show hidden columns, click the blue
columns button at the top of the table.

To get an overview of what is included, you can click the blue "Stats" button which will open the following box:

![Summary statistics for advanced search results](../Attachments/Snapshot%20Advanced%20Search%20Stats%20Pane.png)

_Note that generating these stats can take up to a minute depending on the size of the result set._

The box in the example above shows that the currently filtered data includes 25 Animals, 42 unique measured compounds,
62 unique samples, etc.  The Stats tab is especially useful for identifying Diets, Ages, Genotypes, Feeding Statuses, or
Animal Treatments found within the currently filtered dataset.

Adjust filters and search again, or download the table.

To download all of your search results (across all pages), click the blue download button.  Note that this downloaded
table adheres to strict [Downloaded Data Formats](../Download/About%20the%20Data/Format%20of%20Downloaded%20Data.md).
