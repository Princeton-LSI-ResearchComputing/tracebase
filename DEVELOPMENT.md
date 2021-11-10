# Development Notes

This document will serve to guide developers on implementing new code.

## How to add an advanced search output format

### Procedure

1. `DataRepo/compositeviews.py`
   - Copy and rename PeakGroupsSearchView and make the following edits
      - Set a new ID and name.
      - Set a root model
      - Determine the root queryset.  If it's not all records of the root model (e.g. PeakData.objects.all()), and a filter is required, add a method to the class called getRootQuerySet() that overrides the base class version, and returns the filtered queryset.  See FluxCircSearchView for an example.
         - Note that if you want your pre-filter to be transparent to the user, you can alternatively override the base class's value for `static_filter`.  Any searches you set there, as the value of the `tree` member of the qry object (see the static_filter commented example), will show in the hierarchical search form, but will not be editable to the user.
      - Fill in all the prefetch paths from (but not including) the root model to every model leaf.  The path strings are the foreign key field names in the parent model.
      - Fill in the models data: every model, it's path (from the prefetches, the root model will be an empty string), and all its fields.
         - Cached properties should not be searchable.
         - AutoFields (like IDs) should not be displayed (because they can change depending on how the data is loaded from scratch, thus, they should be obfuscated from the user).
         - If `displayed` is False, set a `handoff` key whose value is a unique field (e.g. `name`).  See any `id` field in the copied class's models data.
   - If not already there, add the root model to the DataRepo.models import at the top of the file.
   - Set each model's many-to-many value based on whether it is in a many-to-many relatioship with the root model.
   - Add the new class to the for loop in BaseAdvancedSearchView.__init__

2. `DataRepo/forms.py`
   - Add an import at the top of the class created in step 1 above
   - Copy and rename AdvSearchPeakGroupsForm and:
      - Set the data member `composite_view_class` to the class from step 1
      - Add the new the new class to the loop in the __init__ function of the AdvSearchForm class.

3. `DataRepo/templates/results/<format name>.html`
   - Copy `DataRepo/templates/results/peakgroups.html` to a new file with a name that indicates the format and edit as you wish, following these guidelines:
      - If a field's path includes a many-to-many relationship, e.g. `models.ManyToManyField`
         - A nested `for` loop will be necessary in the template, e.g. `{% for study in pg.msrun.sample.animal.studies.all %}`.  If there are no M:M relationships, the nested `for` loop in the copied template may be removed.
         - The record from the inner loop must be added to the `mm_lookup` dict using addToDict, e.g. `{% addToDict mm_lookup "peak_group__msrun__sample__animal__studies" study %}`.  Here, a "study" record object is added.  The key must be the model's path from compositeviews for the M:M model (e.g. the Study model).
      - Use column headers that match the field's displayname set in step 1 so that they match the field select list.  (A reference to this value may be supplied in the future.)
      - Numeric values should use `<td class="text-end">`
      - Numeric values that have long decimal strings should be formatted with a tooltip like this: `<p title="{{ rec.longval }}">{{ rec.longval|floatformat:4 }}</p>`
      - Name fields should be linked to their details page using their ID, e.g. `<a href="{% url 'peakgroup_detail' pg.id %}">{{ pg.name }}</a>`

4. `DataRepo/templates/downloads/<format name>.tsv`
   - Copy `DataRepo/templates/downloads/peakgroups.tsv` to a new file with a name that indicates the format (e.g., same name as in step 3 with a different extension) and edit as you wish, following these guidelines:
      - If a field's path includes a many-to-many relationship, e.g. `models.ManyToManyField`
         - A nested `for` loop will be necessary in the template, e.g. `{% for study in pg.msrun.sample.animal.studies.all %}`.  If there are no M:M relationships, the nested `for` loop in the copied template may be removed.
         - The record from the inner loop must be added to the `mm_lookup` dict using addToDict, e.g. `{% addToDict mm_lookup "peak_group__msrun__sample__animal__studies" study %}`.  Here, a "study" record object is added.  The key must be the model's path from compositeviews for the M:M model (e.g. the Study model).
      - Use column headers that match the field's displayname set in step 1 so that they match the field select list.  (A reference to this value may be supplied in the future.)

5. `DataRepo/templates/DataRepo/search/results/display.html`
   - Copy the `{% elif selfmt == "pdtemplate"...` line and the include line below it, paste it above the following `else`, and make the following edits:
      - Replace both occurrences of `pdtemplate` with the ID you assigned at the top of step 1
      - Replace the filename on the include line with the file created in step 3 above

6. `DataRepo/templates/DataRepo/search/downloads/download.tsv`
   - Copy the `elif`:
     `{% elif qry.selectedtemplate == "pdtemplate" %}{% include "DataRepo/search/downloads/peakdata.tsv" %}`
     Paste it before the `endif` and make the following edits:
      - Replace `pdtemplate` with the ID you assigned at the top of step 1
      - Replace the filename on the include line with the file created in step 4 above

7. `DataRepo/templates/navbar.html`
   - For each item in the download `dropdown-menu`, edit the `qryjson` value to add the entry for the new format template.  Basically, you need to append an edited version of:
      `, \&quot;fctemplate\&quot;: {\&quot;name\&quot;: \&quot;FCirc\&quot;, \&quot;tree\&quot;: {\&quot;pos\&quot;: \&quot;\&quot;, \&quot;type\&quot;: \&quot;group\&quot;, \&quot;val\&quot;: \&quot;all\&quot;, \&quot;queryGroup\&quot;: []}}`" just before "`}}&quot;`
   at the end of the value string.  `fctemplate` and `FCirc` of the above string must be changed to the template ID and name used in step 1.
   - Copy the entire last item of the same `dropdown-menu` (from `<li>` to `</li>`), paste it at the end of the list, and...
      - Change the `selectedtemplate` in the `qryjson` value to the template ID used in step 1.
      - Change the submit button text for the new format.  E.g. change `All FCirc Data` to use the name set in step 1.

### Notes

Changing a format's content should be indicated by a version number specific to that format, added as a header to the format file.  Note that a header will be automatically added to the downloaded .tsv file that contains a timestamp, user info, and the search query.

Be careful that the .tsv file has actual tab characters and note that every newline character in the template will end up in every downloaded file, which is why the lines are so long.

The header line of the .tsv file should be commented.
