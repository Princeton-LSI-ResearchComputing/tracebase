# Development Notes

This document will serve to guide developers on implementing new code.

## How to add an advanced search output format

### Procedure

1. `DataRepo/formats/<format name>DataFormat.py`
   - Copy and rename PeakGroupsFormat.py and make the following edits
      - Set a new ID and name.
      - Set a root model
      - Determine the root queryset.  If you don't want the browse functionality to return all records of the root model (e.g. PeakData.objects.all()) (i.e. a filter is required), add a method to the class called getRootQuerySet() that overrides the base class version, and returns the filtered queryset.  See FluxCircFormat.py for an example.
         - Note that if you want your pre-filter to be transparent to the user, you can alternatively override the base class's value for `static_filter`.  Any searches you set there, as the value of the `tree` member of the qry object (see the static_filter commented example), will show in the hierarchical search form, but will not be editable to the user.
      - Fill in the model_instances data: every model, it's path (from the root table to the current table), it's reverse_path (from the current table to the root table), and all its fields.
        (Note that paths (a.k.a. "key paths") use the foreign key names in the models or the "related_name" in the model being linked to.  The forward path does not include the root table and the reverse_path does not include the current table.)
         - Note that the model_instances key can be the model name, but if you need 2 instances of the the same model in the composite view (e.g. "Tracer Compound" linked from Animal and "Measured Compund" linked from PeakGroup, a different instance name must be used.  It may contain no spaces, and is what would be used to create a link for the model from `search_basic` to the advanced search.
         - Cached properties should not be searchable.
         - AutoFields (like IDs) should not be displayed (because they can change depending on how the data is loaded from scratch, thus, they should be obfuscated from the user).
         - If `displayed` is False, set a `handoff` key whose value is a unique field (e.g. `name`).  See any `id` field in the copied class's models data.
   - If not already there, add the root model to the DataRepo.models import at the top of the file.
   - Set each model's manytomany["is"] value as True/False based on whether it is in a many-to-many relationship with the root model.  Any many-to-many relationship on the key path necessitates a many-to-many status for that model instance.  Set each model's manytomany["split_rows"] value as True/False based on whether the template should display a separate row for every root-model/M:M-model combo.
      - A default annotation field (`manytomany["root_annot_fld"]`) will be created that is the lower case version of the M:M model name, but if this causes a conflict with existing root model fields, you can:
         - specify a custom annotation field name by setting `manytomany["root_annot_fld"]`.  E.g. For the MeasureCompound model instance, `manytomany["root_annot_fld"]` is explicitly set to `compound` in the PeakGroupSeachView class.
   - Add the new class to the for loop in SearchGroup.__init__

2. `DataRepo/forms.py`
   - Add an import at the top of the class created in step 1 above
   - Copy and rename AdvSearchPeakGroupsForm and:
      - Set the data member `format_class` to the class from step 1
      - Add the new the new class to the loop in the __init__ function of the AdvSearchForm class.

3. `DataRepo/templates/results/<format name>.html`
   - Copy `DataRepo/templates/results/peakgroups.html` to a new file with a name that indicates the format and edit as you wish, following these guidelines:
      - If a field's path includes a many-to-many relationship, e.g. `models.ManyToManyField`, and the resulting table should include a row for every root-model/M:M-model combo...
         - See the `manytomany` settings for step 1 above.
         - See the MeasuredCompound column code in the `peakgroups.html` for an example.  Othwerwise, follow these guidelines:
            - You should not create a nested `for` loop.  Instead, you can retrieve the M:M related record to be included on the current row of the outer loop by calling the `get_manytomany_rec` template tag.  Based on the `manytomany` config, it will either return all related records as a list or it will return the one related table record (also as a list).
            - It is encouraged that you implement a `for` loop inside the `<td>` tags that can render the records either as a delimited series or as an individual value so that how the field is displayed can be toggled by the `manytomany` settings.
      - If there are no M:M relationships, the nested `for` loop in the copied template may be removed.
      - Use column headers that match the field's displayname set in step 1 so that they match the field select list.  (A reference to this value may be supplied in the future.)
      - Numeric values should use `<td class="text-end">`
      - Numeric values that have long decimal strings should be formatted with a tooltip like this: `<p title="{{ rec.longval }}">{{ rec.longval|floatformat:4 }}</p>`
      - Name fields should be linked to their details page using their ID, e.g. `<a href="{% url 'peakgroup_detail' pg.id %}">{{ pg.name }}</a>`

4. `DataRepo/templates/downloads/<format name>_{colheads,row}.tsv`
   - Copy `DataRepo/templates/downloads/peakgroups_colheads.tsv` and `DataRepo/templates/downloads/peakgroups_row.tsv` to new files with a name that indicates the format (e.g., same name as in step 3 with a different extension) and edit as you wish, following these guidelines:
      - If a field's path includes a many-to-many relationship, e.g. `models.ManyToManyField`, and the resulting table should include a row for every root-model/M:M-model combo...
         - See the `manytomany` settings for step 1 above.
         - See the MeasuredCompound column code in the `peakgroups.html` for an example.  Othwerwise, follow these guidelines:
            - You should not create a nested `for` loop.  Instead, you can retrieve the M:M related record to be included on the current row of the outer loop by calling the `get_manytomany_rec` template tag.  Based on the `manytomany` config, it will either return all related records as a list or it will return the one related table record (also as a list).
            - It is encouraged that you implement a `for` loop inside the `<td>` tags that can render the records either as a delimited series or as an individual value so that how the field is displayed can be toggled by the `manytomany` settings.
      - If there are no M:M relationships, the nested `for` loop in the copied template may be removed.
      - Use column headers that match the field's displayname set in step 1 so that they match the field select list.  (A reference to this value may be supplied in the future.)

5. `DataRepo/templates/DataRepo/search/results/display.html`
   - Copy the `{% elif selfmt == "pdtemplate"...` line and the include line below it, paste it above the following `else`, and make the following edits:
      - Replace both occurrences of `pdtemplate` with the ID you assigned at the top of step 1
      - Replace the filename on the include line with the file created in step 3 above

6. `DataRepo/templates/DataRepo/search/downloads/download_{header,row}.tsv`
   - In both files, copy the `elif` that looks like:
     `{% elif qry.selectedtemplate == "pdtemplate" %}{% include "DataRepo/search/downloads/peakdata_{colheads,row}.tsv" %}`
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

## How to add/remove columns to an advanced search output format

1. `DataRepo/formats/<format name>DataFormat.py`
   - If the model exists in the models datamember
      - Copy one of the existing fields, paste, and edit.  See field editing notes below.
   - Else (if the model does not exist in the models datamember)
      - Copy one of the existing models, paste, and edit.
      - Always include the primary key as a searchable field.
      - See field editing notes below.
   - Field editing notes:
      - Note that all this class does is add the field to the fld field in the search form.  It does not affect the template.
      - "searchable" should only be False if it is a cached property.  Every visible column should otherwise be searchable.
      - Use the same display name as is used in the column header.
      - If a field is not "displayed"...
         - it will not appear to the user in the fld select list.
         - When it also *is* searchable, it means that we use this field to link to the advanced search using a primary key or otherwise obfuscated field value (like auto-generated IDs that may not persist between DB rebuilds).
         - A "handoff" field that specifies how the search form will be pre-filled out must be added.  The handoff field must be unique (or uniquely correspond to the field that is used in the link).

2. `DataRepo/templates/results/<format name>.html`
   - Each template is different, but generally, unless the model doesn't already exist in the template, just add a column to the HTML table.
   - If a model doesn't already exist in the template and is related to the root model, a nested loop must be added.  All current models have at least 1 such model (e.g. Study).  Follow its example.
   - General guidelines:
      - Number fields should be right-aligned
      - Name fields should be linked to the model's detail page
      - Long decimal values should be shortened/truncated
      - None values should be ensured to display as "None" so they can be differentiated from empty string
      - Manipulated values (like truncated decimal values) should have a tooltip that shows the full value
      - Headers should show units in parenthases if a value has units

3. `DataRepo/templates/downloads/<format name>_{colheads,row}.tsv`
   - Each template is different, but generally, unless the model doesn't already exist in the template, just add a column to the tab-delimited headers and row files.
   - General guidelines:
      - Field values should not be manipulated/modified (e.g. do not truncate decimal places) except to match the format supplied by researchers in the loading files (when the stored version in the database differs, e.g. Sample.time_collected or Animal.age, which are saved as time-deltas)
      - None values should be ensured to display as "None" so they can be differentiated from empty string
      - Headers should show units in parentheses if a value has units
