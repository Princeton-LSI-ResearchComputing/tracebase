# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

none

## [2.0.1] - 2022-12-12

### Added

- Advanced Search
  - The last/previous serum sample (peak group) status is now searchable.
  - Animal age and sample time collected are now searchable in weeks, days, hours, or minutes using decimal values.
  - Added a status column to the FCirc page that shows warnings/errors about the validity of the FCirc calculations.
  - Added infusate/tracer fields to advanced search field select lists.
  - Added Tracers, Tracer Compounds, Concentrations, and modified the display of Infusates to the advanced search results and download templates.
- Pages/views
  - Added infusate list page.
  - Add Help menu link to Google Drive folder.

### Changed

- Advanced Search
  - Last serum sample (peak group) determination now falls back to previous serum sample if a tracer compound was not picked in the last serum sample's MSRun.
  - Clicking previous/last FCirc checkboxes now repaginates to always show a constant number of rows per page of results.
  - Advanced search results now link to infusate details pages.
  - Changed displayed isotope naming format to look more similar to the loading template format, but with concentrations included.
  - Fixed overlooked issue with labeled element and count leftover from the multi-tracer/label template update.
  - Fixed an overlooked multi-tracer/label issue with the concentrations in the fcirc tsv download template.
- Pages/views
  - Improved infusate and sample column sorting on detail/list view pages.
  - Minor display and sort bugfixes.
  - Improved pagination performance in Infusate and Sample list/detail view pages.
  - Changes to protocol and compound views and almost all templates for list views.
  - Added dynamic table height to some view pages.
  - Split the protocol list views into two: animal treatment and msrun protocol.
  - Minor page width issue fixed to prevent confusing horizontal scrollbars from appearing.
- Data submission/loading
  - Improved protocol loading (for treatments and MSRun protocols).
  - Updated loading template.
- Dependencies
  - Django updated to 3.2.16.

### Removed

- Data submission/loading
  - Upload data validation page temporarily disabled for improvements.
