# TraceBaseDocs
Documentation for [TraceBase](https://github.com/Princeton-LSI-ResearchComputing/tracebase) users

## Generating the Documentation
Documentation is drafted in markdown using [Obsidian](https://obsidian.md/), or other markdown editors. This set of markdown documents is used to generate a static site using [MkDocs](https://www.mkdocs.org/) (with a [readthedocs](https://readthedocs.org/) template).  Currently, this site is only served locally.  The site updates live as changes are made to the markdown documents in Obsidian (or other markdown editors).

### Setting up the obsidian vault
1) Install the [Obsidian](https://obsidian.md/) electron app.
2) Launch the app, select `open another vault`
3) Select the `TraceBaseDocs` folder within this repo.
4) Confirm the following settings to ensure compatability between Obsidian and mkdocs:

 * Settings > Files and Links > New Link Format = Relative path to file
 * Settings > Files and Links > Use [[Wikilinks]] = toggled off
 
These settings ensure consistent link behavior between Obsidian and mkdocs.  Settings are saved in the `.obsidian` folder within this repo.

### Set up mkdocs
To generate the static site from these markdown documents, install mkdocs and related plugins.
1) `pip install mkdocs` (requires Python and pip as package manager)
2) `pip install mkdocs-roamlinks-plugin` ([roamlinks](https://github.com/Jackiexiao/mkdocs-roamlinks-plugin) adds support for some Obsidian links)

### Serve the static site
Use mkdocs to generate the static site (locally).
1) `python mkdocs serve --verbose` (verbose is optional but recommended)

The site can be accessed at http://127.0.0.1:8000/repo-name/

## Known Issues and Workarounds
Markdown notation includes some quirks and Obsidian interpretation of markdown is slightly different than mkdocs in some cases.  Here is a list of differences between Obsidian and mkdocs / basic requirements for markdown documentation:
 * Obsidian supports links to other markdown pages inside headers, but mkdocs does not.
 * Brackets (`[`) in obsidian text need to be escaped with `\`
 * Bullets in Obsidian do not require an extra line, but in mkdocs they do.
 * To add a new line without creating extra white space in mkdocs, include `<br/>` at the end of the line.  Obsidian does not require this.



