# SeGg Data Pipeline

- Bucket URL: https://cdl-segg.fra1.cdn.digitaloceanspaces.com
- See `exploration.ipynb` for examples on how to load the data and simple descriptive statistics.
- Main pipeline script: `pipeline.py`. Stages are described in the log messages.
- Also see `/tests`, e.g. `tests/test_post_main_scraping.py` to get a feel for the data and how it is structured.

## Data Sources

All sources have a schema in `lib/src/lib/models.py`

### Knowledge Modules / Posts

- SeGg uses the Elementor website builder, which allows designing posts via a GUI (the output is stored in the API as HTML).
- There are knowledge modules as posts with:
  - A main section
  - An optional download section
  - An optional “More Info” section
- If a download section exists, it refers to a dedicated chapter in the SV archive: [https://meinsvwissen.de/sv-archiv/](https://meinsvwissen.de/sv-archiv/).
- The “More Info” section contains links to other posts and other chapters in the SV archive.
- The main section can include various media (see `lib/src/lib/models.py` and `exploration.ipynb` for an overview)

### Download Section / SV Archive

- “Collected examples and templates from other schools.”
- Contains documents and videos.
- Documents may include excerpts from your book.

### Manuals

- Contains complete works (i.e., no excerpts from other works).
- Includes books published by you and others, as well as unpublished brochures.

### Glossary

- Definitions of terms and abbreviations, sometimes differentiated by federal state.
