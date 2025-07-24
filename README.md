# SeGg Data Pipeline

Bucket URL: https://cdl-segg.fra1.cdn.digitaloceanspaces.com

## Data Sources

All sources have a pydantic model in `/models`

### Files
- There seem to be two main sources for files: 
    - https://meinsvwissen.de/wp-content/uploads/ accessible with https://meinsvwissen.de/wp-json/wp/v2/media 
    - files upload with wp plugin  "wp file download". this has no api

- The subpage "Downloadbereich" at  https://meinsvwissen.de/sv-archiv/ is linked to the plugin "wp file download"

### Posts
- Based on Posts scraped from API at: https://meinsvwissen.de/wp-json/wp/v2/posts
- SeGg Wordpress uses elementor (a gui website builder) which results in rendered html to be outptted in wordpress api -> could not find some form of elementor api

- In this scraping project:
    - area means a logically separated part of the content at the top of the hierarchy, i.e. main content, downloads and further links
    - components refers to elementor widgets
    - sections are parts of a component

- Posts usually contain main area, "further links"(or "this might interest you", "more on this") and "downloads (or "materialsammlung")
    - downloads contains a list of downloads from a relevant chapter on the downloads page,
    -  Additional downloads are linked linked in "more on this". it is assume that the parent chapter is linked in futher links and a list of downloads from a sub chapter is displayed in downloads (see https://meinsvwissen.de/sitzungen-2/)
        - sometimes the downloads area contains the same downloads chapter as in further links (https://meinsvwissen.de/gremien-in-der-schule-und-ihre-funktion/)

- If main area or Download area contains a list of downloads, this corresponds to a linked download chapter (WP File Download in elementor).
- Download section can be called "Materialsammlung" sometimes
- we will not include list of download section in main area as section, but add it as id of dedicated  chapter
- We can select elementor widgets with css classes

- There can be posts with duplicate titles (e.g. Sitzungen)

- Further Links can also contain random links e.g. to youtube not only to internal stuff like other posts or downloads chapters

#### Elementor Tags in main area

- elementor-widget-text-editor -> text sections
- "wpfd-tree-categories-files" but without "directory" -> list of downloads
- "elementor-widget-htmega-accordion-addons" -> accordion
- iframe with source contains prezi -> prezi
- elementor-widget-video -> video


#### Main area

- Can contain accordions but also differet components such as flip cards (https://meinsvwissen.de/ideen-oeffentlichkeitsarbeit/)
- Very different media e.g. (prezi, video, quizzes)
- Accordion sections can be empty for some reason sometimes: https://meinsvwissen.de/sitzungen-2/#69-96-methoden
- Tool Type Category is not reliable as some posts contain mixed media: https://meinsvwissen.de/oeffentlichkeitsarbeit/

- Can contain links to downloads/external resources additionally to the download section/further links: https://meinsvwissen.de/mappe-fuer-die-wahl-der-schuelersprecher_innen/
- Some Posts contain the button "Volltext anzeigen", which links to book section (pimp my school) -> **in this case just use the book section?**

##### Video
- It can be youtube shorts  or videos, SeGg has a channel: https://www.youtube.com/@schuleeingesichtgeben6829
- Download video and use whisper to get transcript: 
    - apify.com/store/categories?search=youtube 
    - https://github.com/m-bain/whisperX?tab=readme-ov-file
- Use transcript api: https://www.youtube-transcript.io/api

##### Presentation

- Does not need to be embedded prezi: https://meinsvwissen.de/sv-kalender/ -> **story js**
- For Prezi: scrape transcript from linked website

##### Quiz

- where to get video transcripts?

##### Infographic (Images)

- Example: https://meinsvwissen.de/wegweiser-wie-kann-ich-andere-motivieren/
- Extract Text Content with VLMs 

#### Embedded Apps
- e.g. https://meinsvwissen.de/sv-sitzung-organisieren/ https://meinsvwissen.de/oeffentlichkeitsarbeit-konzept/
- will not scrape content from this



### Archive
#TODO might merge this with books, as it does contains (published) books as well, e.g. "Demokratiepädagogik - Demokratielernen - Eine Aufgabe der Schule" in 4.5.
- The name "Download Area" is a bit misleading, as this contains external ressources


### Glossar
- Scraped automatically from https://meinsvwissen.de/glossar/
- Contains subfields with annotations for jurisdictions if existent, e.g. in the case of "Klassenrat"

### Books/Documents
#TODO add issue with completing book selection etc. See ###Archive. Might add criteria "published" or just rename it "documents". "Documents" is defined as text not being a part of a larger document. One could add the structure of the archive as metadata. One could add a tag that gives selected books (e.g. pimp my school) priority
- Based on a manually compiled [BibLaTeX](https://ctan.org/pkg/biblatex?lang=en) library (`segg.duckdb`) based on the [collection](https://meinsvwissen.de/handbuecher/) of books on student representation on meinsvwissen.de. 
- Is converted to json file using adhering to a pydantic model
    - pdfs are downloaded and uploaded to the bucket
    - a json field for jurisdiction that uses the BibLaTex tag field is added
- One could find all documents in the posts and add the title/metadata of the post 

## Questions

- What makes a document external?
- Books: "Gesammelte Handbücher, Arbeitshilfen, Broschüren von uns und anderen Organisationen."
- Archive: "Gesammelte Beispiele und Vorlagen von anderen Schulen" (whats not an other school?)
    - Contains excerpts from the books listed in the "Handbücher" section, e.g. "1.1. Was ist Schülervertretungsarbeit?" -> merge it with books/documents
    -

## SeGG Todos

- Publish all Prezi presentations so we can access full transcript via p/id instead of /embed
- Fix download file tree
- confirming and consider to ensure/enfore logic 
