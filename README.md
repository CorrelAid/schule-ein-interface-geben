## Data Sources

All sources have a pydantic model in `/models`

### Posts
- Based on Posts scraped from API at: https://meinsvwissen.de/wp-json/wp/v2/posts
- html will have to be processed into structured data, e.g. "further links"
- Very different media (prezi, video, quizzes)
- contains links to other ressources (again, documents...)
#### Types of media
    - how to use quizes?
    - idea for prezi: use transcript
    - where to get video transcripts?
    - 

### Archive
#TODO might merge this with books, as it does contains (published) books as well, e.g. "Demokratiepädagogik - Demokratielernen - Eine Aufgabe der Schule" in 4.5.
- The name "Download Area" is a bit misleading, as this contains external ressources


### Glossar
#TODO add issue 
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