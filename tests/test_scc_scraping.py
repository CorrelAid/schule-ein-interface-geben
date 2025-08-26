from lib.scraping import scrape_scc

def test_scrape_scc():
    sccs = scrape_scc()
    print(sccs)