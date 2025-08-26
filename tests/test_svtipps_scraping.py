from lib.scraping import scrape_svtipps

def test_scrape_svtipps():
    svtipps = scrape_svtipps(sample_k=3) 
    print(svtipps)
    
    assert len(svtipps) > 0, "Should scrape at least one page"
    assert len(svtipps) <= 3, "Should respect sample_k limit"
    
    assert svtipps["title"].null_count() == 0, "All pages should have titles"
    assert svtipps["url"].null_count() == 0, "All pages should have URLs"
    assert svtipps["html_content"].null_count() == 0, "All pages should have content"
    
    for url in svtipps["url"]:
        assert url.startswith("https://svtipps.de/"), f"Invalid URL: {url}"