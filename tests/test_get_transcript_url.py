from lib.rendered_scraping import get_transcript_url

def test_get_prezi_transcript_url():
    media_url = "https://meinsvwissen.de/wp-content/uploads/2025/08/Klimaschutz-1.jpg"
    transcript_url = get_transcript_url(media_url)
    
    media_url = "https://prezi.com/view/VS3INtXFDLbyR0z806Ei/"
    transcript_url = get_transcript_url(media_url)
    assert transcript_url == "https://meinsvwissen.de/wp-content/uploads/2025/07/rollen-sv-team.pdf"

    media_url = "prezi.com/view/GWGX1XDylIhHeu0InLo8/"
    transcript_url = get_transcript_url(media_url)
    assert transcript_url == "https://meinsvwissen.de/wp-content/uploads/2025/07/ideen.pdf"

def test_get_video_transcript_url():
    media_url = "https://youtu.be/ZqFnl5tJi7o"
    transcript_url = get_transcript_url(media_url)
    assert transcript_url == "https://meinsvwissen.de/wp-content/uploads/2025/07/Videoskript-SV-macht-Klimaschutz.pdf"


