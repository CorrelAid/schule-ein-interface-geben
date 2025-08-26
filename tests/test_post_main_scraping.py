import logging
import sys
import requests
from lib.post_parsing import process_posts_row
from rich.logging import RichHandler

logging.basicConfig(
    level=logging.DEBUG,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=sys.stdout, rich_tracebacks=True)],
)
logger = logging.getLogger()  # root logger


def get_result(post_id, logger=logger):
    response = requests.get(f"https://meinsvwissen.de/wp-json/wp/v2/posts/{post_id}")
    response.raise_for_status()
    data = response.json()
    content = data["content"]["rendered"]
    title = data["title"]["rendered"]
    row = {
        "id": post_id,
        "content": content,
        "title": title,
    }
    result = process_posts_row(row, logger)
    return result


# def test_post_scraping_long_accordion():
#     post_id = 4889  # https://meinsvwissen.de/sv-macht-klimaschutz/

#     sections_types_gt = [
#         "plain_text",
#         "accordion_section_prezi",
#         "accordion_section_text",
#         "accordion_section_youtube",
#         "accordion_section_youtube",
#         "accordion_section_text",
#         "accordion_section_text",
#         "accordion_section_prezi",
#         "accordion_section_text",
#         "accordion_section_image",
#         "accordion_section_image",
#         "plain_text",
#     ]

#     result = get_result(post_id)

#     result_sections_types = [section["type"] for section in result]
#     assert result_sections_types == sections_types_gt

#     result_transcript_url = result[4]["transcript_url"]
#     assert (
#         result_transcript_url
#         == "https://meinsvwissen.de/wp-content/uploads/2025/07/Videoskript-SV-macht-Klimaschutz.pdf"
#     )

# def test_post_scraping_just_text():
#     # see also tests/test_related_post_scraping.py -> test_related_post_in_text
#     post_id = 7022  # https://meinsvwissen.de/andere-schulformen/
#     post_gt = [
#         {
#             "type": "plain_text",
#             "text": "Wir arbeiten derzeit hauptsächlich an staatlichen weiterführenden Schulen. Hier beginnt eine erste Sammlung von Materialien für Förderschulen, Schulen in freier Trägerschaft oder Berufs- und Fachschulen.   [Ladet gern euer Material hoch!](https://meinsvwissen.de/feedback/)[Hier geht es zum **Bereich für Grundschulen**.](https://meinsvwissen.de/grundschulen/)",
#             "post_id": 7022,
#         }
#     ]

#     result = get_result(post_id)
#     assert result == post_gt


# def test_h5p():
#     post_id = 8151  # https://meinsvwissen.de/rechte-quiz-nrw/
#     post_gt = [
#         {
#             "type": "plain_text",
#             "text": "Es ist wichtig, die Mitbestimmungsrechte zu kennen, um sie durchsetzen zu können. Nachdem ihr euch mit der Präsentation über eure Rechte informiert habt,\xa0 nutzt nun das Rechte-Quiz, um euer Wissen zu testen. Es eignet sich auch wunderbar für eine Auftaktveranstaltung mit allen Klassensprecher\\_innen.",
#             "post_id": 8151,
#         },
#         {"type": "h5p", "title": "SV-Quiz NRW", "post_id": 8151},
#     ]

#     result = get_result(post_id)
#     assert result == post_gt


# def test_quiz():
#     post_id = 6176  # https://meinsvwissen.de/test-wie-sv-freundlich-ist-unsere-schule/
#     post_gt = [
#         {
#             "type": "quiz",
#             "text": "\nNeben Schüler:innen, die Lust haben sich für ihre Schule zu engagieren und sie mitzugestalten, braucht es Offenheit von der Schulleitung und den Lehrkräften. Wenn diese schlecht informiert sind über die Mitbestimmungsrechte von Schüler:innen oder die SV nicht unterstützen, kann die SV weniger gut arbeiten. Mit diesem Quiz könnt ihr herausfinden, wie SV-freundlich eure Schule ist. Es gibt weder richtig noch falsch – antwortet so ehrlich wie möglich.\n",
#             "post_id": 6176,
#         }
#     ]
#     result = get_result(post_id)
#     assert result == post_gt


# def test_multiple_media_outside_accordion():
#     post_id = 4863
#     post_gt = [
#         {
#             "type": "plain_text",
#             "text": "Ein SV-Kalender kann euch helfen euer Jahr zu strukturieren und dadurch immer zu wissen, was als nächstes ansteht und zu tun ist. Damit habt ihr nicht nur die gemeinsamen Ziele vor Augen, sondern es gibt auch gleich einen klaren Plan für Neueinsteiger\\_innen.",
#             "post_id": 4863,
#         },
#         {
#             "type": "image",
#             "external_link": "https://meinsvwissen.de/wp-content/uploads/2022/03/Jahresuebersicht-1-1.jpg",
#             "post_id": 4863,
#         },
#         {
#             "external_link": "https://www.youtube.com/watch?v=lhRnkcHCjMc",
#             "post_id": 4863,
#             "title": None,
#             "transcript_url": None,
#             "type": "youtube",
#         },
#         {
#             "type": "h5p",
#             "title": "Ablauf eines SV-Jahres: Beispiel-Kalender",
#             "post_id": 4863,
#         },
#         {
#             "type": "image",
#             "external_link": "https://meinsvwissen.de/wp-content/uploads/2022/03/Insta-Post-SV-Kalender-1-1024x1024.jpg",
#             "post_id": 4863,
#         },
#         {
#             "type": "image",
#             "external_link": "https://meinsvwissen.de/wp-content/uploads/2022/03/Insta-Post-SV-Kalender-2-1024x1024.jpg",
#             "post_id": 4863,
#         },
#     ]
#     result = get_result(post_id)
#     print(result)
#     assert result == post_gt


# def test_other_long_accordion():
#     post_id = 4946
#     sections_types_gt = [
#         "plain_text",
#         "accordion_section_youtube",
#         "accordion_section_youtube",
#         "accordion_section_youtube",
#         "accordion_section_youtube",
#         "accordion_section_youtube",
#     ]

#     result = get_result(post_id)
#     print(result)
#     result_sections_types = [section["type"] for section in result]
#     print(result_sections_types)
#     assert result_sections_types == sections_types_gt


# def test_single_yt_transcript():
#     post_id = 7303
#     sections_types_gt = ["plain_text", "youtube"]
#     result = get_result(post_id)
#     print(result)
#     result_sections_types = [section["type"] for section in result]

#     assert result_sections_types == sections_types_gt

#     # result_yt_url = result[1]["external_link"]
#     # assert result_yt_url == "https://youtu.be/SzDEKDoRfwg" #https://youtu.be/SzDEKDoRfwg

#     result_transcript_url = result[1]["transcript_url"]
#     assert result_transcript_url == "https://meinsvwissen.de/wp-content/uploads/2025/07/Videoskript-Schulkonferenz.pdf"


# def test_prezi_transcript():
#     post_id = 7401
#     sections_types_gt = [
#         "plain_text",
#         "accordion_section_text",
#         "accordion_section_text",
#         "accordion_section_prezi",
#         "accordion_section_text",
#         "accordion_section_prezi",
#         "accordion_section_text",
#         "accordion_section_image",
#         "accordion_section_text",
#         "accordion_section_image",
#         "accordion_section_text",
#         "accordion_section_image",
#         "accordion_section_text",
#         "accordion_section_youtube",
#         "accordion_section_image",
#         "accordion_section_image",
#         "accordion_section_text",
#         "accordion_section_image",
#         "accordion_section_image",
#     ]
#     result = get_result(post_id)
#     print(result)
#     result_sections_types = [section["type"] for section in result]
#     assert result_sections_types == sections_types_gt
    

#     result_transcript_url = result[3]["transcript_url"]
#     assert result_transcript_url is None

def test_typeless_sections():
    post_id = 8969
    result = get_result(post_id)
    print(result)