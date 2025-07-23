import logging
import sys
import requests
from lib.rendered_scraping import process_posts_row
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


def test_post_scraping_long_accordion():
    post_id = 6461  # https://meinsvwissen.de/ws-beziehungspflege/

    post_gt = [
        {
            "title": "Wichtige Verbündete",
            "type": "accordion_section_text",
            "text": "Gemeinsam ist man stärker! Sucht euch unter den Erwachsenen in eurer Schule Verbündete, um eure Ideen schneller und einfacher umzusetzen. Durch ein größeres Netzwerk habt ihr viel mehr Möglichkeiten, Projekte durchführen zu können.",
            "post_id": 6461,
        },
        {
            "title": "Wichtige Verbündete",
            "type": "accordion_section_youtube",
            "external_link": "https://www.youtube.com/embed/uuWZjJpsUuU?feature=oembed",
            "post_id": 6461,
        },
        {
            "title": "Wegweiser: Wie kann ich andere motivieren?",
            "type": "accordion_section_text",
            "text": "Wo muss ich anfangen beim Thema Motivation? Erhalte hier einen ersten Überblick über die richtigen Fragen, die du dir stellen solltest.",
            "post_id": 6461,
        },
        {
            "title": "Wegweiser: Wie kann ich andere motivieren?",
            "type": "accordion_section_image",
            "external_link": "https://meinsvwissen.de/wp-content/uploads/2022/05/Andere-Motivieren.png",
            "post_id": 6461,
        },
        {
            "title": "Eine Vollversammlung durchführen",
            "type": "accordion_section_text",
            "text": "Als Schülervertretung könnt ihr regelmäßig eine Vollversammlung für die gesamte Schule einberufen. So habt ihr die Möglichkeit allen von euren Projekte, Ideen und Aktionen zu berichten und auch neue Mitstreiter_innen zu motivieren.",
            "post_id": 6461,
        },
        {
            "title": "Vollversammlung durchführen",
            "type": "accordion_section_youtube",
            "external_link": "https://www.youtube.com/embed/O8jebNJuuqQ?feature=oembed",
            "post_id": 6461,
        },
        {
            "title": "Präsentation Motivation",
            "type": "accordion_section_text",
            "text": "Für eine gut funktionierende Schülervertretung benötigt es natürlich auch motivierte Schüler_innen, die mitmachen wollen. Dafür haben wir für euch einige Tipps zusammengestellt, wie ihr in eurer Schule noch jeden für die SV Arbeit begeistern könnt!",
            "post_id": 6461,
        },
        {
            "title": "Präsentation Motivation",
            "type": "accordion_section_prezi",
            "external_link": "https://prezi.com/p/embed/xgljebkdhlu5/",
            "text": '<div style="padding-left: 0px;"><div><h2 style="font-size: 24px; line-height: 30px; margin-bottom: 0px;">Mitschüler_innen </h2><h2 style="font-size: 24px; line-height: 30px; margin-bottom: 0px;">motivieren</h2></div><div><h3 style="font-size: 18px; line-height: 24px; margin-bottom: 6px;"></h3><h3 style="font-size: 18px; line-height: 24px; margin-bottom: 6px;"></h3><h3 style="font-size: 18px; line-height: 24px; margin-bottom: 6px;"></h3></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Mit diesen Schritten sorgt ihr für mehr Motivation!</p></div><div><h3 style="font-size: 18px; line-height: 24px; margin-bottom: 6px;"></h3><h3 style="font-size: 18px; line-height: 24px; margin-bottom: 6px;"></h3><h3 style="font-size: 18px; line-height: 24px; margin-bottom: 6px;">kreative Wege finden</h3></div></div><div><div style="padding-left: 24px;"><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">So kann eure SV bekannter werden</p></div><div><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;">- den Klassenrat nutzen</h4></div><div><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;">- Vollversammlungen machen</h4></div><div><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;">- SV-Zeitung oder Jahreschronik</h4></div><div><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;">die SV bekannt machen</h4></div><div><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;">- interessante Kampagnen machen     </h4><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;">   (z.B. zu den Schülerrechten)</h4></div><div><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;"> - SV-Pinnwand oder Schaukasten</h4></div></div></div><div><div style="padding-left: 24px;"><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Mehr </p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Schul-Partys</p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Angst </p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">vor Mobbing</p></div><div><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;">Findet heraus,  was eure Mitschüler_innen bewegt!</h4></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">ungerechte </p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Benotung</p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">- macht Umfragen: online oder offline</p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;"></p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">- fragt im Klassenrat nach Ideen und Problemen</p></div><div><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;">Umfragen machen</h4></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Eine  </p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Umwelt-AG!?</p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">- SV-Briefkasten oder Kontakt-Email o.ä.</p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">- Ideen-Plakate aushängen</p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">besseres WLAN</p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">- SV-Aktionstag veranstalten</p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Hier ein Erklärvideo zum Thema:</p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">https://www.youtube.com/watch?v=kN5laxRsB1Y</p></div></div></div><div><div style="padding-left: 24px;"><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">SV-Helfer_in </p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">anheuern</p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Wahl-Helfer &amp; </p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">-Helferinnen</p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">ausbilden</p></div><div><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;"></h4></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Vergebt kleine Aufgaben an Schüler_innen, die nicht </p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Teil des SV-Teams sind.</p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;"></p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Indem ihr kleine Möglichkeiten zum Mitmachen schafft, weckt ihr die Neugier auf mehr. </p></div><div><h4 style="font-size: 13px; line-height: 19px; margin-bottom: 6px;">Angebote zum Mitmachen</h4></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">Praktikum </p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">bei der SV</p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">anbieten</p></div></div></div><div><div style="padding-left: 24px;"><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">- in der Projektwoche SV-Projekt(e) anbieten und so Interessierte gewinnen</p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">- einen Schülerhaushalt einführen, bei dem alle über die Geldvergabe mitentscheiden dürfen</p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">https://schuelerinnen-haushalt.de/</p></div><div><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">- das Modellprojekt Aula ausprobieren, um  </p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">  Beteiligungsmöglichkeiten zu erhöhen</p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;"></p><p style="color: rgb(142, 147, 156); font-size: 13px; line-height: 19px; margin-bottom: 6px;">       https://aula-blog.website/</p></div></div></div>',
            "post_id": 6461,
        },
        {
            "title": "Video Meinungen anderer einbeziehen",
            "type": "accordion_section_text",
            "text": "Eine Schülervertretung sollte, wie es schon der Name sagt, die gesamte Schule mit ihren vielfältigen Meinungen vertreten. Ihr seid also dafür verantwortlich diese Meinungen von so vielen Schüler_innen wie möglich abzufragen. Dafür gibt es viele verschiedene Möglichkeiten. Werdet kreativ!",
            "post_id": 6461,
        },
        {
            "title": "Meinungen aller Schüler:innen einbeziehen",
            "type": "accordion_section_youtube",
            "external_link": "https://www.youtube.com/embed/kN5laxRsB1Y?feature=oembed",
            "post_id": 6461,
        },
        {
            "title": "Checkliste Umfragen an der Schule durchführen",
            "type": "accordion_section_text",
            "text": "Download",
            "post_id": 6461,
        },
        {
            "title": "Checkliste Umfragen an der Schule durchführen",
            "type": "accordion_section_image",
            "post_id": 6461,
            "external_link": "https://meinsvwissen.de/wp-content/uploads/2022/04/Checkliste-Umfragen.png",
        },
    ]

    result = get_result(post_id)
    assert result == post_gt


def test_post_scraping_just_text():
    # see also tests/test_related_post_scraping.py -> test_related_post_in_text
    post_id = 7022  # https://meinsvwissen.de/andere-schulformen/
    post_gt = [
        {
            "type": "plain_text",
            "text": '<div class="elementor-element elementor-element-b591acb elementor-widget elementor-widget-text-editor" data-element_type="widget" data-id="b591acb" data-widget_type="text-editor.default"><div class="elementor-widget-container"><p><span style="-webkit-text-size-adjust: 100%;">Wir arbeiten derzeit hauptsächlich an staatlichen weiterführenden Schulen. Hier beginnt eine erste Sammlung von Materialien für Förderschulen, Schulen in freier Trägerschaft oder Berufs- und Fachschulen. <br/><a href="https://meinsvwissen.de/feedback/">Ladet gern euer Material hoch!</a></span></p><p><a href="https://meinsvwissen.de/grundschulen/">Hier geht es zum <strong>Bereich für Grundschulen</strong>.</a></p> </div></div>',
            "post_id": 7022,
        }
    ]

    result = get_result(post_id)
    print(result)
    assert result == post_gt
