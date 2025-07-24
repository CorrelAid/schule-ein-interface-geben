from lib.custom_scraping_sources import get_terms
import dspy
import os
from lib.config import llm_base_url, llm_model


def test_parse_glossary():
    lm = dspy.LM(
        model=f"openai/{llm_model}",
        model_type="chat",
        temperature=0.3,
        api_key=os.getenv("OR_KEY"),
        base_url=llm_base_url,
        cache=False,
    )
    term_df = get_terms(True, 5, 10, lm)

    dicts = term_df.to_dicts()

    assert dicts[0] == {
        "term": "Antrag",
        "definition": "Ein Antrag ist ein Vorschlag, den ihr einer Konferenz (z.B. Schulkonferenz) zur Diskussion und Abstimmung vorlegen könnt. Es ist ein wichtiges und hilfreiches Mittel für größere Entscheidungen in der Schule.",
        "DE": None,
        "DE_BW": None,
        "DE_BY": None,
        "DE_BE": None,
        "DE_BB": None,
        "DE_HB": None,
        "DE_HH": None,
        "DE_HE": None,
        "DE_MV": None,
        "DE_NI": None,
        "DE_NW": None,
        "DE_RP": None,
        "DE_SL": None,
        "DE_SN": None,
        "DE_ST": None,
        "DE_SH": None,
        "DE_TH": None,
    }

    assert dicts[3] == {
        "term": "Bezirks-/ Kreis & Landesschülervertretung",
        "definition": None,
        "DE": "BSK (Bundesschülerkonferenz – gewählte Mitglieder der Landesvertretungen auf Bundesebene)",
        "DE_BW": None,
        "DE_BY": None,
        "DE_BE": "BSA und LSA (Bezirksschüler- und Landesschülerausschuss)",
        "DE_BB": "KSR und LSR (Kreisschüler- und Landesschülerrat)",
        "DE_HB": None,
        "DE_HH": None,
        "DE_HE": None,
        "DE_MV": "KSR und LSR (Kreisschüler- und Landesschülerrat)",
        "DE_NI": None,
        "DE_NW": "BSV und LSV (Bezirksschüler- und Landesschülervertretung)",
        "DE_RP": None,
        "DE_SL": None,
        "DE_SN": None,
        "DE_ST": "KSR und LSR (Kreisschüler- und Landesschülerrat)",
        "DE_SH": None,
        "DE_TH": "Kreisschülersprecher:innen und LSV (Landesschülervertretung)",
    }
