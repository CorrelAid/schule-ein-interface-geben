from lib.models import TermSchema
from typing import Union
import dspy

gp_examples = [
    dspy.Example(
        raw_text="""
                 Deutschland: BSK (Bundesschülerkonferenz – gewählte Mitglieder der Landesvertretungen auf Bundesebene)

Berlin: BSA und LSA (Bezirksschüler- und Landesschülerausschuss)

NRW:  BSV und LSV (Bezirksschüler- und Landesschülervertretung""",
        input_term="Bezirks-/ Kreis & Landesschülervertretung",
        term="Bezirks-/ Kreis & Landesschülervertretung",
        definition=None,
        DE_BE="BSA und LSA (Bezirksschüler- und Landesschülerausschuss)",
        DE_NW="BSV und LSV (Bezirksschüler- und Landesschülervertretung",
        DE="BSK (Bundesschülerkonferenz – gewählte Mitglieder der Landesvertretungen auf Bundesebene)",
    ).with_inputs("raw_text", "input_term"),
    dspy.Example(
        raw_text="""BSK = Bundesschülerkonferenz. Sie besteht aus den gewählten Mitgliedern von Landesschülervertretungen aus den einzelnen Bundesländern. Die BSK ist sozusagen die Lobby für Schülerinteressen auf Bundesebene.""",
        term="Bezirks-/ Kreis & Landesschülervertretung",
        definition=None,
        DE="Bundesschülerkonferenz. Sie besteht aus den gewählten Mitgliedern von Landesschülervertretungen aus den einzelnen Bundesländern. Die BSK ist sozusagen die Lobby für Schülerinteressen auf Bundesebene.",
    ).with_inputs("raw_text", "input_term"),
    dspy.Example(
        raw_text="""BSK = Bundesschülerkonferenz. Sie besteht aus den gewählten Mitgliedern von Landesschülervertretungen aus den einzelnen Bundesländern. Die BSK ist sozusagen die Lobby für Schülerinteressen auf Bundesebene.""",
        term="Aufgabenprofil",
        definition="Ihr schreibt transparent und klar verständlich auf, wer für welche Aufgaben zuständig ist. Das Profil ist z.B. eine Übersicht über die Aufgaben der Mitglieder im SV-Team, eine Vereinbarung über die Aufgaben von Klassensprecher:innen oder eine Beschreibung der Rolle von SV-Begleiter:innen (weitere Beispiele).",
        DE=None,
    ).with_inputs("raw_text", "input_term")
]


def make_termparser():
    class TermParser(dspy.Signature):
        "Parses a glossary entry to seperate information by jurisdiction without changing the text apart from excluding parts that belong in other fields or field names."

        term_input: str = dspy.InputField(desc="Glossary entry term.")
        valid_jurisdictions: list[dict] = dspy.InputField(
            desc="Valid jurisdictions for which extra information can exist"
        )
        raw_text: str = dspy.InputField(
            desc="All available text for the glossary entry"
        )
    
    schema = TermSchema.to_polars_schema().to_python()
    for field in schema.keys():
        TermParser = TermParser.append(
            name=field,
            field=dspy.OutputField(),
            type_=Union[str,None]
        )

    teleprompter = dspy.LabeledFewShot()
    term_parser = teleprompter.compile(
        student=dspy.Predict(TermParser), trainset=gp_examples
    )

    return term_parser
