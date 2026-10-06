"""Fábrica de arquivos de teste no formato do FFIEC (mesmo cabeçalho de 99 colunas)."""

import hashlib
import zipfile
from pathlib import Path

from hmda.esquema import COLUNAS_LAR

CABECALHO = ",".join(COLUNAS_LAR)

# Um pedido "normal": compra originada, convencional, 1ª hipoteca, moradia própria.
PADRAO = {c: "NA" for c in COLUNAS_LAR} | {
    "activity_year": "2021",
    "lei": "5493001KJTIIGC8Y1R12",
    "state_code": "DE",
    "county_code": "10003",
    "derived_msa_md": "48864",
    "census_tract": "10003010100",
    "action_taken": "1",
    "loan_purpose": "1",
    "loan_type": "1",
    "lien_status": "1",
    "occupancy_type": "1",
    "construction_method": "1",
    "preapproval": "2",
    "purchaser_type": "0",
    "hoepa_status": "2",
    "total_units": "1",
    "conforming_loan_limit": "C",
    "reverse_mortgage": "2",
    "open_end_line_of_credit": "2",
    "business_or_commercial_purpose": "2",
    "negative_amortization": "2",
    "interest_only_payment": "2",
    "balloon_payment": "2",
    "other_nonamortizing_features": "2",
    "loan_amount": "205000.0",
    "property_value": "255000",
    "income": "85",
    "combined_loan_to_value_ratio": "80.0",
    "interest_rate": "3.125",
    "loan_term": "360",
    "debt_to_income_ratio": "20%-<30%",
    "applicant_age": "35-44",
    "applicant_sex": "1",
    "co_applicant_sex": "5",
    "applicant_credit_score_type": "1",
    "denial_reason_1": "10",
    "derived_race": "White",
    "derived_ethnicity": "Not Hispanic or Latino",
    "derived_sex": "Male",
    "tract_population": "4000",
    "tract_minority_population_percent": "30.5",
    "ffiec_msa_md_median_family_income": "90000",
    "tract_to_msa_income_percentage": "110",
}


def linha(**valores: str) -> str:
    base = PADRAO | valores
    return ",".join(base[c] for c in COLUNAS_LAR)


def montar_zip(
    pasta: Path,
    linhas: list[str],
    nome: str = "lar.zip",
    cabecalho: str = CABECALHO,
    lixo_macos: bool = True,
    final_sem_quebra: bool = False,
) -> tuple[Path, str]:
    corpo = cabecalho + "\n" + "\n".join(linhas) + ("" if final_sem_quebra else "\n")
    caminho = pasta / nome
    with zipfile.ZipFile(caminho, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("2021_public_lar_csv.csv", corpo)
        if lixo_macos:
            z.writestr("__MACOSX/._2021_public_lar_csv.csv", b"\x00\x05\x16\x07lixo")
    return caminho, hashlib.sha256(caminho.read_bytes()).hexdigest()
