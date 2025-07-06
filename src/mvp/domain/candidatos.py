from typing import Optional
from datetime import datetime
from pyspark.sql.types import StringType, BooleanType, IntegerType
from pyspark.sql.functions import udf


def mescla_estados_civis(estado_civil: str) -> str:
    match estado_civil:
        case "SEPARADO(A) JUDICIALMENTE" | "DIVORCIADO(A)":
            return "SEPARADO"
        case outro:
            return outro
mescla_estados_civis_udf = udf(mescla_estados_civis, StringType())


def mescla_ocupação(ocupacao: str) -> bool:
    match ocupacao:
        case "VEREADOR":
            return True
        case _:
            return False # type: ignore
mescla_ocupação_udf = udf(mescla_ocupação, BooleanType())

def mescla_situacao_no_turno(situacao_no_turno: str) -> Optional[bool]:
    match situacao_no_turno:
        case "ELEITO POR MÉDIA" | "ELEITO POR QP":
            return True
        case "NÃO ELEITO" | "SUPLENTE":
            return False
        case _:
            return None


mescla_situacao_no_turno_udf = udf(mescla_situacao_no_turno, BooleanType())

def calcula_idade_no_ano(data_nascimento: str, ano_referencia: int) -> int:
    data_nascimento_parsed = datetime.strptime(data_nascimento, "%d/%m/%Y")
    ano_nascimento = data_nascimento_parsed.year
    idade = ano_referencia - ano_nascimento + 1
    return idade
calcula_idade_no_ano_udf = udf(calcula_idade_no_ano, IntegerType())


def segmenta_idade(idade: int) -> Optional[str]:
    """
    Segmenta a idade em buckets de 10 anos
    """
    if idade < 18:
        # inválido, já que o candidato precisa ter no mínimo 18 anos, ou seja, o primeiro bucket seria 1
        return None
    bucket_id = idade // 10

    match bucket_id:
        case 1:
            return "18 a 20"
        case 2:
            return "20 a 30"
        case 3:
            return "30 a 40"
        case 4:
            return "40 a 50"
        case 5:
            return "50 a 60"
        case 6:
            return "60 a 70"
        case 7:
            return "70 a 80"
        case 8:
            return "80 a 90"
        case _:
            return "acima de 90"
segmenta_idade_udf = udf(segmenta_idade, StringType())
        