from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum


def calcula_patrimonio(
    bens_de_candidatos: DataFrame,
    codigo_candidato_column: str,
    codigo_eleicao_column: str,
    valor_do_bem_column: str,
) -> DataFrame:
    """
    Calcula o total de bens para cada candidato.
    Args:
        bens_de_candidatos (DataFrame): DataFrame contendo os bens declarados pelos candiatos
    Returns:
        DataFrame: DataFrame contendo o total de bens para cada candidato, agrupado por SQ_CANDIDATO
    """
    total_de_bens_do_candidato = \
        bens_de_candidatos\
            .groupby(col(codigo_candidato_column), col(codigo_eleicao_column))\
            .agg(
                sum(col(valor_do_bem_column)).alias("patrimonio")
            )

    return total_de_bens_do_candidato
