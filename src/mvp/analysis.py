from pyspark.sql.window import Window
from pyspark.sql import functions as F

from pyspark.sql import DataFrame

def count_column_occurencies(data_frame: DataFrame, column_name: str) -> DataFrame:
    processed = data_frame\
        .select(column_name)\
        .groupBy(column_name)\
        .count()\
        .orderBy(column_name)
    
    return processed


def measure_column_value_proportions(data_frame: DataFrame, column_name: str) -> DataFrame:
    total_count = data_frame.count()
    processed = count_column_occurencies(data_frame, column_name)\
        .withColumn("proportion", (F.col("count") / total_count))\
        .drop("count")\
        .orderBy(column_name)
    
    return processed


def calculate_cumulative_proportion(
        data_frame: DataFrame,
        category_column_name: str,
        value_column_name: str
):
    """
    Calcula a proporção acumulada das categorias ordenadas por valor.
    
    Args:
        df: DataFrame Spark
        category_column_name: Nome da coluna que contém as categorias
        value_column_name: Nome da coluna que contém os valores numéricos para ordenação
        
    Returns:
        DataFrame Spark com as colunas:
        - category_column_name: Categorias ordenadas
        - value_column_name: Valores originais
        - proportion: Proporção de cada categoria
        - cumulative_proportion: Proporção acumulada
    """
    
    # Calcula o total para normalização
    total = data_frame.agg(F.sum(value_column_name).alias('total')).collect()[0]['total']
    
    # Ordena as categorias pelo valor (decrescente)
    window_spec = Window.orderBy(F.col(value_column_name).desc())
    
    result_df = (data_frame
                .orderBy(F.col(value_column_name).desc())
                .withColumn("proportion", F.col(value_column_name) / total)
                .withColumn("cumulative_proportion", 
                           F.sum("proportion").over(window_spec.rowsBetween(Window.unboundedPreceding, 0)))
                .select(
                    category_column_name,
                    value_column_name,
                    "proportion",
                    "cumulative_proportion"
                ))
    
    return result_df