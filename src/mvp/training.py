from os import pipe
from typing import Tuple
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import rand, col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler


def separa_treino_e_teste(data_frame: DataFrame, test_sample_ratio: float, seed=None) -> Tuple[DataFrame, DataFrame]:
    randomized = \
        data_frame\
            .withColumn("peusdo_random_train_test_split", rand(seed=seed))

    train = randomized.filter(col("peusdo_random_train_test_split") > test_sample_ratio)
    test = randomized.filter(col("peusdo_random_train_test_split") <= test_sample_ratio)

    return train, test


def preapre_feature_engineering_pipeline(data_frame: DataFrame) -> PipelineModel:
    genero_indexer = StringIndexer(inputCol="genero", outputCol="genero_encoded")
    grau_instrucao_indexer = StringIndexer(inputCol="grau_instrucao", outputCol="grau_instrucao_encoded")
    estado_civil_indexer = StringIndexer(inputCol="estado_civil", outputCol="estado_civil_encoded")
    cor_raca_indexer = StringIndexer(inputCol="cor_raca", outputCol="cor_raca_encoded")
    segmento_idade_indexer = StringIndexer(inputCol="segmento_idade", outputCol="segmento_idade_encoded")
    busca_reeleicao_indexer = StringIndexer(inputCol="busca_reeleicao", outputCol="busca_reeleicao_encoded")

    encoder = OneHotEncoder(inputCols=["genero_encoded", "grau_instrucao_encoded", "estado_civil_encoded", "cor_raca_encoded", "segmento_idade_encoded", "busca_reeleicao_encoded"],
                            outputCols=["genero_vector", "grau_instrucao_vector", "estado_civil_vector", "cor_raca_vector", "segmento_idade_vector", "busca_reeleicao_vector"])

    vector_assembler = VectorAssembler(inputCols=["genero_vector", "grau_instrucao_vector", "estado_civil_vector", "cor_raca_vector", "segmento_idade_vector", "busca_reeleicao_vector", "vagas", "patrimonio"], outputCol="features")
    scaler = MinMaxScaler(inputCol="features", outputCol="features_scaled")

    pipeline = Pipeline(stages=[
        genero_indexer, grau_instrucao_indexer, estado_civil_indexer, cor_raca_indexer, segmento_idade_indexer, busca_reeleicao_indexer,
        encoder,
        vector_assembler,
        scaler,
    ])

    pipeline_model = pipeline.fit(data_frame)

    return pipeline_model