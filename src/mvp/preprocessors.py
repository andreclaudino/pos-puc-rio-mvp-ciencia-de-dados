from threading import local
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col, percentile, median, stddev, lit, sum, when
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType
from pyspark.sql import SparkSession


def remove_columns(data_frame: DataFrame, column_names: list[str]) -> DataFrame:
    """
    Drop specified columns from a DataFrame.

    Args:
        data_frame (DataFrame): The DataFrame from which to drop columns.
        columns (list[str]): List of column names to drop.

    Returns:
        DataFrame: A new DataFrame with the specified columns dropped.
    """
    return data_frame.drop(*column_names)


def rename_columns(data_frame: DataFrame, column_mapping: dict[str, str]) -> DataFrame:
    """
    Rename specified columns in a DataFrame.

    Args:
        data_frame (DataFrame): The DataFrame in which to rename columns.
        column_mapping (dict[str, str]): A dictionary mapping old column names to new column names.

    Returns:
        DataFrame: A new DataFrame with the specified columns renamed.
    """
    for old_name, new_name in column_mapping.items():
        data_frame = data_frame.withColumnRenamed(old_name, new_name)
    return data_frame





def sort_by_categorical_field(
        spark: SparkSession,
        data_frame: DataFrame,
        sorted_values: List[str],
        value_column_name: str,
        order_column_name: str = "column_order",
        column_data_type = StringType(),
        ascending = True
    ) -> DataFrame:
    
    sorted_content = [(index, value) for index, value in enumerate(sorted_values)]
    schema = StructType([
        StructField(order_column_name, IntegerType()),
        StructField(value_column_name, column_data_type)
    ])

    sort_reference_data_frame = \
        spark.createDataFrame(sorted_content, schema)
    
    joint_dataset = sort_reference_data_frame\
        .join(data_frame, on=[value_column_name])\
        .orderBy(order_column_name, ascending=ascending)\
        .drop(col(order_column_name))
    
    return joint_dataset

    
def parse_text_to_float(valor_numerico_em_texto: Optional[str]) -> float:
    if not valor_numerico_em_texto:
        return 0.0
    valor_numerico_em_texto = valor_numerico_em_texto.replace(",", ".")
    valor_numerico = float(valor_numerico_em_texto)
    return valor_numerico
parse_text_to_float_udf = udf(parse_text_to_float, FloatType())


def calculate_dispersion(
    data_frame: DataFrame,
    target_column: str,
    include_target_column_on_column_names: bool = False,
    count_outliers: bool = False
) -> DataFrame:
    fake_group_column_name = f"__group_column_for_{target_column}"

    fake_data_frame = data_frame\
        .withColumn(fake_group_column_name, lit(fake_group_column_name))
    
    dispersion_data_frame = calculate_dispersion(
        fake_data_frame,
        fake_group_column_name,
        include_target_column_on_column_names,
        count_outliers
    )\
    .drop(fake_group_column_name)

    return dispersion_data_frame


def calculate_group_dispersion(
    data_frame: DataFrame,
    categories_column: str,
    target_column: str,
    include_target_column_on_column_names: bool = False,
    count_outliers: bool = False
) -> DataFrame:
    
    if include_target_column_on_column_names:
        column_name_prefix = f"{target_column}_"
    else:
        column_name_prefix = ""
    
    dispersion_data_frame = \
        data_frame\
            .groupBy(categories_column)\
            .agg(
                percentile(col(target_column), 0.25).alias(f"{column_name_prefix}q1"),
                median(col(target_column)).alias(f"{column_name_prefix}median"),
                percentile(col(target_column), 0.75).alias(f"{column_name_prefix}q3"),
                stddev(col(target_column)).alias(f"{column_name_prefix}desvio_padrao"),
            )\
            .withColumn(f"{column_name_prefix}iqr", col(f"{column_name_prefix}q3")-col(f"{column_name_prefix}q1"))\
            .withColumn(f"{column_name_prefix}lower_bound", col(f"{column_name_prefix}q1") - lit(1.5)*col(f"{column_name_prefix}iqr"))\
            .withColumn(f"{column_name_prefix}lower_bound", when(col(f"{column_name_prefix}lower_bound") < 0, 0).otherwise(col(f"{column_name_prefix}lower_bound")))\
            .withColumn(f"{column_name_prefix}upper_bound", col(f"{column_name_prefix}q3") + lit(1.5)*col(f"{column_name_prefix}iqr"))\
    
    bounds = dispersion_data_frame\
        .select(f"{column_name_prefix}lower_bound", f"{column_name_prefix}upper_bound", f"{column_name_prefix}iqr").first()\
        .asDict() # type: ignore
    
    lower = bounds[f"{column_name_prefix}lower_bound"]
    upper = bounds[f"{column_name_prefix}upper_bound"]

    if count_outliers:
        dispersion_data_frame = \
            data_frame\
                .groupBy(categories_column)\
                .agg(
                    sum(
                        when(col(target_column) < lower, lit(1))\
                            .otherwise(lit(0))).alias(f"{column_name_prefix}lower_outliers"),
                    sum(
                        when(col(target_column) > upper, lit(1))\
                            .otherwise(lit(0))).alias(f"{column_name_prefix}upper_outliers"),
                )\
            .join(dispersion_data_frame, on = categories_column, how = "inner")

    return dispersion_data_frame


def remove_group_outliers(
        data_frame: DataFrame,
        categories_column: str,
        target_column: str,
        include_target_column_on_column_names: bool = True,
) -> DataFrame:
    """
    Remove rows where the target_column values are outside the calculated bounds.

    Args:
        data_frame (DataFrame): The input DataFrame.
        categories_column (str): The column to group by for calculating bounds.
        target_column (str): The column to check for outliers.

    Returns:
        DataFrame: A new DataFrame with outliers removed.
    """
    # Calculate dispersion with target column as prefix
    dispersion_df = calculate_group_dispersion(
        data_frame,
        categories_column,
        target_column,
        include_target_column_on_column_names=include_target_column_on_column_names
    )
    
    data_frame_columns = data_frame.columns
    dispersion_df_columns = [column for column in dispersion_df.columns if column not in data_frame_columns]
    
    # Join the original data with the dispersion df on categories_column
    joined_df = data_frame.join(dispersion_df, on=categories_column, how='inner')
    
    # Filter rows where target_column is within the bounds
    filtered_df = joined_df.filter(
        (col(target_column) >= col(f"{target_column}_lower_bound")) &
        (col(target_column) <= col(f"{target_column}_upper_bound"))
    )\
    .drop(*dispersion_df_columns)
    
    return filtered_df


def replace_outliers(
        data_frame: DataFrame,
        target_column: str,
        use_median: bool = False,
        replace_upper: bool = True,
        replace_lower: bool = True
) -> DataFrame:
    fake_group_column_name = f"__group_column_for_{target_column}"

    fake_data_frame = data_frame\
        .withColumn(fake_group_column_name, lit(fake_group_column_name))
    
    dispersion_data_frame = replace_group_outliers(
        fake_data_frame,
        fake_group_column_name,
        target_column,
        use_median,
        replace_upper,
        replace_lower
    )\
    .drop(fake_group_column_name)

    return dispersion_data_frame

def replace_group_outliers(
        data_frame: DataFrame,
        categories_column: str,
        target_column: str,
        use_median: bool = False,
        replace_upper: bool = True,
        replace_lower: bool = True
) -> DataFrame:
    """
    Replace rows where the target_column values are outside the calculated bounds.

    Args:
        data_frame (DataFrame): The input DataFrame.
        categories_column (str): The column to group by for calculating bounds.
        target_column (str): The column to check for outliers.
        use_median (str): Should replace value by meadian
            * True: Replace by the median
            * False: replace by the closest bound (lower or upper)

    Returns:
        DataFrame: A new DataFrame with outliers removed.
    """
    # Calculate dispersion with target column as prefix
    dispersion_df = calculate_group_dispersion(
        data_frame,
        categories_column,
        target_column,
        include_target_column_on_column_names=True
    )

    dispersion_df_columns = dispersion_df.columns
    
    # Join the original data with the dispersion df on categories_column
    replaced_df = data_frame.join(dispersion_df, on=categories_column, how='inner')
    
    if replace_lower:
        replace_value = col(f"{target_column}_median") if use_median else col(f"{target_column}_lower_bound")
        
        replaced_df = replaced_df\
            .withColumn(
                target_column,
                when(col(target_column) <= col(f"{target_column}_lower_bound"), replace_value)
                    .otherwise(col(target_column))
            )
        
    if replace_upper:
        replace_value = col(f"{target_column}_median") if use_median else col(f"{target_column}_upper_bound")

        replaced_df = replaced_df\
            .withColumn(
                target_column,
                when(col(target_column) >= col(f"{target_column}_upper_bound"), replace_value)
                    .otherwise(col(target_column))
            )

    
    replaced_df = replaced_df.drop(*dispersion_df_columns)

    return replaced_df

