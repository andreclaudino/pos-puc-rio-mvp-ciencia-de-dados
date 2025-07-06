from typing import Optional
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import DataFrame


def create_bar_chart(
    data_frame: DataFrame,
    category_column_name: str,
    count_column_name: str = "count",
    category_label: str = "",
    count_label: str = "Total de registros",
    title: str = "",
    height: int = 4,
    width: int = 6,
    log_scale = False,
    use_legends: bool = False
):
    # Coleta os dados do DataFrame do Spark
    data_pd = data_frame.select(category_column_name, count_column_name).toPandas()

    fig, ax = plt.subplots(figsize=(width, height))

    if use_legends:
        # Gráfico de barras laterais (side-by-side), cada categoria é uma barra separada
        palette = sns.color_palette("husl", len(data_pd[category_column_name].unique()))
        x = range(len(data_pd))
        bars = ax.bar(
            x=x,
            height=data_pd[count_column_name],
            color=palette,
            label=data_pd[category_column_name]
        )
        ax.set_xticks([])
        ax.set_xticklabels([])
        
        # Adiciona legendas
        handles = []
        labels = []
        for i, cat in enumerate(data_pd[category_column_name]):
            handles.append(bars[i])
            labels.append(str(cat))
        ax.legend(handles, labels, title=category_label if category_label else category_column_name, bbox_to_anchor=(1.05, 1), loc='upper left')
    else:
        sns.barplot(
            data=data_pd,
            x=category_column_name,
            y=count_column_name,
            ax=ax,
            color="skyblue"
        )
        ax.set_xlabel(category_label if category_label else category_column_name)

    ax.set_ylabel(count_label)
    ax.set_title(title)
    ax.grid(True, color='gray', linestyle='--', linewidth=0.5, axis='y')
    if log_scale:
        ax.set_yscale('log')
    plt.tight_layout()
    return fig

def create_pie_chart(
    data_frame: DataFrame,
    category_column_name: str,
    count_column_name: str = "count",
    title: str = "",
    height: int = 6,
    width: int = 8,
    startangle: int = 90,
    autopct: str = '%1.1f%%',
    pctdistance: float = 0.85,
    labeldistance: float = 1.1,
    include_value_labels = True,
):
    # Coleta os dados do DataFrame do Spark e ordena do menor para o maior
    data_pd = data_frame.select(category_column_name, count_column_name).toPandas()
    data_pd = data_pd.sort_values(by=count_column_name, ascending=True)
    
    # Calcula os percentuais
    total = data_pd[count_column_name].sum()
    percentages = (data_pd[count_column_name] / total * 100).round(1)
    
    # Cria labels combinando valores absolutos e percentuais
    if include_value_labels:
        labels = [f'{cat}\n({val:})' 
                for cat, val, pct in zip(data_pd[category_column_name], 
                                        data_pd[count_column_name], 
                                        percentages)]
    else:
        labels = [f'{cat}' 
                for cat, val, pct in zip(data_pd[category_column_name], 
                                        data_pd[count_column_name], 
                                        percentages)]
    
    # Configuração do gráfico
    fig, ax = plt.subplots(figsize=(width, height))
    
    # Paleta de cores
    palette = sns.color_palette("husl", len(data_pd))
    
    # Cria o gráfico de pizza
    wedges, texts, autotexts = ax.pie( # type: ignore
        data_pd[count_column_name],
        labels=labels,
        autopct=autopct,
        startangle=startangle,
        colors=palette,
        pctdistance=pctdistance,
        labeldistance=labeldistance,
        wedgeprops={'linewidth': 1, 'edgecolor': 'white'},
        textprops={'fontsize': 10}
    )
    
    # Ajusta o layout dos textos percentuais
    plt.setp(autotexts, size=10, weight="bold", color="white")
    
    # Adiciona título
    ax.set_title(title, pad=20, fontsize=14, fontweight='bold')
    
    # Centraliza o gráfico
    ax.axis('equal')
    
    plt.tight_layout()

    return fig

def create_line_chart(
    data_frame: DataFrame,
    x_column_name: str,
    y_column_name: str,
    title: str = "",
    height: int = 4,
    width: int = 6,
    log_scale: bool = False,
    x_label: Optional[str] = None,
    y_label: Optional[str] = None,
    linewidth: int = 2
):
    data_pd = data_frame.select(x_column_name, y_column_name).toPandas()

    fig, ax = plt.subplots(figsize=(width, height))

    # Linha
    ax.plot(
        data_pd[x_column_name],
        data_pd[y_column_name],
        color="tab:blue",
        linewidth=linewidth
    )

    # Pontos
    ax.scatter(
        data_pd[x_column_name],
        data_pd[y_column_name],
        color="tab:blue",
        edgecolor="tab:blue",
        alpha=0.5,
        s=50,
        zorder=3
    )

    ax.set_title(title)
    ax.set_xlabel(x_label if x_label is not None else x_column_name)
    ax.set_ylabel(y_label if y_label is not None else y_column_name)
    ax.grid(True, color='skyblue', linestyle='--', linewidth=0.5, axis='both')
    if log_scale:
        ax.set_yscale('log')
    plt.tight_layout()
    return fig

def create_cumulative_chart(
    data_frame: DataFrame,
    category_column_name: str,
    value_column_name: str,
    category_label: str = "",
    values_label: str = "",
    title: str = "",
    height: int = 4,
    width: int = 6,
    use_legends: bool = False,
    sum_100: bool = False
):
    # Coleta os dados do DataFrame do Spark
    data_pd = data_frame.select(category_column_name, value_column_name).toPandas()
    # Ordena pelo valor
    data_pd = data_pd.sort_values(by=value_column_name, ascending=True).reset_index(drop=True)
    # Calcula o acumulado
    data_pd['cumulative'] = data_pd[value_column_name].cumsum()
    if sum_100:
        total = data_pd[value_column_name].sum()
        data_pd['cumulative'] = 100 * data_pd['cumulative'] / total
        values_label = values_label or "Percentual acumulado"
    else:
        values_label = values_label or "Valor acumulado"

    fig, ax = plt.subplots(figsize=(width, height))

    if use_legends:
        palette = sns.color_palette("husl", len(data_pd[category_column_name].unique()))
        x = range(len(data_pd))
        bars = ax.bar(
            x=x,
            height=data_pd['cumulative'],
            color=palette,
            label=data_pd[category_column_name]
        )
        ax.set_xticks([])
        ax.set_xticklabels([])
        handles = []
        labels = []
        for i, cat in enumerate(data_pd[category_column_name]):
            handles.append(bars[i])
            labels.append(str(cat))
        ax.legend(handles, labels, title=category_label if category_label else category_column_name, bbox_to_anchor=(1.05, 1), loc='upper left')
    else:
        sns.barplot(
            data=data_pd,
            x=category_column_name,
            y='cumulative',
            ax=ax,
            color="skyblue"
        )
        ax.set_xlabel(category_label if category_label else category_column_name, rotation = 45, ha = 'right')

    ax.set_ylabel(values_label)
    ax.set_title(title)
    ax.grid(True, color='gray', linestyle='--', linewidth=0.5, axis='y')
    plt.tight_layout()
    return fig


def create_histogram(
    data_frame: DataFrame,
    target_column_name: str,
    bins: Optional[int] = None,
    bin_size: Optional[int] = None,
    title: Optional[str] = None,
    height: int = 4,
    width: int = 6,
    x_label: Optional[str] = None,
    y_label: str = "Frequência",
    log_scale: bool = False,
    stat = 'count'
):
    # Coleta os dados do DataFrame do Spark
    data_pd = data_frame.select(target_column_name).toPandas()
    fig, ax = plt.subplots(figsize=(width, height))

    palette = sns.color_palette("husl", len(data_pd[target_column_name].unique()))

    function_bins = bins if bins else "auto"
    sns.histplot(data_pd[target_column_name], kde=True, bins=function_bins, binwidth=bin_size, palette=palette, stat=stat) # type: ignore
        
    funcion_x_label = x_label if x_label else target_column_name
    ax.set_xlabel(funcion_x_label)
    ax.set_ylabel(y_label)

    if not title:
        title = f"Distribuição de valores para '{funcion_x_label}'"
    # Aplica escala logarítmica se necessário
    if log_scale:
        ax.set_yscale('log')

    ax.set_title(title)
    plt.tight_layout()

    return fig

def create_box_plot(
    data_frame: DataFrame,
    column_name: str,
    categories_column_name: Optional[str] = None,
    title: str = "",
    width: int = 6,
    height: int = 4,
    log_scale: bool = False,
    x_label: Optional[str] = None,
    y_label: str = "Valor",
):
    fig, ax = plt.subplots(figsize=(width, height))

    if categories_column_name:
        # Coleta os dados do DataFrame do Spark
        data_pd = data_frame.select(column_name, categories_column_name).toPandas()
        
        # Cria o gráfico de caixa (box plot)
        sns.boxplot(
            x=data_pd[categories_column_name],
            y=data_pd[column_name],
            ax=ax
        )
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
    else:
        # Coleta os dados do DataFrame do Spark
        data_pd = data_frame.select(column_name).toPandas()
        
        # Cria o gráfico de caixa (box plot)
        sns.boxplot(y=data_pd[column_name], ax=ax)

    # Configurações de rótulos e título
    
    ax.set_xlabel(x_label if x_label else column_name)
    ax.set_ylabel(y_label)
    
    ax.set_title(title)
    
    # Adiciona grid
    ax.grid(True, color='gray', linestyle='--', linewidth=0.5, axis='both')
    
    # Aplica escala logarítmica se necessário
    if log_scale:
        ax.set_yscale('log')
    
    plt.tight_layout()
    return fig

