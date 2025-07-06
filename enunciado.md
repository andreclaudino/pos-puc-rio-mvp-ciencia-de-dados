Requisitos para o Desenvolvimento do MVP
Contexto: 
Escolha uma base de dados que não tenha sido utilizada em aula. Sugere-se usar uma das bases de dados disponibilizadas em algum dos repositórios a seguir:
UCI Machine Learning Repository: 
https://archive.ics.uci.edu/ml/datasets

Kaggle: https://www.kaggle.com/datasets.

Google Datasets: https://datasetsearch.research.google.com/. 

       Aproveite os filtros que os repositórios oferecem para encontrar com mais facilidade o dataset da sua preferência. Caso você prefira usar um dataset que reflita um problema real da sua empresa (cuidado apenas com a confidencialidade dos dados), será muito bem-vindo. 

Você deverá trabalhar desde a definição do problema até a etapa de pré-processamento de dados, conforme esquema visto na disciplina Análise exploratória e pré-processamento de dados. 
Produza um notebook no Google Colab, considerando os itens 1 e 2, com as características a seguir:
O notebook servirá como relatório, descrevendo textualmente (utilizando as células de texto) o contexto do problema e as operações com os dados (veja a checklist sugerida abaixo).
Utilize a linguagem Python e bibliotecas que considera apropriadas para abordar o problema.
Crie o notebook seguindo as boas práticas de codificação vistas no curso. 
O notebook deve seguir a estrutura proposta neste 
template
 (lembre-se de copiar pro seu Drive e editar, este link leva para uma versão de leitura apenas). 

Observações: 
O dataset pode ser qualquer um a sua escolha, desde que não sejam os datasets vistos na disciplina Análise exploratória e pré-processamento de Dados
Durante a sua análise exploratória, no momento de utilizar gráficos, podem ser utilizadas as bibliotecas Python vistas na disciplina Análise exploratória e pré-processamento de dados, as ferramentas vistas na disciplina Visualização de informaçãoou outras a sua escolha. Se forem utilizadas outras ferramentas para a construção dos gráficos, que não as bibliotecas Python, você deverá adicionar no notebook uma figura com cada gráfico produzido.

Requisitos e composição da nota: 
(1,0 pt) Execução sem erros:o notebook deve poder ser executado pelo professor do início ao fim sem erros no Google Colab.
(2,0 pts) Documentação consistente:utilize blocos de texto que expliquem textualmente cada etapa e cada decisão do seu código, contando uma história completa e compreensível, do início ao fim.
(1,0 pt) Código limpo:seu código deve estar legível e organizado. Devem ser utilizadas as boas práticas de codificação vistas nas disciplinas Programação orientada a objetose Engenharia de software para ciência de dados, mas não é necessário que você crie classes no seu código.
(2,0 pts) Análise de dados:após cada gráfico, você deverá escrever um parágrafo resumindo os principais achados, analisando os resultados e levantando eventuais pontos de atenção.
(2,0 pts) Checklist:você deverá responder às perguntas (aplicáveis ao seu dataset) da checklist fornecida, utilizando-a como guia para o desenvolvimento do trabalho.
(2,0 pts) Capricho e qualidade do trabalho como um todo. 

Checklist sugerida:
Definição do problema 
Objetivo: entender e descrever claramente o problema que está sendo resolvido. 

Qual é a descrição do problema?
Este é um problema de aprendizado supervisionado ou não supervisionado?
Que premissas ou hipóteses você tem sobre o problema?
Que restrições ou condições foram impostas para selecionar os dados?
Defina cada um dos atributos do dataset. 

Análise de dados 
Objetivo: entender a informação disponível. 

Estatísticas descritivas: 

Quantos atributos e instâncias existem?
Quais são os tipos de dados dos atributos?
Verifique as primeiras linhas do dataset. Algo chama a atenção?
Há valores faltantes, discrepantes ou inconsistentes?
Faça um resumo estatístico dos atributos com valor numérico (mínimo, máximo, mediana, moda, média, desvio-padrão e número de valores ausentes). O que você percebe?
Visualizações: 

Verifique a distribuição de cada atributo. O que você percebe? Dica: esta etapa pode dar ideias sobre a necessidade de transformações na etapa de preparação de dados (por exemplo, converter atributos de um tipo para outro, realizar operações de discretização, normalização, padronização, etc.).
Se for um problema de classificação, verifique a distribuição de frequência das classes. O que você percebe? Dica: esta etapa pode indicar a possível necessidade futura de balanceamento de classes.
Analise os atributos individualmente ou de forma combinada, usando os gráficos mais apropriados. 

Pré-processamento de dados: 
Objetivo: realizar operações de limpeza, tratamento e preparação dos dados. 

Verifique quais operações de pré-processamento podem ser interessantes para o seu problema e salve visões diferentes do seu dataset (por exemplo, normalização, padronização, discretização e one-hot-encoding).
Trate (removendo ou substituindo) os valores faltantes (se existentes).
Realize outras transformações de dados porventura necessárias.
Explique, passo a passo, as operações realizadas, justificando cada uma delas.
Se julgar necessário, utilizando os dados pré-processados, volte na etapa de análise exploratória e verifique se surge algum insight diferente após as operações realizadas.

Sobre a entrega: 
Você deverá disponibilizar seu notebook com o código em Python e eventuais arquivos com os datasets necessários para a execução do seu código em um repositório público

do GitHub. A utilização do dataset dentro do notebook deve ser feita pelo URL do  seu repositório do GitHub (veja o exemplo abaixo). O caminho do notebook deve ser informado na tarefa de entrega do MVP, no ambiente do curso. 

Se tiver dúvidas sobre como criar um repositório público no GitHub, consulte: https://docs.github.com/pt/repositories/creating-and-managing-repositories/creating-a-n ew-repository. 

Exemplo de como ler o dataset a partir do seu repositório do GitHub: colocar o arquivo em um repositório seu do GitHub e referenciá-lo no URL com a sua versão raw. Por exemplo: https://raw.githubusercontent.com/tatianaesc/datascience/main/diabetes.csv. 


Datas importantes: 
* 04/06: Orientação inicial do MVP
* 11/06: Sessão de dúvidas do MVP
* 18/06: Sessão de dúvidas do MVP
* 25/06: Sessão de dúvidas do MVP
* 02/07: Sessão de dúvidas do MVP
* 06/07: Entrega final do MVP
* 21/07: Resultado final do MVP