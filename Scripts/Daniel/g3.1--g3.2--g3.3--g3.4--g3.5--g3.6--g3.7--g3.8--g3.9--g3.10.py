import functions as c
import os
import pandas as pd
import numpy as np
import json
import traceback
import tempfile
import shutil
import sidrapy
import ipeadatapy
from datetime import datetime


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

# # contas da produção
# try:
#     year = datetime.now().year
#     while True:
#         # url da base contas regionais
#         url = f'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Contas_Regionais/{year}/xls'
#         response = c.open_url(url)
        
#         if response.status_code == 200:
#             content = pd.DataFrame(response.json())
#             link = content.query(
#                 'name.str.lower().str.startswith("conta_da_producao_2010") and name.str.lower().str.endswith(".zip")'
#             )['url'].values[0]
#             if link:
#                 response = c.open_url(link)
#                 c.to_file(dbs_path, 'ibge_conta_producao.zip', response.content)
#                 break
#         else:
#             if year > 2020:
#                 year -= 1
#             else:
#                 break

# except Exception as e:
#     errors[url + ' (Conta da Produção)'] = traceback.format_exc()


# # sidra 3939
# url = 'https://apisidra.ibge.gov.br/values/t/3939/n1/all/n2/2/n3/28/v/all/p/all/c79/2670,2672,2677,32793,32794,32796?formato=json'
# try:
#     data = c.open_url(url)
#     df = pd.DataFrame(data.json())
#     df = df[['D3N', 'D1N', 'D4N', 'V']].copy()
#     df.columns = ['Ano', 'Região', 'Variável', 'Valor']
#     df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
#     df[['Ano', 'Valor']] = df[['Ano', 'Valor']].astype(int)

#     c.to_excel(df, dbs_path, 'sidra_3939.xlsx')
# except Exception as e:
#     errors['Sidra 3939'] = traceback.format_exc()


# # sidra 74
# url = 'https://apisidra.ibge.gov.br/values/t/74/n1/all/n2/2/n3/28/v/106/p/all/c80/2682,2685,2687?formato=json'
# try:
#     data = c.open_url(url)
#     df = pd.DataFrame(data.json())
#     df = df[['D3N', 'D1N', 'D4N', 'MN', 'V']].copy()
#     df.columns = ['Ano', 'Região', 'Variável', 'Unidade', 'Valor']
#     df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
#     df[['Ano', 'Valor']] = df[['Ano', 'Valor']].astype(int)

#     c.to_excel(df, dbs_path, 'sidra_74.xlsx')
# except Exception as e:
#     errors['Sidra 74'] = traceback.format_exc()


# sidra 1092
url = 'https://apisidra.ibge.gov.br/values/t/1092/n1/all/n3/all/v/284,1000284/p/all/c12716/115236/c18/992/c12529/118225/d/v1000284%202?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'D2N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Variável', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Trimestre'] = df['Ano'].apply(lambda x: int(x[0]))  # extrai o trimestre da coluna Ano
    df['Ano'] = df['Ano'].str.split(' ').str[-1].astype(int)  # extrai o ano da coluna Ano
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df['Valor'] = df['Valor'].fillna(0)  # substitui valores nulos por 0
    df['Valor'] = df['Valor'].astype(int)

    c.to_excel(df, dbs_path, 'sidra_1092.xlsx')
except Exception as e:
    errors['Sidra 1092'] = traceback.format_exc()


# sidra 3939-2
url = 'https://apisidra.ibge.gov.br/values/t/3939/n1/all/n2/2/n3/11,12,13,14,15,16,21,22,23,24,17,25,26,27,28,29,31,32,33,35,41,42,43,51,52,50/v/all/p/all/c79/2670,32794,32796?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'D4N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Variável', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df['Valor'] = df['Valor'].fillna(0)  # substitui valores nulos por 0
    df['Valor'] = df['Valor'].astype(int)

    c.to_excel(df, dbs_path, 'sidra_3939-2.xlsx')
except Exception as e:
    errors['Sidra 3939-2'] = traceback.format_exc()


# # ************************
# # PLANILHA
# # ************************

# # gráfico 3.1
# try:
#     data = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name='Tabela17', sheet_name='Tabela17.3', skiprows=1)
#     indexes = data[data[data.columns[0]] == 'ANO'].index.tolist()  # extrai os índices das linhas que contêm os anos
#     variables = data.iloc[[i - 3 for i in indexes], 0].to_list()  # extrai as variáveis correspondentes a cada ano
#     # valores previsto: ['Valor Bruto da Produção 2010-2022', 'Consumo intermediário 2010-2022', 'Valor Adicionado Bruto 2010-2022']

#     # tratamento dos dados
#     dfs = []
#     for i in range(len(indexes)):
#         if i < 2:
#             df = data.iloc[indexes[i]:indexes[i + 1]].copy()
#         else:
#             df = data.iloc[indexes[i]:].copy()

#         columns = df.iloc[0].to_list()  # define a primeira linha como cabeçalho
#         cols = [col.split('\n')[0].strip() if '\n' in col else col.strip() for col in columns]  # remove quebras de linha
#         df.columns = cols  # renomeia as colunas
#         # valores previstos: ['ANO', 'VALOR DO ANO ANTERIOR', 'ÍNDICE DE VOLUME', 'VALOR A PREÇOS DO ANO ANTERIOR', 'ÍNDICE DE PREÇO', 'VALOR A PREÇO CORRENTE']

#         df[cols[0]] = df[cols[0]].astype(str)  # converte a primeira coluna para string
#         df_filtered = df[
#             (df[cols[0]].str.startswith('20')) & (df[cols[0]].str.len() == 4)
#         ].copy()

#         df_filtered[cols[0]] = df_filtered[cols[0]].astype(int)  # converte a primeira coluna para inteiro
#         df_filtered[cols[1:]] = df_filtered[cols[1:]].astype(float)  # converte a segunda coluna para float
#         df_filtered['Variável'] = variables[i]  # adiciona a variável correspondente

#         # deflação
#         df_filtered.sort_values(by=cols[0], ascending=False, inplace=True)  # ordena pelo ano
#         df_filtered.reset_index(drop=True, inplace=True)
#         df_filtered['Index'] = 100.00  # cria a coluna de índice
        
#         for row in range(1, len(df_filtered)):
#             df_filtered.loc[row, 'Index'] = df_filtered.loc[row - 1, 'Index'] / df_filtered.loc[row -1, cols[-2]]

#         dfs.append(df_filtered)

#     df_concat = pd.concat(dfs, ignore_index=True)
#     df_concat['Valor Ajustado'] = (df_concat[cols[-1]] / df_concat['Index']) * 100.00  # calcula o valor ajustado
#     df_concat.dropna(subset=['Valor Ajustado'], inplace=True)  # remove linhas com valores ajustados nulos

#     # organização da tabela
#     df_concat = df_concat[[cols[0], 'Variável', 'Valor Ajustado']]
#     df_concat['Variável'] = df_concat['Variável'].apply(lambda x:
#         'Valor bruto da produção' if 'valor bruto da produção' in x.lower().strip() else
#         'Consumo intermediário' if 'consumo intermediário' in x.lower().strip() else
#         'Valor adicionado bruto' if 'valor adicionado bruto' in x.lower().strip() else x
#     )
#     df_pivot = df_concat.pivot(index=cols[0], columns='Variável', values='Valor Ajustado').reset_index()

#     df_pivot.rename(columns={'ANO': 'Ano'}, inplace=True)  # renomeia a coluna de ano
#     df_pivot = df_pivot[['Ano', 'Valor bruto da produção', 'Consumo intermediário', 'Valor adicionado bruto']]  # reordena as colunas

#     df_pivot.to_excel(os.path.join(sheets_path, 'g3.1.xlsx'), index=False, sheet_name='g3.1')

# except Exception as e:
#     errors['Gráfico 3.1'] = traceback.format_exc()


# # gráfico 3.2
# try:
#     data = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name='Tabela17', sheet_name='Tabela17.3', skiprows=1)
#     indexes = data[data[data.columns[0]] == 'ANO'].index.tolist()  # extrai os índices das linhas que contêm os anos
#     variables = data.iloc[[i - 3 for i in indexes], 0].to_list()  # extrai as variáveis correspondentes a cada ano
#     # valores previsto: ['Valor Bruto da Produção 2010-2022', 'Consumo intermediário 2010-2022', 'Valor Adicionado Bruto 2010-2022']

#     # tratamento dos dados
#     dfs = []
#     for i in range(len(indexes)):
#         if i < 2:
#             df = data.iloc[indexes[i]:indexes[i + 1]].copy()
#         else:
#             df = data.iloc[indexes[i]:].copy()

#         columns = df.iloc[0].to_list()  # define a primeira linha como cabeçalho
#         cols = [col.split('\n')[0].strip() if '\n' in col else col.strip() for col in columns]  # remove quebras de linha
#         df.columns = cols  # renomeia as colunas
#         # valores previstos: ['ANO', 'VALOR DO ANO ANTERIOR', 'ÍNDICE DE VOLUME', 'VALOR A PREÇOS DO ANO ANTERIOR', 'ÍNDICE DE PREÇO', 'VALOR A PREÇO CORRENTE']

#         df[cols[0]] = df[cols[0]].astype(str)  # converte a primeira coluna para string
#         df_filtered = df[
#             (df[cols[0]].str.startswith('20')) & (df[cols[0]].str.len() == 4)
#         ].copy()

#         df_filtered[cols[0]] = df_filtered[cols[0]].astype(int)  # converte a primeira coluna para inteiro
#         df_filtered[cols[1:]] = df_filtered[cols[1:]].astype(float)  # converte a segunda coluna para float
#         df_filtered['Variável'] = variables[i]  # adiciona a variável correspondente

#         dfs.append(df_filtered)

#     df_concat = pd.concat(dfs, ignore_index=True)
#     df_concat = df_concat.query('`Variável`.str.lower().str.contains("valor adicionado bruto")', engine='python').copy()
#     df_concat['Valor Ajustado'] = (df_concat[cols[2]] - 1) * 100 # cria a coluna de valor ajustado

#     df_final = df_concat[[cols[0], 'Valor Ajustado']].copy()
#     df_final.rename(columns={cols[0]: 'Ano'}, inplace=True)  # renomeia a coluna de ano
#     df_final.rename(columns={'Valor Ajustado': 'Valor bruto adicionado'}, inplace=True)
#     df_final.sort_values(by='Ano', ascending=True, inplace=True)  # ordena pelo ano
#     df_final.dropna(subset=['Valor bruto adicionado'], inplace=True)  # remove linhas com valores ajustados nulos

#     df_final.to_excel(os.path.join(sheets_path, 'g3.2.xlsx'), index=False, sheet_name='g3.2')

# except Exception as e:
#     errors['Gráfico 3.2'] = traceback.format_exc()


# # gráfico 3.3
# try:
#     data = c.open_file(dbs_path, 'sidra_3939.xlsx', 'xls', sheet_name='Sheet1')
#     data.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano
#     data = data.query('Ano >= 2010').copy()  # filtra os dados para considerar apenas anos a partir de 2010
#     years = (data['Ano'].min(), data['Ano'].max())

#     # Para cada grupo de Região e Variável, pega o valor do ano anterior
#     data['Valor Anterior'] = data.groupby(['Região', 'Variável'])['Valor'].shift(1)
#     data['Valor Inicial'] = data.groupby(['Região', 'Variável'])['Valor'].transform('first')  # pega o valor do primeiro ano do grupo
#     data['Variação Anual'] = ((data['Valor'] / data['Valor Anterior']) - 1) * 100  # calcula a variação anual
#     data['Variação Acumulada'] = ((data['Valor'] / data['Valor Inicial']) - 1) * 100  # calcula a variação acumulada

#     # organização da tabela
#     df = data[['Ano', 'Região', 'Variável', 'Variação Anual', 'Variação Acumulada']].query('Ano == @data.Ano.max()').copy()
#     df_melted = df.melt(id_vars=['Ano', 'Região', 'Variável'], value_vars=['Variação Anual', 'Variação Acumulada'], var_name='Tipo de Variação', value_name='Valor')
#     df_pivoted = df_melted.pivot(index=['Tipo de Variação', 'Variável'], columns='Região', values='Valor').reset_index()

#     # tratamento final
#     df_final = df_pivoted.copy()
#     df_final.rename(columns={'Tipo de Variação': 'Período', 'Variável': 'Rebanho'}, inplace=True)
#     df_final['Período'] = df_final['Período'].apply(lambda x: f'{years[1]}/{years[1] - 1}' if 'Anual' in x else f'{years[1]}/{years[0]}')  # formata o período

#     df_final.to_excel(os.path.join(sheets_path, 'g3.3.xlsx'), index=False, sheet_name='g3.3')

# except Exception as e:
#     errors['Gráfico 3.3'] = traceback.format_exc()


# # gráfico 3.4
# try:
#     data = c.open_file(dbs_path, 'sidra_74.xlsx', 'xls', sheet_name='Sheet1')
#     data.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano
#     data = data.query('Ano >= 2010').copy()  # filtra os dados para considerar apenas anos a partir de 2010
#     data['Variável'] = data['Variável'] + ' (' + data['Unidade'] + ')'
#     data.drop(columns=['Unidade'], inplace=True)  # remove a coluna de unidade

#     # tratamento final
#     df_final = data.copy()
#     df_final = df_final.pivot(index=['Ano', 'Região'], columns='Variável', values='Valor').reset_index()

#     df_final.to_excel(os.path.join(sheets_path, 'g3.4.xlsx'), index=False, sheet_name='g3.4')

# except Exception as e:
#     errors['Gráfico 3.4'] = traceback.format_exc()


# gráfico 3.5
try:
    data = c.open_file(dbs_path, 'sidra_1092.xlsx', 'xls', sheet_name='Sheet1').query(
        'Variável.str.lower() == "animais abatidos" &' \
        'Ano >= 2010', engine='python'
    )
    data_aux = c.open_file(dbs_path, 'sidra_3939-2.xlsx', 'xls', sheet_name='Sheet1').query(
        'Variável.str.lower().str.contains("bovino") &' \
        'Ano >= 2010', engine='python'
    )

    # tratamento dos dados principais
    df_anual = data.groupby(['Ano', 'Região', 'Variável'], as_index=False)['Valor'].sum()  # agrupa os dados por Ano, Região e Variável
    df_ne = df_anual.query('Região in @c.ne_states').copy()  # filtra os dados para as regiões do Nordeste
    assert len(df_ne['Região'].unique()) == 9, 'Número de regiões do Nordeste diferente do esperado.'
    df_ne['Região'] = 'Nordeste'  # renomeia as regiões do Nordeste para 'Nordeste'
    df_ne = df_ne.groupby(['Ano', 'Região', 'Variável'], as_index=False)['Valor'].sum()  # agrupa os dados do Nordeste por Ano e Variável
    df_merged = pd.concat([df_anual, df_ne], ignore_index=True)  # concatena os dados anuais com os do Nordeste

    # união com os dados auxiliares
    df_right = data_aux[['Ano', 'Região', 'Valor']].copy()  # seleciona as colunas necessárias dos dados auxiliares
    df_right.rename(columns={'Valor': 'Valor Auxiliar'}, inplace=True)  # renomeia a coluna de valor auxiliar
    df_joined = pd.merge(df_merged, df_right, on=['Ano', 'Região'], how='left', validate='1:1')  # une os dados principais com os auxiliares

    # cálculos
    df_joined.drop(df_joined[df_joined['Valor Auxiliar'].isna()].index, inplace=True)  # remove linhas onde o valor auxiliar é zero
    df_joined.sort_values(['Região', 'Ano'], inplace=True)
    df_joined['Valor Anterior'] = df_joined.groupby('Região')['Valor'].shift(1)  # pega o valor do ano anterior
    df_joined['Valor Inicial'] = df_joined.groupby('Região')['Valor'].transform('first')  # pega o valor do primeiro ano do grupo

    df_joined['Razão'] = (df_joined['Valor'] / df_joined['Valor Auxiliar']) * 100  # calcula a razão entre o valor e o valor auxiliar
    df_joined['Razão Média'] = df_joined.groupby('Região')['Razão'].transform('mean')  # calcula a média da razão por região
    df_joined['Variação Acumulada'] = ((df_joined['Valor'] / df_joined['Valor Inicial']) - 1) * 100  # calcula a variação acumulada

    # exportação das tabelas
    # top 6 regiões com maior razão
    df_top_razao = df_joined.query('Ano == @df_joined["Ano"].max()').copy()
    df_top_razao['Razão 2'] = df_top_razao['Razão']
    df_top_razao['Razão 2'].loc[df_top_razao['Região'].isin(['Brasil', 'Nordeste'])] = np.nan  # zera a razão para Brasil e Nordeste para não interferir no ranking
    df_top_razao['Ranking'] = df_top_razao['Razão 2'].rank(method='first', ascending=False)  # cria o ranking da razão
    df_top_razao['Ranking'].loc[df_top_razao['Região'].isin(['Brasil', 'Nordeste'])] = np.nan  # zera o ranking para Brasil e Nordeste
    df_top_razao = df_top_razao[['Região', 'Razão', 'Ranking']].query('Ranking <= 6 | `Região` in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_top_razao.sort_values(by='Ranking', ascending=True, inplace=True)  # ordena pelo ranking
    df_top_razao.rename(columns={'Razão': 'Valor', 'Ranking': 'Ordem'}, inplace=True)  # renomeia a coluna de razão
    df_top_razao.to_excel(os.path.join(sheets_path, 'g3.5a.xlsx'), index=False, sheet_name=f'g3.5a {df_joined["Ano"].max()}')

    # top 6 regiões com maior média da razão
    df_top_razao_media = df_joined.query('Ano == @df_joined["Ano"].max()').copy()
    df_top_razao_media['Razão 2'] = df_top_razao_media['Razão Média']
    df_top_razao_media['Razão 2'].loc[df_top_razao_media['Região'].isin(['Brasil', 'Nordeste'])] = np.nan  # zera a razão para Brasil e Nordeste para não interferir no ranking
    df_top_razao_media['Ranking'] = df_top_razao_media['Razão 2'].rank(method='first', ascending=False)  # cria o ranking da razão média
    df_top_razao_media['Ranking'].loc[df_top_razao_media['Região'].isin(['Brasil', 'Nordeste'])] = np.nan  # zera o ranking para Brasil e Nordeste
    df_top_razao_media = df_top_razao_media[['Região', 'Razão Média', 'Ranking']].query('Ranking <= 6 | `Região` in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_top_razao_media.sort_values(by='Ranking', ascending=True, inplace=True)  # ordena pelo ranking
    df_top_razao_media.rename(columns={'Razão Média': 'Valor', 'Ranking': 'Ordem'}, inplace=True)  # renomeia a coluna de razão média
    df_top_razao_media.to_excel(os.path.join(sheets_path, 'g3.5b.xlsx'), index=False, sheet_name=f'g3.5b Média({df_joined["Ano"].min()}-{df_joined["Ano"].max()})')

    # top 6 regiões com maior variação acumulada
    df_top_variacao = df_joined.query('Ano == @df_joined["Ano"].max()').copy()
    df_top_variacao['Variação Acumulada 2'] = df_top_variacao['Variação Acumulada']
    df_top_variacao['Variação Acumulada 2'].loc[df_top_variacao['Região'].isin(['Brasil', 'Nordeste'])] = np.nan  # zera a razão para Brasil e Nordeste para não interferir no ranking
    df_top_variacao['Ranking'] = df_top_variacao['Variação Acumulada 2'].rank(method='first', ascending=False)  # cria o ranking da razão
    df_top_variacao['Ranking'].loc[df_top_variacao['Região'].isin(['Brasil', 'Nordeste'])] = np.nan  # zera o ranking para Brasil e Nordeste
    df_top_variacao = df_top_variacao[['Região', 'Variação Acumulada', 'Ranking']].query('Ranking <= 6 | `Região` in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_top_variacao.sort_values(by='Ranking', ascending=True, inplace=True)  # ordena pelo ranking
    df_top_variacao.rename(columns={'Variação Acumulada': 'Valor', 'Ranking': 'Ordem'}, inplace=True)  # renomeia a coluna de razão
    df_top_variacao.to_excel(os.path.join(sheets_path, 'g3.5c.xlsx'), index=False, sheet_name=f'g3.5c Aumento({df_joined["Ano"].min()}-{df_joined["Ano"].max()})')

except Exception as e:
    errors['Gráfico 3.5'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g3.1--g3.2--g3.3--g3.4--g3.5--g3.6--g3.7--g3.8--g3.9--g3.10.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
