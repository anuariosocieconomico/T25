import functions as c
import os
import pandas as pd
import numpy as np
import json
import traceback
import tempfile
import shutil
import sidrapy


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

# url da base contas regionais
url = 'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Contas_Regionais'

# download do arquivo pib pela ótica da renda
try:
    response = c.open_url(url)
    df = pd.DataFrame(response.json())

    # pequisa pela publicação mais recente --> inicia com '2' e possui 4 caracteres
    df = df.loc[
        (df['name'].str.startswith('2')) &
        (df['name'].str.len() == 4),
        ['name', 'path']
    ]
    df['name'] = df['name'].astype(int)
    df.sort_values(by='name', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # obtém o caminho da publicação mais recente e adiciona à url de acesso aos arquivos
    last_year = df['name'][0]
    url_to_get = url + '/' + str(last_year) + '/xls'
    response = c.open_url(url_to_get)
    df = pd.DataFrame(response.json())

    while True:
        try:
            url_to_get_final = df.loc[
                (df['name'].str.startswith('Conta_da_Producao_2010')) &
                (df['name'].str.endswith('.zip')),
                'url'
            ].values[0]
            break
        except:
            last_year -= 1
            url_to_get = url + '/' + str(last_year) + '/xls'
            response = c.open_url(url_to_get)
            df = pd.DataFrame(response.json())
            if last_year == 0:
                errors[url + ' (Conta da Produção)'] = 'Arquivo não encontrado em anos anteriores'
                raise Exception('Arquivo não encontrado em anos anteriores')

    # downloading e organização do arquivo pib pela ótica da renda
    file = c.open_url(url_to_get_final)
    c.to_file(dbs_path, 'ibge_conta_producao.zip', file.content)
except Exception as e:
    errors[url + ' (Conta da Produção)'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# # gráfico 2.1
# try:
#     data = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name='Tabela17', sheet_name='Tabela17.2', skiprows=1)
#     columns = data[data[data.columns[0]] == 'ANO'].values[0]  # extrai os nomes das colunas armazenados em linhas
#     columns = [col.strip().replace('\n', ' ') for col in columns]  # remove espaços em branco e quebras de linha
#     # COLUNAS = [
#     #   'ANO', 'VALOR DO ANO ANTERIOR (1 000 000 R$)', 'ÍNDICE DE VOLUME', 'VALOR A PREÇOS DO ANO ANTERIOR (1 000 000 R$)',
#     #   'ÍNDICE DE PREÇO', 'VALOR A PREÇO CORRENTE (1 000 000 R$)'
#     # ]

#     # extrai os índices das tabelas que contêm os dados de interesse
#     # numa mesma aba, há três tabelas dispostas verticalmente
#     tables_indexes = data[
#         data[data.columns[0]].str.contains("Valor Bruto da Produção") |
#         data[data.columns[0]].str.contains("Consumo intermediário") |
#         data[data.columns[0]].str.contains("Valor Adicionado Bruto")
#     ].index.tolist()

#     dfs = []
#     for i, index in enumerate(tables_indexes):
#         if i < 2:
#             df = data.iloc[index:tables_indexes[i + 1]].copy()  # se for até a segunda tabela, extrai as linhas dentro do intervalo
#         else:
#             df = data.iloc[index:].copy()  # se for a última tabela, extrai as linhas até o final da aba

#         df.columns = columns
#         df.reset_index(drop=True, inplace=True)
#         var = df.iloc[0, 0]  # extrai a atividade econômica
#         df[columns[0]] = df[columns[0]].astype(str).str.strip()  # transforma a coluna de anos em string e remove espaços em branco
#         df = df.loc[(df[columns[0]].str.startswith('20')) & (df[columns[0]].str.len() == 4)].copy()  # mantém apenas as linhas de interesse

#         # converte os valores das colunas para o tipo adequado
#         for ii, col in enumerate(df.columns):
#             if ii == 0:
#                 df[col] = df[col].astype(int)
#             else:
#                 df[col] = df[col].astype(float)

#         # adiciona a atividade econômica como coluna
#         df['Atividade'] = (
#             'Valor bruto da produção' if var.startswith('Valor Bruto da Produção') else
#             'Consumo intermediário' if var.startswith('Consumo intermediário') else
#             'Valor adicionado bruto'
#         )

#         # deflação dos valores
#         df.sort_values(by=columns[0], ascending=False, inplace=True)
#         df.reset_index(drop=True, inplace=True)
#         df['Index'] = 100.00

#         for row in range(1, len(df)):
#             df.loc[row, 'Index'] = df.loc[row - 1, 'Index'] / df.loc[row - 1, columns[-2]]

#         df['Value'] = (df[columns[-1]] / df['Index']) * 100

#         dfs.append(df)

#     df_concat = pd.concat(dfs, ignore_index=True)
#     df_pivoted = df_concat.pivot_table(
#         index=columns[0], columns='Atividade', values='Value'
#     ).reset_index()
#     df_pivoted = df_pivoted[[columns[0], 'Valor bruto da produção', 'Consumo intermediário', 'Valor adicionado bruto']]
#     df_pivoted.rename(columns={columns[0]: 'Ano'}, inplace=True)
#     df_pivoted['Ano'] = df_pivoted['Ano'].astype(int)

#     df_pivoted.to_excel(os.path.join(sheets_path, 'g2.1.xlsx'), index=False, sheet_name='g2.1')

# except Exception as e:
#     errors['Gráfico 2.1'] = traceback.format_exc()

# gráfico 2.2
try:
    data = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name='Tabela17', sheet_name='Tabela17.2', skiprows=1)
    columns = data[data[data.columns[0]] == 'ANO'].values[0]  # extrai os nomes das colunas armazenados em linhas
    columns = [col.strip().replace('\n', ' ') for col in columns]  # remove espaços em branco e quebras de linha
    # COLUNAS = [
    #   'ANO', 'VALOR DO ANO ANTERIOR (1 000 000 R$)', 'ÍNDICE DE VOLUME', 'VALOR A PREÇOS DO ANO ANTERIOR (1 000 000 R$)',
    #   'ÍNDICE DE PREÇO', 'VALOR A PREÇO CORRENTE (1 000 000 R$)'
    # ]

    # extrai os índices das tabelas que contêm os dados de interesse
    # numa mesma aba, há três tabelas dispostas verticalmente
    tables_indexes = data[
        data[data.columns[0]].str.contains("Valor Adicionado Bruto", na=False)
    ].index.tolist()

    df = data.iloc[tables_indexes[0]:].copy()

    df.columns = columns
    df.reset_index(drop=True, inplace=True)
    var = df.iloc[0, 0]  # extrai a atividade econômica
    df[columns[0]] = df[columns[0]].astype(str).str.strip()  # transforma a coluna de anos em string e remove espaços em branco
    df = df.loc[(df[columns[0]].str.startswith('20')) & (df[columns[0]].str.len() == 4)].copy()  # mantém apenas as linhas de interesse

    # converte os valores das colunas para o tipo adequado
    for ii, col in enumerate(df.columns):
        if ii == 0:
            df[col] = df[col].astype(int)
        else:
            df[col] = df[col].astype(float)

    # deflação dos valores
    df.sort_values(by=columns[0], ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    df['Variação'] = (df[columns[2]] - 1) * 100

    df_final = df[[columns[0], 'Variação']].copy()
    df_final.rename(columns={columns[0]: 'Ano'}, inplace=True)
    df_final['Ano'] = df_final['Ano'].astype(int)
    df_final.dropna(inplace=True)

    df_final.to_excel(os.path.join(sheets_path, 'g2.2.xlsx'), index=False, sheet_name='g2.2')

except Exception as e:
    errors['Gráfico 2.1'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g2.1--g2.2--g2.3--g2.4--g2.5--g2.6.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
