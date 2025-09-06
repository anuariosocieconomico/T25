import functions as c
import os
import pandas as pd
import json
import sidrapy
import traceback
import tempfile
import shutil
from datetime import datetime
from time import sleep


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}
session = c.create_session_with_retries()


# ************************
# DOWNLOAD DA BASE DE DADOS E PLANILHA
# ************************

# # Gráfico 16.1
# try:
#     # looping de requisições para cada tabela da figura
#     dfs = []
#     for reg in [('1', 'all'), ('2', '2'), ('3', '28')]:
#         data = sidrapy.get_table(
#             table_code='6578',
#             territorial_level=reg[0],ibge_territorial_code=reg[1],
#             variable='10163',
#             period="all"
#         )

#         # remoção da linha 0, dados para serem usados como rótulos das colunas
#         data.drop(0, axis='index', inplace=True)

#         dfs.append(data)

#     data = pd.concat(dfs, ignore_index=True)

#     # seleção das colunas de interesse
#     data = data[['D1N', 'D3N', 'D2N', 'V']].copy()
#     data['D2N'] = pd.to_datetime(data['D2N'], format='%Y')

#     # renomeação das colunas
#     data.columns = ['Região', 'Variável', 'Ano', 'Valor']

#     # classificação dos dados
#     data['Ano'] = data['Ano'].dt.strftime('%d/%m/%Y')
#     data['Valor'] = data['Valor'].astype('float')

#     # conversão em arquivo csv
#     c.to_excel(data, sheets_path, 'g16.1.xlsx')

# except Exception as e:
#     errors['Gráfico 16.1'] = traceback.format_exc()


# # Tabela 16.1
# try:
#     # looping de requisições para cada tabela da figura
#     dbs = []
#     for key, table in {
#         'Proporção (%) de domicílios particulares permanentes': {
#             'tb': '6821', 'var': '9784', 'class': {'63': '4342'}
#         },
#         'Com banheiro ou sanitário de uso exclusivo dos moradores': {
#             'tb': '6734', 'var': '9984', 'class': {'1': '6795'}
#         },
#         'Abastecidos por rede geral de água': {
#             'tb': '6731', 'var': '9784', 'class': {'1': '6795', '825': '46285'}
#         }
#         , 'Com esgotamento por rede coletora de esgoto ou pluvial': {
#             'tb': '6735', 'var': '9988', 'class': {'1': '6795', '11558': '46290'}
#         }
#         , 'Com esgotamento por rede coletora de esgoto ou pluvial-atualizado': {
#             'tb': '7192', 'var': '9988', 'class': {'1': '6795', '11558': '47930,47931'}
#         }
#         , 'Atendidos por serviço de coleta de lixo (direta ou indireta)': {
#             'tb': '6736', 'var': '9784', 'class': {'1': '6795', '67': '4661'}
#         }
#         , 'Com energia elétrica fornecida por rede geral': {
#             'tb': '6737', 'var': '5074', 'class': {'1': '6795', '827': '46297'}
#         }
#         , 'Com gás de botijão, ou encanado, como combustível para preparos de alimentos': {
#             'tb': '6739', 'var': '10250', 'class': {'828': '46298'}
#         }
#     }.items():
#         # looping de requisições para cada região da tabela
#         dfs = []
#         for reg in [('1', 'all'), ('2', '2'), ('3', '28')]:
#             data = sidrapy.get_table(
#                 table_code=table['tb'],
#                 territorial_level=reg[0],ibge_territorial_code=reg[1],
#                 variable=table['var'],
#                 classifications=table['class'],
#                 period="all"
#             )

#             # remoção da linha 0, dados para serem usados como rótulos das colunas
#             data.drop(0, axis='index', inplace=True)

#             dfs.append(data)

#         # união das regiões da variável
#         data = pd.concat(dfs, ignore_index=True)
#         data['TAG'] = key
#         dbs.append(data)
#         sleep(1)

#     # todos os dfs
#     df_final = pd.concat(dbs, ignore_index=True)

#     # seleção das colunas de interesse
#     df_final = df_final[['D1N', 'D3N', 'D4N', 'D2N', 'V', 'TAG']].copy()
#     df_final.columns = ['Região', 'Variável', 'Classe', 'Ano', 'Valor', 'TAG']
#     df_final['Valor'] = df_final['Valor'].astype(float )
    
#     # agrupamento da variável de esgoto, obtida em tabelas distintas
#     df_grouped = df_final[df_final['TAG'].str.startswith('Com esgotamento por rede coletora de esgoto ou pluvial')].copy()
#     df_grouped['TAG'] = df_grouped['TAG'].str.split('-').str[0]
#     df_grouped = df_grouped.groupby(by=['Região', 'Variável', 'Classe', 'Ano', 'TAG'], as_index=False)['Valor'].sum()
    
#     # remoção dos dados não agrupados e df_final adição dos agrupados
#     data_filtered = df_final[~df_final['TAG'].str.contains('Com esgotamento por rede coletora de esgoto ou pluvial')].copy()
#     data = pd.concat([data_filtered, df_grouped], ignore_index=True)

#     # classificação dos dados
#     data['Ano'] = pd.to_datetime(data['Ano'], format='%Y')
#     data['Ano'] = data['Ano'].dt.strftime('%d/%m/%Y')

#     data.loc[:, 'Variável'] = data['TAG']
#     data = data[['Região', 'Variável', 'Ano', 'Valor']].copy()    
#     data.sort_values(by=['Região', 'Variável', 'Ano',], inplace=True)

#     # conversão em arquivo csv
#     c.to_excel(data, sheets_path, 't16.1.xlsx')

# except Exception as e:
#     errors['Tabela 16.1'] = traceback.format_exc()


# # Gráfico 16.3
# try:
#     # looping de requisições para cada tabela da figura
#     dfs = []
#     for reg in [('1', 'all'), ('2', '2'), ('3', '28')]:
#         data = sidrapy.get_table(
#             table_code='6821',
#             territorial_level=reg[0],ibge_territorial_code=reg[1],
#             variable='9784',
#             classifications={'63': 'allxt'},
#             period="all"
#         )

#         # remoção da linha 0, dados para serem usados como rótulos das colunas
#         data.drop(0, axis='index', inplace=True)

#         dfs.append(data)

#     data = pd.concat(dfs, ignore_index=True)

#     # seleção das colunas de interesse
#     data = data[['D1N', 'D4N', 'D2N', 'V']].copy()
#     data['D2N'] = pd.to_datetime(data['D2N'], format='%Y')

#     # renomeação das colunas
#     data.columns = ['Região', 'Variável', 'Ano', 'Valor']

#     # classificação dos dados
#     data['Ano'] = data['Ano'].dt.strftime('%d/%m/%Y')
#     data['Valor'] = pd.to_numeric(data['Valor'], errors='coerce')

#     # conversão em arquivo csv
#     c.to_excel(data, sheets_path, 'g16.3.xlsx')

# except Exception as e:
#     errors['Gráfico 16.3'] = traceback.format_exc()


# Planilha de Indicadores Sociais g16.2
try:
    # projeção da taxa
    year = datetime.today().year
    dfs = []
    while True:
        print(f'Tentando baixar a planilha de Indicadores Sociais do ano {year}...')
        url = f'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Indicadores_Sociais/Sintese_de_Indicadores_Sociais/Sintese_de_Indicadores_Sociais_{str(year)}/Tabelas/xls'
        response = c.open_url(url)
        if response.status_code == 200 and len(response.json()) > 1:  # verifica se a resposta é válida e contém dados
            data = pd.DataFrame(response.json())
            download = data.loc[data['name'].str.lower().str.contains('moradia'), 'url'].values[0]  # extrai a URL de download do arquivo de moradia
            file = c.open_url(download)
            if file.status_code == 200:
                zip_file = c.open_file(file_path=file.content, ext='zip', excel_name='Tabela 3.8', skiprows=9)
                for tb in zip_file.keys():  # percorre as abas do excel, ignorando as que contêm (CV)
                    if '(CV)' not in tb:
                        df = zip_file[tb].dropna(how='any').reset_index(drop=True)  # remove linhas com valores nulos
                        df = df.iloc[:, [0, 4]]  # seleciona apenas as colunas de interesse (UF e Mais de 3 moradores)
                        df.columns = ['UF', 'Valor']
                        df['Ano'] = tb

                        dfs.append(df)
                    
                df_final = pd.concat(dfs, ignore_index=True)
                c.to_csv(df_final, dbs_path, 'indicadores_sociais_moradia.csv')
                break

            else:
                errors['Planilha de Indicadores Sociais (Moradia)'] = f'Erro ao baixar a planilha de Indicadores Sociais do ano {year}. Status code: {file.status_code}'
                break

        else:
            if year > 2023:
                year -= 1
            else:
                errors['Planilha de Indicadores Sociais (Moradia)'] = 'Não foi possível encontrar uma planilha válida de Indicadores Sociais.'
                break

except:
    errors['Planilha de Indicadores Sociais (Moradia)'] = traceback.format_exc()


# Gráfico 16.2
try:
    # importação dos indicadores
    df = c.open_file(dbs_path, 'indicadores_sociais_moradia.csv', 'csv').query('UF in ["Brasil", "Nordeste", "Sergipe"]')
    df.sort_values(by=['UF', 'Ano'], inplace=True)
    df['Ano'] = '31/12/' + df['Ano'].astype(str)
    df.rename(columns={'UF': 'Região'}, inplace=True)
    df_final = df[['Região', 'Ano', 'Valor']].copy()

    c.to_excel(df_final, sheets_path, 'g16.2.xlsx')

except:
    errors['Gráfico 16.2'] = traceback.format_exc()


# Planilha de Indicadores Sociais g16.4
try:
    # projeção da taxa
    year = datetime.today().year
    dfs = []
    while True:
        print(f'Tentando baixar a planilha de Indicadores Sociais do ano {year}...')
        url = f'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Indicadores_Sociais/Sintese_de_Indicadores_Sociais/Sintese_de_Indicadores_Sociais_{str(year)}/Tabelas/xls'
        response = c.open_url(url)
        if response.status_code == 200 and len(response.json()) > 1:  # verifica se a resposta é válida e contém dados
            data = pd.DataFrame(response.json())
            download = data.loc[data['name'].str.lower().str.contains('moradia'), 'url'].values[0]  # extrai a URL de download do arquivo de moradia
            file = c.open_url(download)
            if file.status_code == 200:
                zip_file = c.open_file(file_path=file.content, ext='zip', excel_name='Tabela 3.22', skiprows=8)
                for tb in zip_file.keys():  # percorre as abas do excel, ignorando as que contêm (CV)
                    if '(CV)' not in tb:
                        df = zip_file[tb].dropna(how='any').reset_index(drop=True)  # remove linhas com valores nulos
                        df = df.iloc[:, [0, 4]]  # seleciona apenas as colunas de interesse (UF e Mais de 3 moradores)
                        df.columns = ['UF', 'Valor']
                        df['Ano'] = tb

                        dfs.append(df)
                    
                df_final = pd.concat(dfs, ignore_index=True)
                c.to_csv(df_final, dbs_path, 'indicadores_sociais_moradia_2.csv')
                break

            else:
                errors['Planilha de Indicadores Sociais (Moradia)'] = f'Erro ao baixar a planilha de Indicadores Sociais do ano {year}. Status code: {file.status_code}'
                break

        else:
            if year > 2023:
                year -= 1
            else:
                errors['Planilha de Indicadores Sociais (Moradia)'] = 'Não foi possível encontrar uma planilha válida de Indicadores Sociais.'
                break

except:
    errors['Planilha de Indicadores Sociais (Moradia)'] = traceback.format_exc()

# Gráfico 16.4
try:
    # importação dos indicadores
    df = c.open_file(dbs_path, 'indicadores_sociais_moradia_2.csv', 'csv').query('UF in ["Brasil", "Nordeste", "Sergipe"]')
    df.sort_values(by=['UF', 'Ano'], inplace=True)
    df['Ano'] = '31/12/' + df['Ano'].astype(str)
    df.rename(columns={'UF': 'Região'}, inplace=True)
    df_final = df[['Região', 'Ano', 'Valor']].copy()

    c.to_excel(df_final, sheets_path, 'g16.4.xlsx')

except:
    errors['Gráfico 16.4'] = traceback.format_exc()


# # Tabela 16.2
# try:
#     # looping de requisições para cada tabela da figura
#     dfs = []
#     for reg in [('1', 'all'), ('2', '2'), ('3', '28')]:
#         data = sidrapy.get_table(
#             table_code='6677',
#             territorial_level=reg[0],ibge_territorial_code=reg[1],
#             variable='10250',
#             classifications={'845': 'allxt'},
#             period="all"
#         )

#         # remoção da linha 0, dados para serem usados como rótulos das colunas
#         data.drop(0, axis='index', inplace=True)

#         dfs.append(data)

#     data = pd.concat(dfs, ignore_index=True)

#     # seleção das colunas de interesse
#     data = data[['D1N', 'D4N', 'D2N', 'V']].copy()
#     data['D2N'] = pd.to_datetime(data['D2N'], format='%Y')

#     # renomeação das colunas
#     data.columns = ['Região', 'Variável', 'Ano', 'Valor']

#     # classificação dos dados
#     data['Ano'] = data['Ano'].dt.strftime('%d/%m/%Y')
#     data['Valor'] = data['Valor'].astype('float')

#     # conversão em arquivo csv
#     c.to_excel(data, sheets_path, 't16.2.xlsx')

# except Exception as e:
#     errors['Tabela 16.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g16.1--t16.1--g16.3--g16.4--t16.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
