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
import time
import re
import requests


# pandas config
pd.set_option('display.float_format', lambda x: '{:,.4f}'.format(x))  # mostra os números com 4 casas decimais e separador de milhar

# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

# # deflator IPEA IPCA
# try:
#     data = ipeadatapy.timeseries('PRECOS_IPCAG')
#     data.rename(columns={'YEAR': 'Ano', 'VALUE ((% a.a.))': 'Valor'}, inplace=True)  # renomeia as colunas
#     c.to_excel(data, dbs_path, 'ipeadata_ipca.xlsx')
# except Exception as e:
#     errors['IPEA IPCA'] = traceback.format_exc()


# # sidra 1187 - estimativa da população
# url = 'https://apisidra.ibge.gov.br/values/t/1187/n1/all/n2/2/n3/28/v/all/p/all/c2/6794/d/v2513%201?formato=json'
# try:
#     data = c.open_url(url)
#     df = pd.DataFrame(data.json())
#     df = df[['D3N', 'D1N', 'V']].copy()
#     df.columns = ['Ano', 'Região', 'Valor']
#     df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
#     df['Ano'] = df['Ano'].astype(int)
#     df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

#     c.to_excel(df, dbs_path, 'sidra_1187.xlsx')
# except Exception as e:
#     errors['Sidra 1187'] = traceback.format_exc()


# # sidra 7113 - estimativa da população
# url = 'https://apisidra.ibge.gov.br/values/t/7113/n1/all/n2/2/n3/28/v/10267/p/all/c2/6794/c58/2795/d/v10267%201?formato=json'
# try:
#     data = c.open_url(url)
#     df = pd.DataFrame(data.json())
#     df = df[['D3N', 'D1N', 'V']].copy()
#     df.columns = ['Ano', 'Região', 'Valor']
#     df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
#     df['Ano'] = df['Ano'].astype(int)
#     df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

#     c.to_excel(df, dbs_path, 'sidra_7113.xlsx')
# except Exception as e:
#     errors['Sidra 7113'] = traceback.format_exc()


# sidra 356 - estimativa da população
url = 'https://apisidra.ibge.gov.br/values/t/356/n1/all/n2/2/n3/28/v/1887/p/all/c1/6795/c2/6794/c58/2795/d/v1887%201?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    c.to_excel(df, dbs_path, 'sidra_356.xlsx')
except Exception as e:
    errors['Sidra 356'] = traceback.format_exc()


# sidra 7126 - estimativa da população
url = 'https://apisidra.ibge.gov.br/values/t/7126/n1/all/n2/2/n3/28/v/3593/p/all/c2/6794/c58/2795/d/v3593%201?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    c.to_excel(df, dbs_path, 'sidra_7126.xlsx')
except Exception as e:
    errors['Sidra 7126'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# # gráfico 17.1
# try:
#     # tabela ibge
#     data = c.open_file(dbs_path, 'sidra_1187.xlsx', 'xls', sheet_name='Sheet1')
#     data['Valor'] = 100 - data['Valor']
#     df_analfabetismo = c.open_file(dbs_path, 'sidra_7113.xlsx', 'xls', sheet_name='Sheet1')
    
#     df = pd.concat([data, df_analfabetismo], ignore_index=True)

#     df['Ano'] = '01/01/' + df['Ano'].astype(str)  # formata a coluna Ano para o formato de data
#     df.rename(columns={'Valor': 'Taxa de analfabetismo'}, inplace=True)
#     df.sort_values(by=['Região', 'Ano'], inplace=True)  # ordena os dados por Região e Ano

#     df_final = df[['Região', 'Ano', 'Taxa de analfabetismo']].copy()

#     c.to_excel(df_final, sheets_path, 'g17.1.xlsx')

# except Exception as e:
#     errors['Gráfico 17.1'] = traceback.format_exc()


# gráfico 17.2
try:
    # tabela ibge
    data = c.open_file(dbs_path, 'sidra_356.xlsx', 'xls', sheet_name='Sheet1')
    data_new = c.open_file(dbs_path, 'sidra_7126.xlsx', 'xls', sheet_name='Sheet1')
    
    df = pd.concat([data, data_new], ignore_index=True)

    df['Ano'] = '01/01/' + df['Ano'].astype(str)  # formata a coluna Ano para o formato de data
    df.sort_values(by=['Região', 'Ano'], inplace=True)  # ordena os dados por Região e Ano

    df_final = df[['Região', 'Ano', 'Valor']].copy()

    c.to_excel(df_final, sheets_path, 'g17.2.xlsx')

except Exception as e:
    errors['Gráfico 17.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g17.1--g17.2--g17.3--g17.4--t17.1--t17.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
