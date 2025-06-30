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


# # sidra 5603
# url = 'https://apisidra.ibge.gov.br/values/t/5603/n3/28/v/631,706,808,810,811/p/all?formato=json'
# try:
#     data = c.open_url(url)
#     df = pd.DataFrame(data.json())
#     df = df[['D3N', 'D1N', 'D2N', 'V']].copy()
#     df.columns = ['Ano', 'Região', 'Variável', 'Valor']
#     df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
#     df[['Ano', 'Valor']] = df[['Ano', 'Valor']].astype(int)

#     c.to_excel(df, dbs_path, 'sidra_5603.xlsx')
# except Exception as e:
#     errors['Sidra 5603'] = traceback.format_exc()


# # deflator IPEA IPP
# try:
#     data = ipeadatapy.timeseries('IPP12_IPPC12')
#     df = data.loc[data['MONTH'] == 12, ['YEAR', 'VALUE (-)']].copy()  # seleciona as colunas de ano e deflator
#     df.sort_values(by='YEAR', ascending=False, inplace=True)  # ordena pelo ano
#     df.reset_index(drop=True, inplace=True)  # reseta o índice
#     df.rename(columns={'YEAR': 'Ano', 'VALUE (-)': 'Deflator IPP'}, inplace=True)

#     c.to_excel(df, dbs_path, 'ipeadata_ipp.xlsx')
# except Exception as e:
#     errors['IPEA IPP'] = traceback.format_exc()


# sidra 1849
url = 'https://apisidra.ibge.gov.br/values/t/1849/n3/all/v/810/p/all/c12762/116880,116911,116952,116965,116985,117048,117099,117136,117261,117897?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'D2N', 'D4N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Variável', 'Atividade', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna de valor para numérico, tratando erros
    df['Valor'] = df['Valor'].fillna(0)  # substitui valores NaN por 0
    df[['Ano', 'Valor']] = df[['Ano', 'Valor']].astype(int)

    c.to_excel(df, dbs_path, 'sidra_1849.xlsx')
except Exception as e:
    errors['Sidra 1849'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# # gráfico 4.1
# try:
#     data = c.open_file(dbs_path, 'sidra_5603.xlsx', 'xls', sheet_name='Sheet1').query(
#         '(`Variável`.str.lower().str.contains("valor bruto da produção") |' \
#         '`Variável`.str.lower().str.contains("custos das operações") |' \
#         '`Variável`.str.lower().str.contains("valor da transformação")) &' \
#         'Ano >= 2010', engine='python'
#     )
#     deflator = c.open_file(dbs_path, 'ipeadata_ipp.xlsx', 'xls', sheet_name='Sheet1')
#     min_year = data['Ano'].min()  # ano mínimo da tabela
#     max_year = data['Ano'].max()  # ano máximo da tabela

#     # tratamento do deflator
#     deflator = deflator.query(f'Ano >= {min_year} and Ano <= {max_year}')  # filtra os anos
#     deflator.reset_index(drop=True, inplace=True)  # reseta o índice
#     deflator['Diff'] = None
#     deflator['Index'] = 100.00
#     for row in range(1, len(deflator)):
#         deflator.loc[row, 'Diff'] = deflator.loc[row - 1, 'Deflator IPP'] / deflator.loc[row, 'Deflator IPP']  # variação percentual
#         deflator.loc[row, 'Index'] = deflator.loc[row - 1, 'Index'] / deflator.loc[row, 'Diff']  # índice de preços

#     # join das tabelas
#     df = pd.merge(data, deflator[['Ano', 'Index']], on='Ano', how='left', validate='m:1')
#     df.sort_values(by=['Região', 'Variável', 'Ano'], ascending=[True, True, False], inplace=True)  # ordena pela região, variável e ano
#     df['Valor corrigido'] = (df['Valor'] / df['Index']) * 100  # valor corrigido
#     df['Variação anual'] = df.groupby(['Região', 'Variável'])['Valor corrigido'].pct_change() * 100  # variação anual
#     df.rename(columns={'Index': 'Índice'}, inplace=True)
#     df = df[['Região', 'Variável', 'Ano', 'Valor', 'Valor corrigido', 'Variação anual', 'Índice']].copy()  # reordena as colunas

#     c.to_excel(df, sheets_path, 'g4.1.xlsx')

# except Exception as e:
#     errors['Gráfico 4.1'] = traceback.format_exc()


# # gráfico 4.2
# try:
#     data = c.open_file(dbs_path, 'sidra_5603.xlsx', 'xls', sheet_name='Sheet1').query(
#         '(`Variável`.str.lower().str.contains("número de unidades locais") |' \
#         '`Variável`.str.lower().str.contains("pessoal ocupado")) &' \
#         'Ano >= 2010', engine='python'
#     )
    
#     df = data.pivot(index=['Região', 'Ano'], columns='Variável', values='Valor').reset_index(drop=False)  # cria a tabela dinâmica
#     df['Ano'] = '01/01/' + df['Ano'].astype(str)  # formata a coluna de ano

#     c.to_excel(df, sheets_path, 'g4.2.xlsx')

# except Exception as e:
#     errors['Gráfico 4.2'] = traceback.format_exc()


# tabela 4.1
try:
    data = c.open_file(dbs_path, 'sidra_1849.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= 2010', engine='python')
    
    df_br = data.copy()
    df_br.loc[:, 'Região'] = 'Brasil'  # adiciona a linha do Brasil
    df_br = df_br.groupby(data.columns.to_list()[:-1], as_index=False).sum()  # agrupa por ano, variável e atividade, somando os valores

    df_ne = data.query('`Região` in @c.ne_states', engine='python').copy()
    df_ne.loc[:, 'Região'] = 'Nordeste'  # adiciona a linha do Nordeste
    df_ne = df_ne.groupby(data.columns.to_list()[:-1], as_index=False).sum()  # agrupa por ano, variável e atividade, somando os valores

    df_se = data.loc[data['Região'] == 'Sergipe', ['Ano', 'Atividade', 'Valor']].copy()
    df_se.rename(columns={'Valor': 'Valor Sergipe'}, inplace=True)  # renomeia a coluna de valor para Valor Sergipe

    df = pd.concat([df_br, df_ne], ignore_index=True)  # concatena as tabelas
    df_joined = pd.merge(df, df_se, on=['Ano', 'Atividade'], how='left', validate='m:1')  # join com a tabela de Sergipe
    df_joined['Razão'] = np.where(df_joined['Valor'] != 0, (df_joined['Valor Sergipe'] / df_joined['Valor']) * 100, np.nan)

    df_final = df_joined[['Ano', 'Atividade', 'Região', 'Razão']].copy()
    df_final['Região'] = df_final['Região'].map({'Brasil': 'BR', 'Nordeste': 'NE'})  # mapeia as regiões
    df_final.rename(columns={'Razão': 'Valor'}, inplace=True)  # renomeia a coluna de razão
    df_final.sort_values(by=['Ano', 'Região', 'Atividade'], inplace=True)  # ordena pela região, ano e atividade

    df_final.to_excel(os.path.join(sheets_path, 't4.1.xlsx'), index=False, sheet_name='t4.1')

except Exception as e:
    errors['Tabela 4.1'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g4.1--g4.2--t4.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
