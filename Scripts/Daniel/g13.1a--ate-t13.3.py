import functions as c
import os
import pandas as pd
import numpy as np
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


# ************************
# DOWNLOAD DA BASE DE DADOS E PLANILHA
# ************************


# Tabela 16.1
try:
    # looping de requisições para cada tabela da figura
    dbs = []
    for key, table in {
        'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência': {
            'tb': '6402', 'var': '4088,4090,4099', 'class': {'86': '95251'}
        },
        'População': {
            'tb': '5917', 'var': '606', 'class': {'2': '6794'}
        }
    }.items():
        # looping de requisições para cada região da tabela
        dfs = []
        for reg in [('1', 'all'), ('2', '2'), ('3', 'all')]:
            data = sidrapy.get_table(
                table_code=table['tb'],
                territorial_level=reg[0],ibge_territorial_code=reg[1],
                variable=table['var'],
                classifications=table['class'],
                period="all"
            )

            # remoção da linha 0, dados para serem usados como rótulos das colunas
            data.drop(0, axis='index', inplace=True)

            dfs.append(data)

        # união das regiões da variável
        data = pd.concat(dfs, ignore_index=True)
        dbs.append(data)
        sleep(1)

    # todos os dfs
    df_concat = pd.concat(dbs, ignore_index=True)

    # seleção das colunas e linhas de interesse e tratamentos básicos
    df_concat = df_concat[['D1N', 'D3N', 'D2N', 'V']].copy()
    df_concat.columns = ['Região', 'Variável', 'Ano', 'Valor']
    df_concat['Trimestre'] = df_concat['Ano'].apply(lambda x: x[0]).astype(int)
    df_concat['Ano'] = df_concat['Ano'].apply(lambda x: x.split(' ')[-1]).astype(int)
    df_concat['Valor'] = df_concat['Valor'].replace('...', 0).astype(float)  # valores nulos são definidos por '...'

    c.to_csv(df_concat, dbs_path, 'forca_trabalho.csv')
except:
    errors['Base Força de Trabalho'] = traceback.format_exc()


# # Gráfico 13.1 A
# try:
#     # importação da base de dados
#     data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
#     data = data.loc[
#         (data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
#         (data['Variável'] == 'População')
#     ].copy()

#     # encontra o último trimestre do ano mais recente
#     df = data.loc[data['Ano'] == data['Ano'].max()].copy()
#     df = df.loc[df['Trimestre'] == df['Trimestre'].max()].copy()
#     tri = df['Trimestre'].max()

#     # filtra o trimestre pelo ano mais recente e adiciona o trimestre do ano anterior
#     df_tri = data[
#         (data['Ano'].isin([data['Ano'].max(), data['Ano'].max() - 1])) &
#         (data['Trimestre'] == tri)
#         ].copy()

#     # pivoting do valor e população, para cálculo da taxa
#     df_tri.loc[
#         df_tri['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência', 'Variável'
#         ] = 'Valor'
#     df_pivoted = pd.pivot_table(df_tri, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()

#     # calculo da taxa
#     df_pivoted['Taxa'] = (df_pivoted['Valor'] / df_pivoted['População']) * 100
#     df_pivoted['Taxa'] = df_pivoted['Taxa'].replace(np.nan, 0)

#     # seleção dos estados e ranqueamento para elaboração do arquivo g13.1a
#     df_states = df_pivoted[
#         ~(df_pivoted['Região'].isin(['Brasil', 'Nordeste'])) &
#         (df_pivoted['Ano'] == df_pivoted['Ano'].max())
#     ].copy()

#     df_states['Colocação'] = df_states['Taxa'].rank(ascending=False).astype(int)

#     # seleção dos top 6
#     if 'Sergipe' in df_states[df_states['Colocação'] <= 6]['Região'].values:  # verifica se SE está entre os tops 6
#         df_filtered = df_states[df_states['Colocação'] <= 6].copy()
#     else:
#         df_filtered =df_states[(df_states['Colocação'] <= 6) | (df_states['Região'] == 'Sergipe')]  # adiciona SE e os tops 6

#     # seleção das regiões e concatenação ao df de estados ranqueados
#     df_regions = df_states = df_pivoted[
#         (df_pivoted['Região'].isin(['Brasil', 'Nordeste'])) &
#         (df_pivoted['Ano'] == df_pivoted['Ano'].max())
#     ].copy()

#     df_final = pd.concat([df_filtered, df_regions], ignore_index=True)
#     df_final.sort_values(by=['Colocação', 'Região'], inplace=True)

#     # classificação dos dados
#     if tri == 1:
#         month = '01/'
#     elif tri == 2:
#         month = '04/'
#     elif tri == 3:
#         month = '07/'
#     elif tri == 4:
#         month = '10/'

#     df_final['Variável'] = 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência'
#     df_final['Trimesetre'] = '01/' + month + df_final['Ano'].astype(str)
    
#     df_final.drop('Valor', axis='columns', inplace=True)
#     df_final.rename(columns={'Taxa': 'Valor'}, inplace=True)
#     df_final['Valor'] = df_final['Valor'].round(2)
#     df_export = df_final[['Região', 'Variável', 'Trimesetre', 'Valor', 'Colocação']].copy()
    
#     # tratamento para inclusão do símbolo de ordem
#     df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
#     df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')

#     # conversão em arquivo csv
#     c.to_excel(df_export, sheets_path, 'g13.1a.xlsx')
# except:
#     errors['Gráfico 13.1 A'] = traceback.format_exc()


# # Gráfico 13.1 B
# try:
#     # importação da base de dados
#     data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
#     data = data.loc[
#         (data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
#         (data['Variável'] == 'População')
#     ].copy()

#     # encontra o último trimestre do ano mais recente
#     df = data.loc[data['Ano'] == data['Ano'].max()].copy()
#     df = df.loc[df['Trimestre'] == df['Trimestre'].max()].copy()
#     tri = df['Trimestre'].max()

#     # filtra o trimestre pelo ano mais recente e adiciona o trimestre do ano anterior
#     df_tri = data[
#         (data['Ano'].isin([data['Ano'].max(), data['Ano'].max() - 1])) &
#         (data['Trimestre'] == tri)
#         ].copy()

#     # pivoting do valor e população, para cálculo da taxa
#     df_tri.loc[
#         df_tri['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência', 'Variável'
#         ] = 'Valor'
#     df_pivoted = pd.pivot_table(df_tri, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()

#     # calculo da taxa
#     df_pivoted['Taxa'] = (df_pivoted['Valor'] / df_pivoted['População']) * 100
#     df_pivoted['Taxa'] = df_pivoted['Taxa'].replace(np.nan, 0)

#     # pivoting do valores por ano
#     years = sorted(df_pivoted['Ano'].unique().tolist())
#     df_diff = pd.pivot_table(df_pivoted, index=['Região'], columns='Ano', values='Taxa').reset_index()

#     # cálculo da diferença e ranqueamento
#     df_diff['Diferença'] = df_diff[years[-1]] - df_diff[years[-2]]
#     df_regions = df_diff[df_diff['Região'].isin(['Brasil', 'Nordeste'])].copy()
#     df_states = df_diff[~df_diff['Região'].isin(['Brasil', 'Nordeste'])].copy()
#     df_states['Colocação'] = df_states['Diferença'].rank(ascending=False)
    
#     # filtra os top 6
#     if 'Sergipe' in df_states[df_states['Colocação'] <= 6]['Região'].values:
#         df_filtered = df_states[df_states['Colocação'] <= 6].copy()
#     else:
#         df_filtered =df_states[(df_states['Colocação'] <= 6) | (df_states['Região'] == 'Sergipe')]

#     # concatenação dos dfs
#     df_final = pd.concat([df_filtered, df_regions], ignore_index=True)
#     df_final.sort_values(by=['Colocação', 'Região'], inplace=True)

#     # seleção e renomeação de colunas
#     df_export = df_final[['Região', 'Diferença', 'Colocação']].copy()
#     df_export.rename(columns={'Diferença': 'Valor'}, inplace=True)
#     df_export['Valor'] = df_export['Valor'].round(2)
    
#     # tratamento para definição do período na variável e inclusão do símbolo de ordem
#     if tri == 1:
#         month = '01/'
#     elif tri == 2:
#         month = '04/'
#     elif tri == 3:
#         month = '07/'
#     elif tri == 4:
#         month = '10/'

#     df_export['Variável'] = f'Diferença {years[-1]}/{month[:-1]} - {years[-2]}/{month[:-1]}'
#     df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
#     df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')
#     df_export = df_export[['Região', 'Variável', 'Valor', 'Colocação']].copy()

#     # conversão em arquivo csv
#     c.to_excel(df_export, sheets_path, 'g13.1b.xlsx')
# except:
#     errors['Gráfico 13.1 B'] = traceback.format_exc()


# # Gráfico 13.2
# try:
#     # importação da base de dados
#     data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
#     data = data.loc[
#         (data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
#         (data['Variável'] == 'População')
#     ].copy()
#     df = data[data['Região'].isin(['Brasil', 'Nordeste', 'Sergipe'])].copy()

#     # pivoting do valor e população, para cálculo da taxa
#     df.loc[
#         df['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência', 'Variável'
#         ] = 'Valor'
#     df_pivoted = pd.pivot_table(df, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()

#     # calculo da taxa
#     df_pivoted['Taxa'] = (df_pivoted['Valor'] / df_pivoted['População']) * 100
#     df_pivoted['Taxa'] = df_pivoted['Taxa'].replace(np.nan, 0)

#     # concatenação do período
#     tri = {
#         1: '01',
#         2: '04',
#         3: '07',
#         4: '10'
#     }

#     df_pivoted['Month'] = df_pivoted['Trimestre'].map(tri)
#     df_pivoted['Period'] = '01/' + df_pivoted['Month'].astype(str) + '/' + df_pivoted['Ano'].astype(str)
#     df_pivoted['Variável'] = 'Taxa de pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência'
    
#     # seleção das colunas
#     df_final = df_pivoted[df_pivoted['Ano'] >= 2019][['Região', 'Variável', 'Period', 'Taxa']].copy()
#     df_final.rename(columns={'Taxa': 'Valor', 'Period': 'Trimestre'}, inplace=True)
#     df_final['Valor'] = df_final['Valor'].round(2)

#     # conversão em arquivo csv
#     c.to_excel(df_final, sheets_path, 'g13.2.xlsx')
# except:
#     errors['Gráfico 13.2'] = traceback.format_exc()


# # Gráfico 13.3
# try:
#     # importação da base de dados
#     data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
#     data = data.loc[  # filtra pelas variáveis de interesse e pelo ano mais recente
#         ((data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
#         (data['Variável'] == 'Pessoas de 14 anos ou mais de idade ocupadas na semana de referência')) &
#         (data['Ano'] == data['Ano'].max())
#     ].copy()
#     data = data[data['Trimestre'] == data['Trimestre'].max()].copy()  # filtra o trimestre mais recente

#     # renomeação das variáveis
#     data.loc[data['Variável'].str.contains('ocupadas'), 'Variável'] = 'ocupados'
#     data.loc[data['Variável'].str.contains('Pessoas'), 'Variável'] = 'na força de trabalho'

#     # pivotagem e cálculo da taxa
#     df_pivoted = pd.pivot_table(data, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()
#     df_pivoted['Taxa'] = (df_pivoted['ocupados'] / df_pivoted['na força de trabalho']) * 100

#     # ranqueamento e seleção dos top 6 e regiões
#     df_states = df_pivoted[~df_pivoted['Região'].isin(['Brasil', 'Nordeste'])].copy()
#     df_states['Colocação'] = df_states['Taxa'].rank(ascending=False)
#     df_regions = df_pivoted[df_pivoted['Região'].isin(['Brasil', 'Nordeste'])].copy()

#     if 'Sergipe' in df_states[df_states['Colocação'] <= 6]['Região'].values:
#         df_states = df_states[df_states['Colocação'] <= 6].copy()
#     else:
#         df_states = df_states[(df_states['Colocação'] <= 6) | (df_states['Região'] == 'Sergipe')].copy()

#     df_final = pd.concat([df_states, df_regions], ignore_index=True)
#     df_final.sort_values(by=['Colocação', 'Região'], inplace=True)

#     # concatenação do período
#     tri = {
#         1: '01',
#         2: '04',
#         3: '07',
#         4: '10'
#     }

#     df_final['Month'] = df_final['Trimestre'].map(tri)
#     df_final['Periodo'] = '01/' + df_final['Month'].astype(str) + '/' + df_final['Ano'].astype(str)
#     df_final['Variável'] = 'Taxa de pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência'

#     # seleção das colunas
#     df_export = df_final[['Região', 'Variável', 'Periodo', 'Taxa', 'Colocação']].copy()
#     df_export.rename(columns={'Taxa': 'Valor', 'Periodo': 'Trimestre'}, inplace=True)
#     df_export['Valor'] = df_export['Valor'].round(2)

#     # tratamento para inclusão do símbolo de ordem
#     df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
#     df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')

#     # conversão em arquivo csv
#     c.to_excel(df_export, sheets_path, 'g13.3a.xlsx')
# except:
#     errors['Gráfico 13.3 A'] = traceback.format_exc()



# # Gráfico 13.3 B
# try:
#     # importação da base de dados
#     data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
#     data = data.loc[  # filtra pelas variáveis de interesse e pelos anos mais recentes
#         (data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
#         (data['Variável'] == 'Pessoas de 14 anos ou mais de idade ocupadas na semana de referência')
#     ].copy()
    
#     # encontra o último trimestre do ano mais recente
#     df = data.loc[data['Ano'] == data['Ano'].max()].copy()
#     df = df.loc[df['Trimestre'] == df['Trimestre'].max()].copy()
#     years = sorted(data[(data['Ano'] == data['Ano'].max()) | (data['Ano'] == data['Ano'].max() - 1)]['Ano'].unique().tolist())
#     tri = df['Trimestre'].max()

#     # filtra o trimestre pelo ano mais recente e adiciona o trimestre do ano anterior
#     df = data[
#         (data['Ano'].isin(years)) &
#         (data['Trimestre'] == tri)
#         ].copy()

#     # renomeação das variáveis
#     df.loc[df['Variável'].str.contains('ocupadas'), 'Variável'] = 'ocupados'
#     df.loc[df['Variável'].str.contains('Pessoas'), 'Variável'] = 'na força de trabalho'

#     # pivotagem e cálculo da taxa
#     df_pivoted = pd.pivot_table(df, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()
#     df_pivoted['Taxa'] = (df_pivoted['ocupados'] / df_pivoted['na força de trabalho']) * 100

#     # pivotagem para cálculo da diferença
#     df_diff = pd.pivot_table(df_pivoted, index=['Região', 'Trimestre'], columns='Ano', values='Taxa').reset_index()
#     df_diff['Diferença'] = df_diff[years[-1]] - df_diff[years[-2]]

#     # ranqueamento e seleção dos top 6 e regiões
#     df_states = df_diff[~df_diff['Região'].isin(['Brasil', 'Nordeste'])].copy()
#     df_states['Colocação'] = df_states['Diferença'].rank(ascending=False)
#     df_regions = df_diff[df_diff['Região'].isin(['Brasil', 'Nordeste'])].copy()

#     if 'Sergipe' in df_states[df_states['Colocação'] <= 6]['Região'].values:
#         df_states = df_states[df_states['Colocação'] <= 6].copy()
#     else:
#         df_states = df_states[(df_states['Colocação'] <= 6) | (df_states['Região'] == 'Sergipe')].copy()

#     df_final = pd.concat([df_states, df_regions], ignore_index=True)
#     df_final.sort_values(by=['Colocação', 'Região'], inplace=True)

#     # concatenação do período
#     tri = {
#         1: '01',
#         2: '04',
#         3: '07',
#         4: '10'
#     }

#     df_final['Month'] = df_final['Trimestre'].map(tri)
#     month = df_final['Month'].max()
#     df_final['Variável'] = f'Diferença {years[-1]}/{month} - {years[-2]}/{month}'

#     # seleção das colunas
#     df_export = df_final[['Região', 'Variável', 'Diferença', 'Colocação']].copy()
#     df_export.rename(columns={'Diferença': 'Valor'}, inplace=True)
#     df_export['Valor'] = df_export['Valor'].round(2)

#     # tratamento para inclusão do símbolo de ordem
#     df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
#     df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')

#     # conversão em arquivo csv
#     c.to_excel(df_export, sheets_path, 'g13.3b.xlsx')

# except:
#     errors['Gráfico 13.3 B'] = traceback.format_exc()



# # Gráfico 13.4
# try:
#     # importação da base de dados
#     data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
#     data = data.loc[  # filtra pelas variáveis de interesse e pelos anos mais recentes
#         ((data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
#         (data['Variável'] == 'Pessoas de 14 anos ou mais de idade ocupadas na semana de referência')) &
#         (data['Região'].isin(['Brasil', 'Nordeste', 'Sergipe'])) &
#         (data['Ano'] >= 2019)
#     ].copy()

#     # renomeação das variáveis
#     data.loc[data['Variável'].str.contains('ocupadas'), 'Variável'] = 'ocupados'
#     data.loc[data['Variável'].str.contains('Pessoas'), 'Variável'] = 'na força de trabalho'

#     # pivotagem e cálculo da taxa
#     df_pivoted = pd.pivot_table(data, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()
#     df_pivoted['Taxa'] = (df_pivoted['ocupados'] / df_pivoted['na força de trabalho']) * 100
#     df_pivoted['Variável'] = 'Taxa de pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência'

#     # concatenação do período
#     tri = {
#         1: '01',
#         2: '04',
#         3: '07',
#         4: '10'
#     }

#     df_final = df_pivoted[['Região', 'Variável', 'Ano', 'Trimestre', 'Taxa']].copy()
#     df_final['Month'] = df_final['Trimestre'].map(tri)
#     df_final['Trimestre'] = '01/' + df_final['Month'].astype(str) + '/' + df_final['Ano'].astype(str)
#     df_final.sort_values(by=['Região', 'Ano', 'Month'], inplace=True)

#     # seleção das colunas
#     df_export = df_final[['Região', 'Variável', 'Trimestre', 'Taxa']].copy()
#     df_export.rename(columns={'Taxa': 'Valor'}, inplace=True)
#     df_export['Valor'] = df_export['Valor'].round(2)

#     # conversão em arquivo csv
#     c.to_excel(df_export, sheets_path, 'g13.4.xlsx')
# except:
#     errors['Gráfico 13.4'] = traceback.format_exc()


# # Gráfico 13.5
# try:
#     # importação da base de dados
#     data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
#     data = data.loc[  # filtra pelas variáveis de interesse e pelos anos mais recentes
#         (data['Variável'] == 'Taxa de desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade') &
#         (data['Ano'] == data['Ano'].max())
#     ].copy()
#     data = data[data['Trimestre'] == data['Trimestre'].max()].copy()

#     # ranqueamento e seleção dos top 6 e regiões
#     df_states = data[~data['Região'].isin(['Brasil', 'Nordeste'])].copy()
#     df_states['Colocação'] = df_states['Valor'].rank(ascending=False)
#     df_regions = data[data['Região'].isin(['Brasil', 'Nordeste'])].copy()

#     if 'Sergipe' in df_states[df_states['Colocação'] <= 6]['Região'].values:
#         df_states = df_states[df_states['Colocação'] <= 6].copy()
#     else:
#         df_states = df_states[(df_states['Colocação'] <= 6) | (df_states['Região'] == 'Sergipe')].copy()

#     df_final = pd.concat([df_states, df_regions], ignore_index=True)
#     df_final.sort_values(by=['Colocação', 'Região'], inplace=True)

#     # concatenação do período
#     tri = {
#         1: '01',
#         2: '04',
#         3: '07',
#         4: '10'
#     }

#     df_final['Month'] = df_final['Trimestre'].map(tri)
#     month = df_final['Month'].max()
#     df_final['Trimestre'] = '01/' + month +  '/' + df_final['Ano'].astype(str)

#     # seleção das colunas
#     df_export = df_final[['Região', 'Variável', 'Trimestre', 'Valor', 'Colocação']].copy()
#     df_export['Valor'] = df_export['Valor'].round(2)

#     # tratamento para inclusão do símbolo de ordem
#     df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
#     df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')

#     # conversão em arquivo csv
#     c.to_excel(df_export, sheets_path, 'g13.5a.xlsx')
# except:
#     errors['Gráfico 13.5'] = traceback.format_exc()


# Gráfico 13.5 B
try:
    # importação da base de dados
    data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
    data = data.loc[  # filtra pelas variáveis de interesse e pelos anos mais recentes
        (data['Variável'] == 'Taxa de desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade')
    ].copy()
    
    # encontra e filtra os perídos de interesse
    df = data[data['Ano'] == data['Ano'].max()].copy()
    df = df[df['Trimestre'] == df['Trimestre'].max()].copy()
    data = data[
        ((data['Ano'] == data['Ano'].max()) | (data['Ano'] == data['Ano'].max() - 1)) &
        (data['Trimestre'] == df['Trimestre'].max())
        ].copy()
    years = sorted(data['Ano'].unique())

    # pivotagem e cálculo da taxa
    df_pivoted = pd.pivot_table(data, index=['Região', 'Variável', 'Trimestre'], columns='Ano', values='Valor').reset_index()
    df_pivoted['Taxa'] = (df_pivoted[years[-1]] / df_pivoted[years[-2]])

    # ranqueamento e seleção dos top 6 e regiões
    df_states = df_pivoted[~df_pivoted['Região'].isin(['Brasil', 'Nordeste'])].copy()
    df_states['Colocação'] = df_states['Taxa'].rank(ascending=False)
    df_regions = df_pivoted[df_pivoted['Região'].isin(['Brasil', 'Nordeste'])].copy()

    if 'Sergipe' in df_states[df_states['Colocação'] <= 6]['Região'].values:
        df_states = df_states[df_states['Colocação'] <= 6].copy()
    else:
        df_states = df_states[(df_states['Colocação'] <= 6) | (df_states['Região'] == 'Sergipe')].copy()

    df_final = pd.concat([df_states, df_regions], ignore_index=True)
    df_final.sort_values(by=['Colocação', 'Região'], inplace=True)

    # concatenação do período
    tri = {
        1: '01',
        2: '04',
        3: '07',
        4: '10'
    }

    df_final['Month'] = df_final['Trimestre'].map(tri)
    month = df_final['Month'].max()

    # seleção das colunas
    df_export = df_final[['Região', 'Variável', 'Taxa', 'Colocação']].copy()
    df_export['Variável'] = f'Diferença {years[-1]}/{month} - {years[-1]}/{month}'
    df_export.rename(columns={'Taxa': 'Valor'}, inplace=True)
    df_export['Valor'] = df_export['Valor'].round(2)

    # tratamento para inclusão do símbolo de ordem
    df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
    df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')

    # conversão em arquivo csv
    c.to_excel(df_export, sheets_path, 'g13.5b.xlsx')
except:
    errors['Gráfico 13.5 B'] = traceback.format_exc()




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





# # Planilha de Indicadores Sociais
# try:
#     # projeção da taxa
#     url = 'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Indicadores_Sociais/Sintese_de_Indicadores_Sociais'
#     response = c.open_url(url)
#     df = pd.DataFrame(response.json())

#     df = df.loc[df['name'].str.startswith('Sintese_de_Indicadores_Sociais_2'),
#                 ['name', 'path']].sort_values(by='name', ascending=False).reset_index(drop=True)

#     url_to_get = df['path'][0].split('/')[-1]
#     response = c.open_url(url + '/' + url_to_get + '/Tabelas/xls')
#     df = pd.DataFrame(response.json())
#     url_to_get_pib = df.loc[df['name'].str.startswith('2_'), 'url'].values[0]

#     file = c.open_url(url_to_get_pib)
# except:
#     errors['Planilha de Indicadores Sociais'] = traceback.format_exc()


# # Gráfico 16.4
# try:
#     # importação dos indicadores
#     df = c.open_file(file_path=file.content, ext='zip', excel_name='', skiprows=6, sheet_name='4) INDICADORES')
# except:
#     errors['Gráfico 16.4'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
file_name = 'script--g13.1a--ate-t13.3.txt'
if errors:
    with open(os.path.join(errors_path, file_name), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
