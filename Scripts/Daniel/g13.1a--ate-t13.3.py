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


'''
Este script deve ser configurado para ser executado mensalmente
As figuras g13.9a, g13.9b, g13.10 e t13.3 não foram programadas por conta do site estar fora do ar
'''


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************


# Gráfico 13.1a até Tabela 13.1
try:
    # looping de requisições para cada tabela da figura
    dbs = []
    for key, table in {
        'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência': {
            'tb': '6402', 'var': '1641,4088,4090,4092,4094,4099', 'class': {'86': '95251'}
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


# ************************
# ELABORAÇÃO DAS PLANILHAS
# ************************


# Gráfico 13.1 A
try:
    # importação da base de dados
    data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
    data = data.loc[
        (data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
        (data['Variável'] == 'População')
    ].copy()

    # encontra o último trimestre do ano mais recente
    df = data.loc[data['Ano'] == data['Ano'].max()].copy()
    df = df.loc[df['Trimestre'] == df['Trimestre'].max()].copy()
    tri = df['Trimestre'].max()

    # filtra o trimestre pelo ano mais recente e adiciona o trimestre do ano anterior
    df_tri = data[
        (data['Ano'].isin([data['Ano'].max(), data['Ano'].max() - 1])) &
        (data['Trimestre'] == tri)
        ].copy()

    # pivoting do valor e população, para cálculo da taxa
    df_tri.loc[
        df_tri['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência', 'Variável'
        ] = 'Valor'
    df_pivoted = pd.pivot_table(df_tri, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()

    # calculo da taxa
    df_pivoted['Taxa'] = (df_pivoted['Valor'] / df_pivoted['População']) * 100
    df_pivoted['Taxa'] = df_pivoted['Taxa'].replace(np.nan, 0)

    # seleção dos estados e ranqueamento para elaboração do arquivo g13.1a
    df_states = df_pivoted[
        ~(df_pivoted['Região'].isin(['Brasil', 'Nordeste'])) &
        (df_pivoted['Ano'] == df_pivoted['Ano'].max())
    ].copy()

    df_states['Colocação'] = df_states['Taxa'].rank(ascending=False).astype(int)

    # seleção dos top 6
    if 'Sergipe' in df_states[df_states['Colocação'] <= 6]['Região'].values:  # verifica se SE está entre os tops 6
        df_filtered = df_states[df_states['Colocação'] <= 6].copy()
    else:
        df_filtered =df_states[(df_states['Colocação'] <= 6) | (df_states['Região'] == 'Sergipe')]  # adiciona SE e os tops 6

    # seleção das regiões e concatenação ao df de estados ranqueados
    df_regions = df_states = df_pivoted[
        (df_pivoted['Região'].isin(['Brasil', 'Nordeste'])) &
        (df_pivoted['Ano'] == df_pivoted['Ano'].max())
    ].copy()

    df_final = pd.concat([df_filtered, df_regions], ignore_index=True)
    df_final.sort_values(by=['Colocação', 'Região'], inplace=True)

    # classificação dos dados
    if tri == 1:
        month = '01/'
    elif tri == 2:
        month = '04/'
    elif tri == 3:
        month = '07/'
    elif tri == 4:
        month = '10/'

    df_final['Variável'] = 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência'
    df_final['Trimesetre'] = '01/' + month + df_final['Ano'].astype(str)
    
    df_final.drop('Valor', axis='columns', inplace=True)
    df_final.rename(columns={'Taxa': 'Valor'}, inplace=True)
    df_final['Valor'] = df_final['Valor'].round(2)
    df_export = df_final[['Região', 'Variável', 'Trimesetre', 'Valor', 'Colocação']].copy()
    
    # tratamento para inclusão do símbolo de ordem
    df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
    df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')

    # conversão em arquivo csv
    c.to_excel(df_export, sheets_path, 'g13.1a.xlsx')
except:
    errors['Gráfico 13.1 A'] = traceback.format_exc()


# Gráfico 13.1 B
try:
    # importação da base de dados
    data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
    data = data.loc[
        (data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
        (data['Variável'] == 'População')
    ].copy()

    # encontra o último trimestre do ano mais recente
    df = data.loc[data['Ano'] == data['Ano'].max()].copy()
    df = df.loc[df['Trimestre'] == df['Trimestre'].max()].copy()
    tri = df['Trimestre'].max()

    # filtra o trimestre pelo ano mais recente e adiciona o trimestre do ano anterior
    df_tri = data[
        (data['Ano'].isin([data['Ano'].max(), data['Ano'].max() - 1])) &
        (data['Trimestre'] == tri)
        ].copy()

    # pivoting do valor e população, para cálculo da taxa
    df_tri.loc[
        df_tri['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência', 'Variável'
        ] = 'Valor'
    df_pivoted = pd.pivot_table(df_tri, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()

    # calculo da taxa
    df_pivoted['Taxa'] = (df_pivoted['Valor'] / df_pivoted['População']) * 100
    df_pivoted['Taxa'] = df_pivoted['Taxa'].replace(np.nan, 0)

    # pivoting do valores por ano
    years = sorted(df_pivoted['Ano'].unique().tolist())
    df_diff = pd.pivot_table(df_pivoted, index=['Região'], columns='Ano', values='Taxa').reset_index()

    # cálculo da diferença e ranqueamento
    df_diff['Diferença'] = df_diff[years[-1]] - df_diff[years[-2]]
    df_regions = df_diff[df_diff['Região'].isin(['Brasil', 'Nordeste'])].copy()
    df_states = df_diff[~df_diff['Região'].isin(['Brasil', 'Nordeste'])].copy()
    df_states['Colocação'] = df_states['Diferença'].rank(ascending=False)
    
    # filtra os top 6
    if 'Sergipe' in df_states[df_states['Colocação'] <= 6]['Região'].values:
        df_filtered = df_states[df_states['Colocação'] <= 6].copy()
    else:
        df_filtered =df_states[(df_states['Colocação'] <= 6) | (df_states['Região'] == 'Sergipe')]

    # concatenação dos dfs
    df_final = pd.concat([df_filtered, df_regions], ignore_index=True)
    df_final.sort_values(by=['Colocação', 'Região'], inplace=True)

    # seleção e renomeação de colunas
    df_export = df_final[['Região', 'Diferença', 'Colocação']].copy()
    df_export.rename(columns={'Diferença': 'Valor'}, inplace=True)
    df_export['Valor'] = df_export['Valor'].round(2)
    
    # tratamento para definição do período na variável e inclusão do símbolo de ordem
    if tri == 1:
        month = '01/'
    elif tri == 2:
        month = '04/'
    elif tri == 3:
        month = '07/'
    elif tri == 4:
        month = '10/'

    df_export['Variável'] = f'Diferença {years[-1]}/{month[:-1]} - {years[-2]}/{month[:-1]}'
    df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
    df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')
    df_export = df_export[['Região', 'Variável', 'Valor', 'Colocação']].copy()

    # conversão em arquivo csv
    c.to_excel(df_export, sheets_path, 'g13.1b.xlsx')
except:
    errors['Gráfico 13.1 B'] = traceback.format_exc()


# Gráfico 13.2
try:
    # importação da base de dados
    data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
    data = data.loc[
        (data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
        (data['Variável'] == 'População')
    ].copy()
    df = data[data['Região'].isin(['Brasil', 'Nordeste', 'Sergipe'])].copy()

    # pivoting do valor e população, para cálculo da taxa
    df.loc[
        df['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência', 'Variável'
        ] = 'Valor'
    df_pivoted = pd.pivot_table(df, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()

    # calculo da taxa
    df_pivoted['Taxa'] = (df_pivoted['Valor'] / df_pivoted['População']) * 100
    df_pivoted['Taxa'] = df_pivoted['Taxa'].replace(np.nan, 0)

    # concatenação do período
    tri = {
        1: '01',
        2: '04',
        3: '07',
        4: '10'
    }

    df_pivoted['Month'] = df_pivoted['Trimestre'].map(tri)
    df_pivoted['Period'] = '01/' + df_pivoted['Month'].astype(str) + '/' + df_pivoted['Ano'].astype(str)
    df_pivoted['Variável'] = 'Taxa de pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência'
    
    # seleção das colunas
    df_final = df_pivoted[df_pivoted['Ano'] >= 2019][['Região', 'Variável', 'Period', 'Taxa']].copy()
    df_final.rename(columns={'Taxa': 'Valor', 'Period': 'Trimestre'}, inplace=True)
    df_final['Valor'] = df_final['Valor'].round(2)

    # conversão em arquivo csv
    c.to_excel(df_final, sheets_path, 'g13.2.xlsx')
except:
    errors['Gráfico 13.2'] = traceback.format_exc()


# Gráfico 13.3
try:
    # importação da base de dados
    data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
    data = data.loc[  # filtra pelas variáveis de interesse e pelo ano mais recente
        ((data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
        (data['Variável'] == 'Pessoas de 14 anos ou mais de idade ocupadas na semana de referência')) &
        (data['Ano'] == data['Ano'].max())
    ].copy()
    data = data[data['Trimestre'] == data['Trimestre'].max()].copy()  # filtra o trimestre mais recente

    # renomeação das variáveis
    data.loc[data['Variável'].str.contains('ocupadas'), 'Variável'] = 'ocupados'
    data.loc[data['Variável'].str.contains('Pessoas'), 'Variável'] = 'na força de trabalho'

    # pivotagem e cálculo da taxa
    df_pivoted = pd.pivot_table(data, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()
    df_pivoted['Taxa'] = (df_pivoted['ocupados'] / df_pivoted['na força de trabalho']) * 100

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
    df_final['Periodo'] = '01/' + df_final['Month'].astype(str) + '/' + df_final['Ano'].astype(str)
    df_final['Variável'] = 'Taxa de pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência'

    # seleção das colunas
    df_export = df_final[['Região', 'Variável', 'Periodo', 'Taxa', 'Colocação']].copy()
    df_export.rename(columns={'Taxa': 'Valor', 'Periodo': 'Trimestre'}, inplace=True)
    df_export['Valor'] = df_export['Valor'].round(2)

    # tratamento para inclusão do símbolo de ordem
    df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
    df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')

    # conversão em arquivo csv
    c.to_excel(df_export, sheets_path, 'g13.3a.xlsx')
except:
    errors['Gráfico 13.3 A'] = traceback.format_exc()



# Gráfico 13.3 B
try:
    # importação da base de dados
    data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
    data = data.loc[  # filtra pelas variáveis de interesse e pelos anos mais recentes
        (data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
        (data['Variável'] == 'Pessoas de 14 anos ou mais de idade ocupadas na semana de referência')
    ].copy()
    
    # encontra o último trimestre do ano mais recente
    df = data.loc[data['Ano'] == data['Ano'].max()].copy()
    df = df.loc[df['Trimestre'] == df['Trimestre'].max()].copy()
    years = sorted(data[(data['Ano'] == data['Ano'].max()) | (data['Ano'] == data['Ano'].max() - 1)]['Ano'].unique().tolist())
    tri = df['Trimestre'].max()

    # filtra o trimestre pelo ano mais recente e adiciona o trimestre do ano anterior
    df = data[
        (data['Ano'].isin(years)) &
        (data['Trimestre'] == tri)
        ].copy()

    # renomeação das variáveis
    df.loc[df['Variável'].str.contains('ocupadas'), 'Variável'] = 'ocupados'
    df.loc[df['Variável'].str.contains('Pessoas'), 'Variável'] = 'na força de trabalho'

    # pivotagem e cálculo da taxa
    df_pivoted = pd.pivot_table(df, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()
    df_pivoted['Taxa'] = (df_pivoted['ocupados'] / df_pivoted['na força de trabalho']) * 100

    # pivotagem para cálculo da diferença
    df_diff = pd.pivot_table(df_pivoted, index=['Região', 'Trimestre'], columns='Ano', values='Taxa').reset_index()
    df_diff['Diferença'] = df_diff[years[-1]] - df_diff[years[-2]]

    # ranqueamento e seleção dos top 6 e regiões
    df_states = df_diff[~df_diff['Região'].isin(['Brasil', 'Nordeste'])].copy()
    df_states['Colocação'] = df_states['Diferença'].rank(ascending=False)
    df_regions = df_diff[df_diff['Região'].isin(['Brasil', 'Nordeste'])].copy()

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
    df_final['Variável'] = f'Diferença {years[-1]}/{month} - {years[-2]}/{month}'

    # seleção das colunas
    df_export = df_final[['Região', 'Variável', 'Diferença', 'Colocação']].copy()
    df_export.rename(columns={'Diferença': 'Valor'}, inplace=True)
    df_export['Valor'] = df_export['Valor'].round(2)

    # tratamento para inclusão do símbolo de ordem
    df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
    df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')

    # conversão em arquivo csv
    c.to_excel(df_export, sheets_path, 'g13.3b.xlsx')

except:
    errors['Gráfico 13.3 B'] = traceback.format_exc()



# Gráfico 13.4
try:
    # importação da base de dados
    data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
    data = data.loc[  # filtra pelas variáveis de interesse e pelos anos mais recentes
        ((data['Variável'] == 'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência') |
        (data['Variável'] == 'Pessoas de 14 anos ou mais de idade ocupadas na semana de referência')) &
        (data['Região'].isin(['Brasil', 'Nordeste', 'Sergipe'])) &
        (data['Ano'] >= 2019)
    ].copy()

    # renomeação das variáveis
    data.loc[data['Variável'].str.contains('ocupadas'), 'Variável'] = 'ocupados'
    data.loc[data['Variável'].str.contains('Pessoas'), 'Variável'] = 'na força de trabalho'

    # pivotagem e cálculo da taxa
    df_pivoted = pd.pivot_table(data, index=['Região', 'Ano', 'Trimestre'], columns='Variável', values='Valor').reset_index()
    df_pivoted['Taxa'] = (df_pivoted['ocupados'] / df_pivoted['na força de trabalho']) * 100
    df_pivoted['Variável'] = 'Taxa de pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência'

    # concatenação do período
    tri = {
        1: '01',
        2: '04',
        3: '07',
        4: '10'
    }

    df_final = df_pivoted[['Região', 'Variável', 'Ano', 'Trimestre', 'Taxa']].copy()
    df_final['Month'] = df_final['Trimestre'].map(tri)
    df_final['Trimestre'] = '01/' + df_final['Month'].astype(str) + '/' + df_final['Ano'].astype(str)
    df_final.sort_values(by=['Região', 'Ano', 'Month'], inplace=True)

    # seleção das colunas
    df_export = df_final[['Região', 'Variável', 'Trimestre', 'Taxa']].copy()
    df_export.rename(columns={'Taxa': 'Valor'}, inplace=True)
    df_export['Valor'] = df_export['Valor'].round(2)

    # conversão em arquivo csv
    c.to_excel(df_export, sheets_path, 'g13.4.xlsx')
except:
    errors['Gráfico 13.4'] = traceback.format_exc()


# Gráfico 13.5
try:
    # importação da base de dados
    data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
    data = data.loc[  # filtra pelas variáveis de interesse e pelos anos mais recentes
        (data['Variável'] == 'Taxa de desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade') &
        (data['Ano'] == data['Ano'].max())
    ].copy()
    data = data[data['Trimestre'] == data['Trimestre'].max()].copy()

    # ranqueamento e seleção dos top 6 e regiões
    df_states = data[~data['Região'].isin(['Brasil', 'Nordeste'])].copy()
    df_states['Colocação'] = df_states['Valor'].rank(ascending=False)
    df_regions = data[data['Região'].isin(['Brasil', 'Nordeste'])].copy()

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
    df_final['Trimestre'] = '01/' + month +  '/' + df_final['Ano'].astype(str)

    # seleção das colunas
    df_export = df_final[['Região', 'Variável', 'Trimestre', 'Valor', 'Colocação']].copy()
    df_export['Valor'] = df_export['Valor'].round(2)

    # tratamento para inclusão do símbolo de ordem
    df_export['Colocação'] = df_export['Colocação'].fillna(0.0).astype(int)
    df_export['Colocação'] = df_export['Colocação'].apply(lambda x: str(x) + 'º' if x != 0 else '')

    # conversão em arquivo csv
    c.to_excel(df_export, sheets_path, 'g13.5a.xlsx')
except:
    errors['Gráfico 13.5'] = traceback.format_exc()


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


# Gráfico 13.6
try:
    # importação da base de dados
    data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')
    data = data.loc[  # filtra pelas variáveis de interesse e pelos anos mais recentes
        (data['Variável'] == 'Taxa de desocupação, na semana de referência, das pessoas de 14 anos ou mais de idade') &
        (data['Trimestre'] == 4) &
        (data['Região'].isin(['Brasil', 'Nordeste', 'Sergipe']))
    ].copy()

    # concatenação do trimestre
    tri = {
        1: '01',
        2: '04',
        3: '07',
        4: '10'
    }

    data['Month'] = data['Trimestre'].map(tri)
    data['Trimestre'] = '01/' + data['Month'].max() + '/' + data['Ano'].astype(str)

    # seleção das colunas
    df = data[['Região', 'Variável', 'Trimestre', 'Valor']].copy()

    # conversão em arquivo csv
    c.to_excel(df, sheets_path, 'g13.6.xlsx')
except:
    errors['Gráfico 13.6'] = traceback.format_exc()


# Tabela 13.1
try:
    # importação da base de dados
    data = c.open_file(dbs_path, 'forca_trabalho.csv', 'csv')

    # encontra trimestre mais recente do ano mais recente
    df = data.loc[data['Ano'] == data['Ano'].max()].copy()
    df = df.loc[df['Trimestre'] == df['Trimestre'].max()].copy()
    tri = df['Trimestre'].max()

    # filtra pelas variáveis e períodos de interesse e faz mapeamento
    variable_to_category = {
        'Pessoas de 14 anos ou mais de idade': 'Categoria 1',
        'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência': 'Categoria 2',
        'Pessoas de 14 anos ou mais de idade ocupadas na semana de referência': 'Categoria 3',
        'Pessoas de 14 anos ou mais de idade, desocupadas na semana de referência': 'Categoria 4',
        'Pessoas de 14 anos ou mais de idade, fora da força de trabalho, na semana de referência': 'Categoria 5',
        'População': 'Categoria 6'
    }

    category_to_variable = {v: k for k, v in variable_to_category.items()}

    df = data.loc[
        (data['Variável'].isin(variable_to_category.keys())) &
        (data['Trimestre'] == tri) &
        (data['Região'].isin(['Brasil', 'Nordeste', 'Sergipe']))
    ].copy()

    df['Categoria'] = df['Variável'].map(variable_to_category)

    # seleciona os anos em intervalos de 2
    years = df['Ano'].unique()
    max_year = years.max()
    [year - 2 for year in years]
    selected_years = [max_year - 2 * i for i in range((max_year - years.min()) // 2 + 1)]

    # pivotagem e cálculo da taxa
    df_pivoted = pd.pivot_table(
        df[df['Ano'].isin(selected_years)],
        index=['Região', 'Ano', 'Trimestre'],
        columns='Categoria',
        values='Valor'
        ).reset_index()
    
    for i, col in enumerate(df_pivoted.columns):
        if col not in ['Região', 'Ano', 'Trimestre', 'Categoria 6']:
            df_pivoted[category_to_variable[col]] = (df_pivoted[col] / df_pivoted['Categoria 6']) * 100
            df_pivoted[category_to_variable[col]] = df_pivoted[category_to_variable[col]].replace(np.nan, 0)
    
    # melting das colunas
    df_melted = pd.melt(
        df_pivoted[[col for col in df_pivoted.columns if not col.startswith('Categoria')]],
        id_vars=['Região', 'Ano', 'Trimestre'],
        var_name='Variável', value_name='Valor'
    )

    # concatenação do trimestre
    tri = {
        1: '01',
        2: '04',
        3: '07',
        4: '10'
    }

    df_melted['Month'] = df_melted['Trimestre'].map(tri)
    df_melted['Trimestre'] = '01/' + df_melted['Month'].max() + '/' + df_melted['Ano'].astype(str)

    # seleção das colunas
    df = df_melted[['Região', 'Variável', 'Trimestre', 'Valor']].copy()
    df.sort_values(by=['Região', 'Variável', 'Trimestre'], inplace=True)

    # conversão em arquivo csv
    c.to_excel(df, sheets_path, 't13.1.xlsx')
except:
    errors['Tabela 13.1'] = traceback.format_exc()


# Gráfico 13.7
try:
    # download dos dados
    data = sidrapy.get_table(
        table_code='4097',
        territorial_level='3',ibge_territorial_code='28',
        variable='4108',
        classifications={'11913': '31722,31723,31725,31726,31727,31731,96170,96171'},
        period="all"
    )

    # remoção da linha 0, dados para serem usados como rótulos das colunas
    data.drop(0, axis='index', inplace=True)

    # seleção das colunas e linhas de interesse e tratamentos básicos
    df = data[['D1N', 'D3N', 'D4N', 'D2N', 'V']].copy()
    df.columns = ['Região', 'Variável', 'Classe', 'Ano', 'Valor']
    df['Trimestre'] = df['Ano'].apply(lambda x: x[0]).astype(int)
    df['Ano'] = df['Ano'].apply(lambda x: x.split(' ')[-1]).astype(int)
    df['Valor'] = df['Valor'].replace('...', 0).astype(float)  # valores nulos são definidos por '...'
    df = df[df['Ano'] >= 2019].copy()

    # agrupamento das variáveis
    df_formal = df[df['Classe'].str.contains('com carteira de trabalho')].copy()
    df_informal = df[df['Classe'].str.contains('sem carteira de trabalho')].copy()
    df_others = df[
        (~df['Classe'].str.contains('sem carteira de trabalho')) &
        (~df['Classe'].str.contains('com carteira de trabalho'))
    ].copy()

    df_formal['Classe'] = 'Setor privado com carteira'
    df_informal['Classe'] = 'Setor privado sem carteira'
    df_others.loc[df_others['Classe'] == 'Empregado no setor público', 'Classe'] = 'Setor público'

    df_formal_grouped = df_formal.groupby(['Região', 'Variável', 'Classe', 'Trimestre', 'Ano'])['Valor'].sum().reset_index()
    df_informal_grouped = df_informal.groupby(['Região', 'Variável', 'Classe', 'Trimestre', 'Ano'])['Valor'].sum().reset_index()

    df_concat = pd.concat([df_formal_grouped, df_informal_grouped, df_others], ignore_index=True)
    df_concat.sort_values(by=['Região', 'Classe', 'Ano', 'Trimestre'], inplace=True)

    # concatenação do trimestre
    tri = {
        1: '01',
        2: '04',
        3: '07',
        4: '10'
    }

    df_concat['Month'] = df_concat['Trimestre'].map(tri)
    df_concat['Trimestre'] = '01/' + df_concat['Month'] + '/' + df_concat['Ano'].astype(str)

    # seleção das colunas
    df_export = df_concat[['Região', 'Classe', 'Trimestre', 'Valor']].copy()
    df_export.rename(columns={'Classe': 'Variável'}, inplace=True)
    df_export['Valor'] = df_export['Valor'].round(2)
    # df_export.sort_values(by=['Região', 'Variável', 'Trimestre'], inplace=True)


    # conversão em arquivo csv
    c.to_excel(df_export, sheets_path, 'g13.7.xlsx')

except Exception as e:
    errors['Gráfico 13.7'] = traceback.format_exc()


# Gráfico 13.8
try:
    # projeção da taxa
    url = 'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Indicadores_Sociais/Sintese_de_Indicadores_Sociais'
    response = c.open_url(url)
    df = pd.DataFrame(response.json())

    df = df.loc[df['name'].str.startswith('Sintese_de_Indicadores_Sociais_2'),
                ['name', 'path']].sort_values(by='name', ascending=False).reset_index(drop=True)

    url_to_get = df['path'][0].split('/')[-1]
    response = c.open_url(url + '/' + url_to_get + '/Tabelas/xls')
    df = pd.DataFrame(response.json())
    url_to_get_pib = df.loc[df['name'].str.startswith('1_'), 'url'].values[0]

    file = c.open_url(url_to_get_pib)

    # importação dos indicadores
    df = c.open_file(file_path=file.content, ext='zip', excel_name='Tabela 1.42', skiprows=6)

    dfs = []
    for tb in df.keys():
        if '(CV)' not in tb:
            df_tb = df[tb].iloc[:, [0, 6, 7, 8, 9]]  # cópia de cada aba da planilha
            df_tb = df_tb[df_tb['Unnamed: 0'] == 'Sergipe'].copy()
            df_tb.dropna(how='all', inplace=True)
            df_tb.columns = ['Região'] + [col[:-2] for col in df_tb.columns if col != 'Unnamed: 0']
            df_tb['Ano'] = tb

            dfs.append(df_tb)

    df_concat = pd.concat(dfs, ignore_index=True)

    # melting das colunas
    df_melted = pd.melt(df_concat, id_vars=['Região', 'Ano'], var_name='Variável', value_name='Valor')
    df_melted['Ano'] = '31/12/' + df_melted['Ano']
    df_melted.loc[df_melted['Variável'] == 'Estuda e está ocupado', 'Variável'] = 'Estuda e trabalha'
    df_melted.loc[df_melted['Variável'] == 'Só está ocupado', 'Variável'] = 'Só trabalha'
    df_melted.loc[df_melted['Variável'] == 'Não estuda e não está ocupado', 'Variável'] = 'Não estuda e não trabalha'

    # seleção das colunas
    df_export = df_melted[['Região', 'Variável', 'Ano', 'Valor']].copy()
    df_export.sort_values(by=['Região', 'Variável', 'Ano'], inplace=True)

    # conversão em arquivo csv
    c.to_excel(df_export, sheets_path, 'g13.8.xlsx')

except:
    errors['Gráfico 13.8'] = traceback.format_exc()


# Tabela 13.2
try:
    data = sidrapy.get_table(
        table_code='5434',
        territorial_level='3', ibge_territorial_code='28',
        variable='4090,4108',
        classifications={'888': '47947,47948,47949,47950,56622,56623,56624,60032'},
        period="all"
    )

    # remoção da linha 0, dados para serem usados como rótulos das colunas
    # não foram usados porque variam de acordo com a tabela
    # seleção das colunas de interesse
    data.drop(0, axis='index', inplace=True)
    data = data[['MN', 'D1N', 'D2N', 'D3N', 'D4N', 'V']]

    # separação de valores; valores inteiros e percentuais estão armazenados na mesma coluna
    data_ab = data.loc[data['MN'] == 'Mil pessoas']
    data_per = data.loc[data['MN'] == '%']
    data = data_ab.iloc[:, 1:]
    data['Percentual'] = data_per.loc[:, 'V'].to_list()

    # renomeação das colunas
    # filtragem de dados referentes ao 4º trimestre de cada ano
    data.columns = ['Região', 'Trimestre', 'Classe', 'Variável', 'Pessoas', 'Percentual']
    data = data.loc[data['Trimestre'].str.startswith('4º trimestre')].copy()
    data['Trimestre'] = data['Trimestre'].apply(lambda x: '31/10/' + x[-4:])
    data = data[['Região', 'Variável', 'Trimestre', 'Pessoas', 'Percentual']]

    # classificação dos dados
    data['Pessoas'] = data['Pessoas'].astype(int)
    data['Percentual'] = data['Percentual'].astype(float)

    # conversão em arquivo csv
    c.to_excel(data, sheets_path, 't13.2.xlsx')

except Exception as e:
    errors['Tabela 13.2'] = traceback.format_exc()

'''
SCRIPT INCOMPLETO POR CONTA DA FALTA DE ACESSO AO SISTEMA
'''
# # gráfico 13.9
# url = 'http://bi.mte.gov.br/bgcaged/login.php'
# caged_login = c.caged_login
# caged_password = c.caged_password

# # tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
# try:
#     driver = c.Google(visible=True, rep=dbs_path)  # instância do objeto driver do Selenium
#     driver.get(url)  # acessa a página

#     # preenche o formulário de acesso
#     login_form = driver.get_tag('/html/body/center/div/div[3]/form/div/input[1]')
#     login_form.send_keys(caged_login)

#     password_form = driver.get_tag('/html/body/center/div/div[3]/form/div/input[2]')
#     password_form.send_keys(caged_password)

#     # clica no botão de login
#     driver.click('/html/body/center/div/div[3]/form/div/input[3]')

#     # configurações da tabela
#     driver.click([
#         # Opção CAGED do menu lateral esquerdo
#         '/html/body/table/tbody/tr/td/table/tbody/tr[2]/td/font/table/tbody/tr/td[1]/table/tbody/tr/td/font/table/tbody/tr/td/div/table/tbody/tr[1]/td/a',
#         # Opção CAGED estatístico do menu central
#         '/html/body/table/tbody/tr/td/table/tbody/tr[2]/td/font/table/tbody/tr/td[2]/table/tbody/tr[1]/td/table/tbody/tr[2]/td/table/tbody/tr/td/div[1]',
#         # Opção tabelas do menu central
#         '/html/body/table/tbody/tr/td/table/tbody/tr[2]/td/font/table/tbody/tr/td[2]/table/tbody/tr[1]/td/table/tbody/tr[2]/td/table/tbody/tr/td/div[2]/table/tbody/tr[2]/td/ul/li/font/a',
#     ])

#     # altera para o iframe "lista", que armazena os elementos do menu lateral esquerdo, e abre o menu Estrutura
#     driver.switch_iframe('lista')
#     driver.click('/html/body/div[2]/center/table/tbody/tr[2]/td/table/tbody/tr[2]/td')

#     # volta para o contexto inicial
#     driver.switch_to_default_content()

#     # altera para o iframe "principal", que armazena os elementos do menu central, para seleção das opções da tabela
#     driver.switch_iframe('principal')

#     # seleciona as opções de linha da tabela
#     options = driver.get_tag('/html/body/form[2]/center/table[1]/tbody/tr[1]/td/select[1]')
#     driver.select(options, 'UF')

#     # seleciona as opções de coluna da tabela
#     options = driver.get_tag('/html/body/form[2]/center/table[1]/tbody/tr[2]/td/select[1]')
#     driver.select(options, 'Ano Declarado')

#     # seleciona as opções de conteúdo da tabela
#     options = driver.get_tag('/html/body/form[2]/center/table[1]/tbody/tr[6]/td/select')
#     driver.select(options, 'Saldo Mov')

#     # volta para o contexto inicial e abre o menu Seleções aceleradoras
#     driver.switch_to_default_content()
#     driver.switch_iframe('lista')
#     driver.click('/html/body/div[2]/center/table/tbody/tr[2]/td/table/tbody/tr[1]/td')
#     driver.switch_to_default_content()

#     # seleciona os períodos de interesse
#     driver.switch_iframe('principal')
#     options = driver.get_tag('/html/body/form[2]/center/form[1]/center/table/tbody/tr[3]/td/select')
#     first_option = driver.get_tag('/html/body/form[2]/center/form[1]/center/table/tbody/tr[3]/td/select/option[1]')
#     last_option = driver.get_tag('/html/body/form[2]/center/form[1]/center/table/tbody/tr[3]/td/select/option[96]')
#     excedent = driver.get_tag('/html/body/form[2]/center/form[1]/center/table/tbody/tr[3]/td/select/option[97]')  # ano a mais do necessário para evitar erro de visibildidade
#     # last_option = driver.get_tag('/html/body/form[2]/center/form[1]/center/table/tbody/tr[3]/td/select/option[12]')
#     # excedent = driver.get_tag('/html/body/form[2]/center/form[1]/center/table/tbody/tr[3]/td/select/option[13]')
#     driver.select_with_shift(options, first_option, last_option, excedent)
#     driver.switch_to_default_content()

#     # abre a tabelas
#     driver.switch_iframe('iFrm')
#     driver.click('/html/body/table/tbody/tr/td[2]/a[2]')
#     driver.switch_to_default_content()

#     # altera para o iframe "botoes", que armazena os elementos do menu horizontal, para download da tabela
#     driver.switch_iframe('botoes')
#     driver.click("/html[1]/body[1]/span[@id='xbotoes']/table[1]/tbody[1]/tr[1]/td[2]/span[@id='botoesnaoimpressao']/img[13]")
#     sleep(3)
#     print('Download da base de dados CAGED concluído!')

#     driver.quit()
# # registro do erro em caso de atualizações
# except Exception as e:
#     errors[url + ' (CAGED OLD)'] = traceback.format_exc()



# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
file_name = 'script--g13.1a--ate-t13.3.txt'
if errors:
    with open(os.path.join(errors_path, file_name), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
