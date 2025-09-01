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

# Planilha de Indicadores Sociais
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
    url_to_get_pib = df.loc[df['name'].str.startswith('2_'), 'url'].values[0]

    file = c.open_url(url_to_get_pib)
except:
    errors['Planilha de Indicadores Sociais'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# Gráfico 20.5
try:
    # importação dos indicadores
    df = c.open_file(file_path=file.content, ext='zip', excel_name='Tabela 2.18', skiprows=5)
    df_last_year = df[list(df.keys())[0]]
    df_fisrt_year = df[list(df.keys())[-2]]
    
    dfs = []
    for i, tb in enumerate([df_last_year, df_fisrt_year]):
        df_tb = tb.copy()
        df_tb.columns = ['uf', 'total', '< 2,15', '< 3,65', '< 6,85', 'mediana nacional', 'mediana regional', 'tanto faz']
        df_tb.dropna(how='any', inplace=True)
        df_tb['ano'] = int(list(df.keys())[0]) if i == 0 else int(list(df.keys())[-2])

        dfs.append(df_tb.query('uf.isin(@c.mapping_states_abbreviation.keys())')[['uf', 'ano', '< 6,85']])

    df_concat = pd.concat(dfs, ignore_index=True)

    # tabela a
    df_a = df_concat.query('ano == @df_concat.ano.max()').copy()
    df_a['valor'] = df_a['< 6,85']
    df_a.loc[df_a['uf'].isin(['Brasil', 'Nordeste']), 'valor'] = np.nan
    df_a['rank'] = df_a['valor'].rank(method='first', ascending=False)
    df_a.sort_values(by=['rank'], inplace=True)
    df_a_final = df_a[['uf', '< 6,85', 'rank']].query('rank <= 6 or uf in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_a_final.columns = ['Região', 'Percentual', 'Posição']

    df_a_final.to_excel(os.path.join(sheets_path, 'g20.5a.xlsx'), index=False, sheet_name='g20.5a')

    # tabela b
    df_b = df_concat.copy()
    df_b.sort_values(by=['uf', 'ano'], inplace=True)
    df_b['diff'] = df_b.groupby('uf')['< 6,85'].diff()
    df_b['valor'] = df_b['diff']
    df_b.loc[df_b['uf'].isin(['Brasil', 'Nordeste']), 'valor'] = np.nan
    df_b = df_b.query('ano == @df_concat.ano.max()').copy()
    df_b['rank'] = df_b['valor'].rank(method='first', ascending=False)
    df_b.sort_values(by=['ano', 'rank'], inplace=True)
    df_b_final = df_b[['uf', 'diff', 'rank']].query('rank <= 6 or uf in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_b_final.columns = ['Região', 'Variação (pp)', 'Posição']

    df_b_final.to_excel(os.path.join(sheets_path, 'g20.5b.xlsx'), index=False, sheet_name='g20.5b')

except:
    errors['Gráfico 20.5'] = traceback.format_exc()


# Gráfico 20.6
try:
    # importação dos indicadores
    df = c.open_file(file_path=file.content, ext='zip', excel_name='Tabela 2.18', skiprows=5)
    
    dfs = []
    for i, tb in enumerate(df.keys()):
        if '(CV)' not in tb:
            df_tb = df[tb].copy()
            df_tb.columns = ['uf', 'total', '< 2,15', '< 3,65', '< 6,85', 'mediana nacional', 'mediana regional', 'tanto faz']
            df_tb.dropna(how='any', inplace=True)
            df_tb['Ano'] = int(tb)

            dfs.append(df_tb.query('uf.isin(["Brasil", "Nordeste", "Sergipe"])')[['uf', 'Ano', '< 6,85']])

    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat['uf'] = df_concat['uf'].map({'Brasil': 'BR', 'Nordeste': 'NE', 'Sergipe': 'SE'})
    df_pivoted = df_concat.pivot(index='Ano', columns='uf', values='< 6,85').reset_index()

    df_pivoted.to_excel(os.path.join(sheets_path, 'g20.6.xlsx'), index=False, sheet_name='g20.6')

except:
    errors['Gráfico 20.6'] = traceback.format_exc()


# Gráfico 20.7
try:
    # importação dos indicadores
    df = c.open_file(file_path=file.content, ext='zip', excel_name='Tabela 2.18', skiprows=5)
    df_last_year = df[list(df.keys())[0]]
    df_fisrt_year = df[list(df.keys())[-2]]
    
    dfs = []
    for i, tb in enumerate([df_last_year, df_fisrt_year]):
        df_tb = tb.copy()
        df_tb.columns = ['uf', 'total', '< 2,15', '< 3,65', '< 6,85', 'mediana nacional', 'mediana regional', 'tanto faz']
        df_tb.dropna(how='any', inplace=True)
        df_tb['ano'] = int(list(df.keys())[0]) if i == 0 else int(list(df.keys())[-2])

        dfs.append(df_tb.query('uf.isin(@c.mapping_states_abbreviation.keys())')[['uf', 'ano', '< 2,15']])

    df_concat = pd.concat(dfs, ignore_index=True)

    # tabela a
    df_a = df_concat.query('ano == @df_concat.ano.max()').copy()
    df_a['valor'] = df_a['< 2,15']
    df_a.loc[df_a['uf'].isin(['Brasil', 'Nordeste']), 'valor'] = np.nan
    df_a['rank'] = df_a['valor'].rank(method='first', ascending=False)
    df_a.sort_values(by=['rank'], inplace=True)
    df_a_final = df_a[['uf', '< 2,15', 'rank']].query('rank <= 6 or uf in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_a_final.columns = ['Região', 'Percentual', 'Posição']

    df_a_final.to_excel(os.path.join(sheets_path, 'g20.7a.xlsx'), index=False, sheet_name='g20.7a')

    # tabela b
    df_b = df_concat.copy()
    df_b.sort_values(by=['uf', 'ano'], inplace=True)
    df_b['diff'] = df_b.groupby('uf')['< 2,15'].diff()
    df_b['valor'] = df_b['diff']
    df_b.loc[df_b['uf'].isin(['Brasil', 'Nordeste']), 'valor'] = np.nan
    df_b = df_b.query('ano == @df_concat.ano.max()').copy()
    df_b['rank'] = df_b['valor'].rank(method='first', ascending=False)
    df_b.sort_values(by=['ano', 'rank'], inplace=True)
    df_b_final = df_b[['uf', 'diff', 'rank']].query('rank <= 6 or uf in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_b_final.columns = ['Região', 'Variação (pp)', 'Posição']

    df_b_final.to_excel(os.path.join(sheets_path, 'g20.7b.xlsx'), index=False, sheet_name='g20.7b')

except:
    errors['Gráfico 20.7'] = traceback.format_exc()


# Gráfico 20.8
try:
    # importação dos indicadores
    df = c.open_file(file_path=file.content, ext='zip', excel_name='Tabela 2.18', skiprows=5)
    
    dfs = []
    for i, tb in enumerate(df.keys()):
        if '(CV)' not in tb:
            df_tb = df[tb].copy()
            df_tb.columns = ['uf', 'total', '< 2,15', '< 3,65', '< 6,85', 'mediana nacional', 'mediana regional', 'tanto faz']
            df_tb.dropna(how='any', inplace=True)
            df_tb['Ano'] = int(tb)

            dfs.append(df_tb.query('uf.isin(["Brasil", "Nordeste", "Sergipe"])')[['uf', 'Ano', '< 2,15']])

    df_concat = pd.concat(dfs, ignore_index=True)
    df_pivoted = df_concat.pivot(index='Ano', columns='uf', values='< 2,15').reset_index()

    df_pivoted.to_excel(os.path.join(sheets_path, 'g20.8.xlsx'), index=False, sheet_name='g20.8')

except:
    errors['Gráfico 20.8'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g20.5--g20.6--g20.7--g20.8.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
