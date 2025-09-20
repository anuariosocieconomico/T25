import functions as c
import os
import pandas as pd
import numpy as np
import json
import io
import sidrapy
from bs4 import BeautifulSoup
import traceback
import tempfile
import shutil
from datetime import datetime
from time import sleep
import ipeadatapy


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS E PLANILHA
# ************************

# sidra 5442
url = 'https://apisidra.ibge.gov.br/values/t/5442/n1/all/n2/2/n3/28/v/5932/p/all/c888/47946,47947,47948,47949,47950,56622,56623,56624,60032?formato=json'
try:
    session = c.create_session_with_retries()
    data = session.get(url, timeout=session.request_timeout, headers=c.headers)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'D4N', 'V']].copy()
    df.columns = ['Data', 'Região', 'Variável', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df['Valor'] = df['Valor'].fillna(0)  # substitui valores nulos por 0
    df['Ano'] = df['Data'].str.split(' ').str[-1].astype(int)  # extrai o ano da coluna Data
    df['Trimestre'] = df['Data'].str.split(' ').str[0].str[0].astype(int)  # extrai o ano da coluna Data
    df['Valor'] = df['Valor'].astype(int)

    c.to_excel(df, dbs_path, 'sidra_5442.xlsx')
except Exception as e:
    errors['Sidra 5442'] = traceback.format_exc()

"""
Para esta fonte, foi hardcoded o ano do arquivo, em vez de buscar o ano mais recente.
No arquivo mais recente, o dado descrito da documentação não está mais disponível.
Enquanto o professor busca uma alternativa, ele pediu que coletasse os dados do ano que continha o dado.
"""
# sintese de indicadores sociais
try:
    url = 'https://ftp.ibge.gov.br/Indicadores_Sociais/Sintese_de_Indicadores_Sociais/Sintese_de_Indicadores_Sociais_2020/xls/2_Rendimento_xls.zip'
    session = c.create_session_with_retries()
    file = session.get(url, timeout=session.request_timeout, headers=c.headers)
    content = c.open_file(file_path=file.content, ext='zip', excel_name='Tabela 2.3', skiprows=6)
    dfs = []
    for tb in content.keys():
        if '(CV)' not in tb:
            df = content[tb].dropna(how='any').reset_index(drop=True)
            df = df.iloc[:, [0, 3, 9, 15, 21]]
            df.columns = ['Região', 'Total', 'Até 1/4', 'Mais de 1/4 até 1/2', 'Mais de 3']
            df['Ano'] = int(tb)
            
            dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True).query('`Região` in @c.mapping_states_abbreviation.keys()')

    c.to_csv(df, dbs_path, 'indicadores_sociais.csv')
except Exception as e:
    errors['IBGE - Síntese de Indicadores Sociais'] = traceback.format_exc()


# sidra 5606
url = 'https://apisidra.ibge.gov.br/values/t/5606/n1/all/n2/2/n3/28/v/6293/p/all?formato=json'
try:
    session = c.create_session_with_retries()
    data = session.get(url, timeout=session.request_timeout, headers=c.headers)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'D2N', 'V']].copy()
    df.columns = ['Data', 'Região', 'Variável', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df['Valor'] = df['Valor'].fillna(0)  # substitui valores nulos por 0
    df['Ano'] = df['Data'].str.split(' ').str[-1].astype(int)  # extrai o ano da coluna Data
    df['Trimestre'] = df['Data'].str.split(' ').str[0].str[0].astype(int)  # extrai o ano da coluna Data
    df['Valor'] = df['Valor'].astype(int)

    c.to_excel(df, dbs_path, 'sidra_5606.xlsx')
except Exception as e:
    errors['Sidra 5606'] = traceback.format_exc()


# deflator IPEA IPCA
try:
    data = ipeadatapy.timeseries('PRECOS_IPCAG')
    data.rename(columns={'YEAR': 'Ano', 'VALUE ((% a.a.))': 'Valor'}, inplace=True)  # renomeia as colunas
    c.to_excel(data, dbs_path, 'ipeadata_ipca.xlsx')
except Exception as e:
    errors['IPEA IPCA'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# g14.1
try:
    # leitura da base de dados
    data = c.open_file(dbs_path, 'sidra_5442.xlsx', 'xls', sheet_name='Sheet1').query("`Variável` == 'Total'")
    ipca = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1')
    max_year = data['Ano'].max()  # obtém o ano mais recente da base de dados
    min_year = data['Ano'].min()  # obtém o ano mais antigo da base de dados

    # tratamento do deflator
    df_deflator = ipca.query('Ano >= @min_year & Ano <= @max_year', engine='python').copy()  # filtra o deflator para o ano mais recente
    df_deflator.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    df_deflator.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    df_deflator['Index'] = 100.00
    df_deflator['Diff'] = None

    for row in range(1, len(df_deflator)):
        df_deflator.loc[row,'Diff'] = 1 +(df_deflator.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
        df_deflator.loc[row, 'Index'] = df_deflator.loc[row - 1, 'Index'] / df_deflator.loc[row, 'Diff']  # calcula o índice de preços

    df_merged = data.merge(df_deflator[['Ano', 'Index']], how='left', on='Ano', validate='m:1')  # une as bases de dados
    df_merged.dropna(subset=['Index'], inplace=True)  # remove linhas onde o índice de preços é nulo
    df_merged['Valor'] = (df_merged['Valor'] / df_merged['Index']) * 100  # calcula o valor deflacionado
    df_merged.loc[:, 'Variável'] = 'Rendimento médio mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de referência com rendimento de trabalho, habitualmente recebido em todos os trabalhos'
    df_merged['Mês'] = df_merged['Trimestre'].map({
        1: '01',
        2: '04',
        3: '07',
        4: '10'
    })
    df_merged['Trimestre'] = '01/' + df_merged['Mês'] + '/' + df_merged['Ano'].astype(str)  # formata o trimestre

    df_final = df_merged.query('Ano >= 2017')[['Região', 'Variável', 'Trimestre', 'Valor']].copy()  # seleciona as colunas relevantes
    df_final['Valor'] = df_final['Valor'].round(0)

    c.to_excel(df_final, sheets_path, 'g14.1.xlsx')

except:
    errors['Gráfico 14.1'] = traceback.format_exc()


# g14.2
try:
    # leitura da base de dados
    data = c.open_file(dbs_path, 'sidra_5606.xlsx', 'xls', sheet_name='Sheet1')
    ipca = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1')
    max_year = data['Ano'].max()  # obtém o ano mais recente da base de dados
    min_year = data['Ano'].min()  # obtém o ano mais antigo da base de dados

    # tratamento do deflator
    df_deflator = ipca.query('Ano >= @min_year & Ano <= @max_year', engine='python').copy()  # filtra o deflator para o ano mais recente
    df_deflator.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    df_deflator.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    df_deflator['Index'] = 100.00
    df_deflator['Diff'] = None

    for row in range(1, len(df_deflator)):
        df_deflator.loc[row,'Diff'] = 1 +(df_deflator.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
        df_deflator.loc[row, 'Index'] = df_deflator.loc[row - 1, 'Index'] / df_deflator.loc[row, 'Diff']  # calcula o índice de preços

    df_merged = data.merge(df_deflator[['Ano', 'Index']], how='left', on='Ano', validate='m:1')  # une as bases de dados
    df_merged.dropna(subset=['Index'], inplace=True)  # remove linhas onde o índice de preços é nulo
    df_merged['Valor'] = (df_merged['Valor'] / df_merged['Index']) * 100  # calcula o valor deflacionado
    df_merged.loc[:, 'Variável'] = 'Massa de rendimento mensal real das pessoas de 14 anos ou mais de idade ocupadas na semana de referência com rendimento de trabalho, habitualmente recebido em todos os trabalhos'
    df_merged['Mês'] = df_merged['Trimestre'].map({
        1: '01',
        2: '04',
        3: '07',
        4: '10'
    })
    df_merged['Trimestre'] = '01/' + df_merged['Mês'] + '/' + df_merged['Ano'].astype(str)  # formata o trimestre

    df_final = df_merged.query('Ano >= 2019')[['Região', 'Variável', 'Trimestre', 'Valor']].copy()  # seleciona as colunas relevantes
    df_final['Valor'] = df_final['Valor'].round(0)

    c.to_excel(df_final, sheets_path, 'g14.2.xlsx')

except:
    errors['Gráfico 14.2'] = traceback.format_exc()


# t14.1
try:
    # leitura da base de dados
    data = c.open_file(dbs_path, 'sidra_5442.xlsx', 'xls', sheet_name='Sheet1').query("`Variável` != 'Total'")
    ipca = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1')
    max_year = min(data['Ano'].max(), ipca['Ano'].max())  # obtém o ano mais recente da base de dados
    min_year = max(data['Ano'].min(), ipca['Ano'].min())  # obtém o ano mais antigo da base de dados
    max_quarter = data.query('Ano == @max_year')['Trimestre'].max()  # obtém o trimestre mais recente da base de dados

    # tratamento do deflator
    df_deflator = ipca.query('Ano >= @min_year & Ano <= @max_year', engine='python').copy()  # filtra o deflator para o ano mais recente
    df_deflator.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    df_deflator.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    df_deflator['Index'] = 100.00
    df_deflator['Diff'] = None

    for row in range(1, len(df_deflator)):
        df_deflator.loc[row,'Diff'] = 1 +(df_deflator.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
        df_deflator.loc[row, 'Index'] = df_deflator.loc[row - 1, 'Index'] / df_deflator.loc[row, 'Diff']  # calcula o índice de preços

    # filtra os dados para o trimestre mais recente
    df_merged = data.query('Trimestre == @max_quarter').merge(df_deflator[['Ano', 'Index']], how='left', on='Ano', validate='m:1')  # une as bases de dados
    df_merged.dropna(subset=['Index'], inplace=True)  # remove linhas onde o índice de preços é nulo
    df_merged['Valor'] = (df_merged['Valor'] / df_merged['Index']) * 100  # calcula o valor deflacionado
    df_merged['Variação % mesmo trimestre ano anterior'] = df_merged.groupby(['Região', 'Variável'])['Valor'].pct_change() * 100  # calcula a variação percentual em relação ao mesmo trimestre do ano anterior
    df_merged['Mês'] = df_merged['Trimestre'].map({
        1: '01',
        2: '04',
        3: '07',
        4: '10'
    })
    df_merged['Trimestre'] = '01/' + df_merged['Mês'] + '/' + df_merged['Ano'].astype(str)  # formata o trimestre

    df_final = df_merged.query('Ano >= 2015')[['Região', 'Variável', 'Trimestre', 'Valor', 'Variação % mesmo trimestre ano anterior']].copy()  # seleciona as colunas relevantes
    df_final.rename(columns={'Variável': 'Setor'}, inplace=True)
    df_final['Valor'] = df_final['Valor'].round(0)

    c.to_excel(df_final, sheets_path, 't14.1.xlsx')

except:
    errors['Tabela 14.1'] = traceback.format_exc()


# g14.3
try:
    # leitura da base de dados
    df = c.open_file(dbs_path, 'indicadores_sociais.csv', 'csv')[['Região', 'Ano', 'Total']]
    df['Dummy'] = df['Total']
    df.loc[df['Região'].isin(['Brasil', 'Nordeste']), 'Dummy'] = np.nan
    df['Colocação'] = df.groupby('Ano')['Dummy'].rank(ascending=False, method='first')

    df_a = df.loc[
        (df['Ano'] == df['Ano'].max()) & 
        ((df['Colocação'] <= 6) | (df['Região'].isin(['Brasil', 'Nordeste', 'Sergipe'])))
    ].copy()

    df_a['Variável'] = 'Trabalho como origem na renda (%)'
    df_a['Ano'] = '31/12/' + df_a['Ano'].astype(str)
    df_final = df_a[['Região', 'Variável', 'Ano', 'Total', 'Colocação']].copy()
    df_final.sort_values(['Colocação', 'Região'], ascending=[True, False], inplace=True)
    df_final['Colocação'] = df_final['Colocação'].apply(lambda x: f'{int(x)}º' if pd.notnull(x) else '')
    df_final.rename(columns={'Total': 'Valor'}, inplace=True)
    
    c.to_excel(df_final, sheets_path, 'g14.3a.xlsx')

    df_b = df.groupby('Região')['Total'].mean().reset_index()
    df_b['Dummy'] = df_b['Total']
    df_b.loc[df_b['Região'].isin(['Brasil', 'Nordeste']), 'Dummy'] = np.nan
    df_b['Colocação'] = df_b['Dummy'].rank(ascending=False, method='first')

    df_b = df_b.loc[
        (df_b['Colocação'] <= 6) | (df_b['Região'].isin(['Brasil', 'Nordeste', 'Sergipe']))
    ].copy()

    df_b['Variável'] = f'Trabalho como origem na renda (%): média de {df["Ano"].min()} a {df["Ano"].max()}'
    df_b = df_b[['Região', 'Variável', 'Total', 'Colocação']].copy()
    df_b.sort_values(['Colocação', 'Região'], ascending=[True, False], inplace=True)
    df_b['Colocação'] = df_b['Colocação'].apply(lambda x: f'{int(x)}º' if pd.notnull(x) else '')
    df_b.rename(columns={'Total': 'Valor'}, inplace=True)

    c.to_excel(df_b, sheets_path, 'g14.3b.xlsx')

except:
    errors['Gráfico 14.3'] = traceback.format_exc()


# g14.4
try:
    # leitura da base de dados
    data = c.open_file(dbs_path, 'indicadores_sociais.csv', 'csv').query('`Região` in ["Brasil", "Nordeste", "Sergipe"]')[['Região', 'Ano', 'Total']]
    data['Variável'] = 'Trabalho como origem na renda (%)'
    data.sort_values(['Região', 'Ano'], inplace=True)
    data['Ano'] = '31/12/' + data['Ano'].astype(str)
    data.rename(columns={'Total': 'Valor'}, inplace=True)
    df_final = data[['Região', 'Variável', 'Ano', 'Valor']].copy()

    c.to_excel(df_final, sheets_path, 'g14.4.xlsx')

except:
    errors['Tabela 14.4'] = traceback.format_exc()


# t14.2
try:
    # leitura da base de dados
    data = c.open_file(dbs_path, 'indicadores_sociais.csv', 'csv').query('`Região` in ["Brasil", "Nordeste", "Sergipe"]')
    df_melted = data.melt(id_vars=['Região', 'Ano'], value_vars=['Até 1/4', 'Mais de 1/4 até 1/2', 'Mais de 3'], var_name='Variável', value_name='Valor')
    df_melted.sort_values(['Região', 'Variável', 'Ano'], inplace=True)
    df_melted['Ano'] = '31/12/' + df_melted['Ano'].astype(str)
    df_final = df_melted[['Região', 'Variável', 'Ano', 'Valor']].copy()

    c.to_excel(df_final, sheets_path, 't14.2.xlsx')

except:
    errors['Tabela 14.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g14.1--g14.2--t14.1--t14.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
