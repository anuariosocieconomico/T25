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

# sidra 5906
url = 'https://apisidra.ibge.gov.br/values/t/5906/n1/all/n3/all/v/7168/p/all/c11046/all/d/v7168%205?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'D4N', 'V']].copy()
    df.columns = ['Data', 'Região', 'Variável', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df['Ano'] = df['Data'].str.split(' ').str[-1]  # extrai o ano da coluna Data
    df['Ano'] = df['Ano'].astype(int)  # converte a coluna Ano para inteiro
    df['Mês'] = df['Data'].str.split(' ').str[0]  # extrai o mês da coluna Data

    c.to_excel(df, dbs_path, 'sidra_5906.xlsx')
except Exception as e:
    errors['Sidra 5906'] = traceback.format_exc()


# sidra 2715 (IBGE - Pesquisa Anual de Serviços (PAS))
url = 'https://apisidra.ibge.gov.br/values/t/2715/n1/all/v/672/p/all/c12354/all/c12355/9309,31399,106869,106874,106876,106882,106883,107071?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D4N', 'D5N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Variável', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros
    df['Ano'] = df['Ano'].astype(int)  # converte a coluna Ano para inteiro

    c.to_excel(df, dbs_path, 'sidra_2715.xlsx')
except Exception as e:
    errors['Sidra 2715 (PAS)'] = traceback.format_exc()


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

# gráfico 10.1
try:
    data = c.open_file(dbs_path, 'sidra_5906.xlsx', 'xls', sheet_name='Sheet1').query(
        '`Mês` == "dezembro" and `Variável`.str.contains("receita nominal")', engine='python'
    )  # filtra os dados para o mês de dezembro e para as variáveis que contêm "receita nominal"
    data.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano
    
    # tratamento dos dados principais
    df_ne = data.query('`Região` in @c.ne_states', engine='python').copy()
    assert df_ne['Região'].nunique() == 9, "Número de estados do NE diferente de 9"
    df_ne['Região'] = 'Nordeste'  # renomeia a região para Nordeste
    df_ne = df_ne.groupby(['Ano', 'Região', 'Variável'], as_index=False).agg({'Valor': 'mean'})  # agrupa por Ano e Variável, somando os valores

    df = data.loc[data['Região'].isin(['Brasil', 'Sergipe']), ['Ano', 'Região', 'Variável', 'Valor']].copy()  # seleciona as colunas relevantes
    df = pd.concat([df, df_ne], ignore_index=True)  # concatena os dados originais com os do Nordeste
    df['Variação'] = df.groupby(['Região', 'Variável'])['Valor'].pct_change() * 100  # calcula a variação percentual

    df_pivot = pd.pivot(df, index='Ano', columns='Região', values='Variação').reset_index()
    df_pivot.dropna(subset=['Brasil', 'Nordeste', 'Sergipe'], inplace=True)  # remove linhas onde Brasil ou Sergipe são NaN

    df_pivot.to_excel(os.path.join(sheets_path, 'g10.1.xlsx'), index=False, sheet_name='g10.1')

except Exception as e:
    errors['Gráfico 10.1'] = traceback.format_exc()


# gráfico 10.2
try:
    data = c.open_file(dbs_path, 'sidra_5906.xlsx', 'xls', sheet_name='Sheet1').query(
        '`Mês` == "dezembro" and `Variável`.str.contains("volume de serviços")', engine='python'
    )  # filtra os dados para o mês de dezembro e para as variáveis que contêm "receita nominal"
    data.sort_values(['Região', 'Variável', 'Ano'], inplace=True)  # ordena os dados por Região, Variável e Ano
    
    # tratamento dos dados principais
    df_ne = data.query('`Região` in @c.ne_states', engine='python').copy()
    assert df_ne['Região'].nunique() == 9, "Número de estados do NE diferente de 9"
    df_ne['Região'] = 'Nordeste'  # renomeia a região para Nordeste
    df_ne = df_ne.groupby(['Ano', 'Região', 'Variável'], as_index=False).agg({'Valor': 'mean'})  # agrupa por Ano e Variável, somando os valores

    df = data.loc[data['Região'].isin(['Brasil', 'Sergipe']), ['Ano', 'Região', 'Variável', 'Valor']].copy()  # seleciona as colunas relevantes
    df = pd.concat([df, df_ne], ignore_index=True)  # concatena os dados originais com os do Nordeste
    df['Variação'] = df.groupby(['Região', 'Variável'])['Valor'].pct_change() * 100  # calcula a variação percentual

    df_pivot = pd.pivot(df, index='Ano', columns='Região', values='Variação').reset_index()
    df_pivot.dropna(subset=['Brasil', 'Nordeste', 'Sergipe'], inplace=True)  # remove linhas onde Brasil ou Sergipe são NaN

    df_pivot.to_excel(os.path.join(sheets_path, 'g10.2.xlsx'), index=False, sheet_name='g10.2')

except Exception as e:
    errors['Gráfico 10.2'] = traceback.format_exc()


# tabela 10.1
try:
    data = c.open_file(dbs_path, 'sidra_2715.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= 2010', engine='python')  # filtra os dados para o ano de 2012 ou posterior
    deflator = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1')
    max_year = data['Ano'].max()
    
    # tratamento do deflator
    df_deflator = deflator.query('Ano <= @max_year', engine='python').copy()  # filtra o deflator para o ano mais recente
    df_deflator.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    df_deflator.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    df_deflator['Index'] = 100.00
    df_deflator['Diff'] = None

    for row in range(1, len(df_deflator)):
        df_deflator.loc[row,'Diff'] = df_deflator.loc[row - 1, 'Valor'] / df_deflator.loc[row, 'Valor']  # calcula a diferença entre o valor atual e o anterior
        df_deflator.loc[row, 'Index'] = df_deflator.loc[row - 1, 'Index'] / df_deflator.loc[row, 'Diff']  # calcula o índice de preços

    # tratamento dos dados principais
    df = data.loc[
        ((data['Região'].isin(['Brasil', 'Região Nordeste'])) & (data['Variável'] == 'Total')) |
        ((data['Região'] == 'Sergipe') & (
            data['Variável'].str.contains('Serviços prestados às famílias') |
            data['Variável'].str.contains('Serviços de informação e comunicação') |
            data['Variável'].str.contains('Serviços profissionais, administrativos e complementares') |
            data['Variável'].str.contains('Transportes, serviços auxiliares aos transportes e correio') |
            data['Variável'].str.contains('Atividades imobiliárias') |
            data['Variável'].str.contains('Serviços de manutenção e reparação') |
            data['Variável'].str.contains('Outras atividades de serviços') |
            data['Variável'].str.startswith('Total')
            )
        )
    , :].copy()  # seleciona as colunas relevantes

    df_merged = pd.merge(df, df_deflator[['Ano', 'Index']], on='Ano', how='left', validate='m:1')  # mescla os dados com o deflator
    df_merged['Valor'] = (df_merged['Valor'] / df_merged['Index']) * 100  # ajusta os valores com o deflatord
    df_merged['Valor'] = df_merged['Valor'] / 1000  # converte os valores para milhões
    df_self_merged = df_merged.merge(
        df_merged[(df_merged['Região'] == 'Sergipe') & (df_merged['Variável'] == 'Total')][['Ano', 'Variável', 'Valor']],
        on=['Ano', 'Variável'],
        how='left',
        suffixes=('', '_Sergipe'),
        validate='m:1'
    )  # mescla os dados com eles mesmos para obter os valores de Sergipe

    df_self_merged['Participação'] = (df_self_merged['Valor_Sergipe'] / df_self_merged['Valor']) * 100  # calcula a participação de Sergipe

    # dataframe de sergipe
    df_sergipe = df_self_merged.loc[df_self_merged['Região'] == 'Sergipe', ['Ano', 'Variável', 'Valor']].copy()

    # renomeia as variáveis
    df_sergipe.loc[df_sergipe['Variável'].str.contains('Serviços prestados às famílias', case=False, na=False), 'Variável'] = '   Serviços prestados principalmente às famílias'
    df_sergipe.loc[df_sergipe['Variável'].str.contains('Serviços de informação e comunicação', case=False, na=False), 'Variável'] = '   Serviços de informação e comunicação'
    df_sergipe.loc[df_sergipe['Variável'].str.contains('Serviços profissionais, administrativos e complementares', case=False, na=False), 'Variável'] = '   Serviços profissionais, administrativos e complementares'
    df_sergipe.loc[df_sergipe['Variável'].str.contains('Transportes, serviços auxiliares aos transportes e correio', case=False, na=False), 'Variável'] = '   Transportes, serviços auxiliares aos transportes e correio'
    df_sergipe.loc[df_sergipe['Variável'].str.contains('Atividades imobiliárias', case=False, na=False), 'Variável'] = '   Atividades imobiliárias '
    df_sergipe.loc[df_sergipe['Variável'].str.contains('Serviços de manutenção e reparação', case=False, na=False), 'Variável'] = '   Serviços de manutenção e reparação'
    df_sergipe.loc[df_sergipe['Variável'].str.contains('Outras atividades de serviços', case=False, na=False), 'Variável'] = '   Outras atividades de serviços'
    df_sergipe.loc[df_sergipe['Variável'].str.startswith('Total', na=False), 'Variável'] = 'Receita bruta de prestação de Serviços'

    df_sergipe['Unidade'] = 'R$ milhões'
    df_sergipe.rename(columns={'Variável': 'Indicador'}, inplace=True)  # renomeia a coluna Valor para Sergipe
    df_sergipe = df_sergipe[['Ano', 'Indicador', 'Unidade', 'Valor']].copy()  # seleciona as colunas relevantes

    # dataframe brasil e nordeste
    df_brasil_ne = df_self_merged.loc[df_self_merged['Região'].isin(['Brasil', 'Região Nordeste']), ['Ano', 'Região', 'Variável', 'Participação']].copy()
    df_brasil_ne['Unidade'] = '%'
    df_brasil_ne['Variável'] = 'Participação da receita bruta de Serviços em Sergipe no ' + df_brasil_ne['Região'].str.split(' ').str[-1]
    df_brasil_ne.rename(columns={'Variável': 'Indicador', 'Participação': 'Valor'}, inplace=True)  # renomeia as colunas
    df_brasil_ne = df_brasil_ne[['Ano', 'Indicador', 'Unidade', 'Valor']].copy()  # seleciona as colunas relevantes

    df_concat = pd.concat([df_sergipe, df_brasil_ne], ignore_index=True)  # concatena os dataframes de Sergipe e Brasil/Nordeste
    df_concat.sort_values(['Ano', 'Indicador'], ascending=[True, False], inplace=True)  # ordena os dados por Ano e Indicador

    df_concat.to_excel(os.path.join(sheets_path, 't10.1.xlsx'), index=False, sheet_name='t10.1')

except Exception as e:
    errors['Tabela 10.1'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g10.1--g10.2--t10.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
