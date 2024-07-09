import functions as c
import os
import pandas as pd
import json
import datetime
import time
import re
import traceback
import tempfile
import shutil


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}

# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    base_year = 2013
    current_year = datetime.datetime.now().year
    dfs = []

    for y in range(base_year, current_year + 1):
        url = f'https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca?an_exercicio={y}&no_anexo=DCA-Anexo%20I-C' \
              f'&id_ente=28'
        response = c.open_url(url)
        time.sleep(1)
        if response.status_code == 200 and len(response.json()['items']) > 1:
            df = pd.DataFrame(response.json()['items'])
            dfs.append(df)

    df_concat = pd.concat(dfs, ignore_index=True)
    c.to_csv(df_concat, dbs_path, 'siconfi_DCA.csv')

except Exception as e:
    errors['https://apidatalake.tesouro.gov.br'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

try:
    df = c.open_file(dbs_path, 'siconfi_DCA.csv', 'csv')

    pattern = r"(\d+\.\d+\.\d+\.\d+\.\d+\.\d+\.\d+\s*-\s*)(.*)"
    df['conta'] = df['conta'].apply(
        lambda x: re.search(pattern, x).group(2).strip() if re.search(pattern, x) else x)
    pattern = r"(\d+\.\d+\.\d+\.\d+\.\d+\.\d+\.\d+\s*)(.*)"
    df['conta'] = df['conta'].apply(
        lambda x: re.search(pattern, x).group(2).strip() if re.search(pattern, x) else x)

    df = df.loc[
        (df['conta'] == 'Receitas Correntes') |
        (df['conta'].str.contains('Receita Tributária')) | (df['conta'].str.contains('Taxas e Contribuições')) |
        (df['conta'].str.contains('Cota-Parte do Fundo')) & (df['conta'].str.contains('Estados')) |
        (df['conta'].str.contains('Transferência')) & (df['conta'].str.contains('Recursos Naturais')) & ~(
            df['conta'].str.contains('Outras')) |
        (df['conta'].str.contains('FUNDEB'))
        ]

    dfs = []
    for y in df['exercicio'].unique():
        df4 = pd.DataFrame()
        df3 = df.loc[df['exercicio'] == y]
        df4['ano'] = [df3['exercicio'].values[0]]

        operand1 = df3.loc[(df3['conta'].str.contains('Receitas Correntes')) & (
            df3['coluna'].str.contains('Receitas Brutas Realizadas')), 'valor'].sum()
        operand2 = df3.loc[
            (df3['conta'].str.contains('Receitas Correntes')) & (df3['coluna'].str.contains('Deduções')), 'valor'].sum()
        df4['receita corrente líquida'] = operand1 - operand2

        operand1 = df3.loc[
            ((df3['conta'].str.contains('Tributária')) | (df3['conta'].str.contains('Taxas e Contribuições'))) &
            (df3['coluna'].str.contains('Receitas Brutas Realizadas')), 'valor'].sum()
        operand2 = df3.loc[
            ((df3['conta'].str.contains('Tributária')) | (df3['conta'].str.contains('Taxas e Contribuições'))) &
            (df3['coluna'].str.contains('Deduções')), 'valor'].sum()
        df4['receita tributária líquida'] = operand1 - operand2

        operand1 = df3.loc[
            (df['conta'].str.contains('Cota-Parte do Fundo')) & (df['conta'].str.contains('Estados')) &
            (df3['coluna'].str.contains('Receitas Brutas Realizadas')), 'valor'].sum()
        operand2 = df3.loc[
            (df['conta'].str.contains('Cota-Parte do Fundo')) & (df['conta'].str.contains('Estados')) &
            (df3['coluna'].str.contains('Deduções')), 'valor'].sum()
        df4['fpe líquido'] = operand1 - operand2

        operand1 = df3.loc[
            (df['conta'].str.contains('Transferência')) & (df['conta'].str.contains('Recursos Naturais')) &
            (df3['coluna'].str.contains('Receitas Brutas Realizadas')), 'valor'].sum()
        operand2 = df3.loc[
            (df['conta'].str.contains('Transferência')) & (df['conta'].str.contains('Recursos Naturais')) &
            (df3['coluna'].str.contains('Deduções')), 'valor'].sum()
        df4['receita de exploração de recursos naturais'] = operand1 - operand2

        operand1 = df3.loc[
            (df['conta'].str.contains('FUNDEB')) &
            (df3['coluna'].str.contains('Receitas Brutas Realizadas')), 'valor'].sum()
        operand2 = df3.loc[
            (df['conta'].str.contains('FUNDEB')) &
            (df3['coluna'].str.contains('Deduções')), 'valor'].sum()
        df4['fundeb líquido'] = operand1 - operand2

        dfs.append(df4)

    df_concat = pd.concat(dfs, ignore_index=True)

    df = pd.DataFrame()
    df['ano'] = df_concat['ano']
    for col in df_concat.columns:
        if col != 'ano':
            df[col] = (df_concat[col] / df_concat['receita corrente líquida']) * 100

    df[df.columns[1:]] = df[df.columns[1:]].astype('float64')
    df[df.columns[0]] = df[df.columns[0]].astype('str')

    df.rename(columns={'receita tributária líquida': 'Receitas tributárias',
                       'fpe líquido': 'FPE',
                       'receita de exploração de recursos naturais': 'Receita de exploração de RN',
                       'fundeb líquido': 'Fundeb',
                       'ano': 'Ano'}, inplace=True)
    df = df[['Ano', 'Receitas tributárias', 'FPE', 'Fundeb', 'Receita de exploração de RN']]

    c.to_excel(df, sheets_path, 'g11.2.xlsx')

except Exception as e:
    errors['Gráfico 11.2'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g11.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
