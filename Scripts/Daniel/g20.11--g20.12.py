import functions as c
import os
import pandas as pd
import numpy as np
import json
import sidrapy
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
# PLANILHA
# ************************

# g20.11
url = 'https://apisidra.ibge.gov.br/values/t/7435/n1/all/n2/2/n3/all/v/10681/p/all/d/v10681%203?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    min_year = df['Ano'].min()
    max_year = df['Ano'].max()

    # tabela a
    df_a = df.query('Ano == @max_year').copy()
    df_a['val'] = df_a['Valor']
    df_a.loc[df_a['Região'].isin(['Brasil', 'Nordeste']), 'val'] = np.nan
    df_a['rank'] = df_a['val'].rank(method='first', ascending=False)
    df_a.sort_values(by=['rank'], inplace=True)
    df_a['Variável'] = 'Índice de Gini do rendimento domiciliar per capita, a preços médios do ano'
    df_a['Ano'] = '31/12/' + df_a['Ano'].astype(str)
    df_a['Colocação'] = df_a['rank'].apply(lambda x: np.nan if pd.isna(x) else f"{int(x)}º")

    df_a_final = df_a.query('rank <= 6 or Região in ["Brasil", "Nordeste", "Sergipe"]')[['Região', 'Variável', 'Ano', 'Valor', 'Colocação']].copy()
    c.to_excel(df_a_final, sheets_path, 'g20.11a.xlsx')

    # tabela b
    df_b = df.query('Ano in [@min_year, @max_year]').copy()
    df_b.sort_values(by=['Região', 'Ano'], inplace=True)
    df_b['diff'] = df_b.groupby('Região')['Valor'].diff()
    df_b['val'] = df_b['diff']
    df_b.loc[df_b['Região'].isin(['Brasil', 'Nordeste']), 'val'] = np.nan
    df_b = df_b.query('Ano == @max_year').copy()
    df_b['rank'] = df_b['val'].rank(method='first', ascending=False)
    df_b.sort_values(by=['Ano', 'rank'], inplace=True)
    df_b['Variável'] = f'Diferença {max_year}-{min_year}'
    df_b['Colocação'] = df_b['rank'].apply(lambda x: np.nan if pd.isna(x) else f"{int(x)}º")

    df_b_final = df_b.query('rank <= 6 or Região in ["Brasil", "Nordeste", "Sergipe"]')[['Região', 'Variável', 'diff', 'Colocação']].copy()
    df_b_final.rename(columns={'diff': 'Valor'}, inplace=True)
    c.to_excel(df_b_final, sheets_path, 'g20.11b.xlsx')



except Exception as e:
    errors['Gráfico 20.11'] = traceback.format_exc()

# g20.12
try:
    regions = [('1', 'all'), ('2', '2'), ('3', '28')]
    dfs = []
    attempts = 0
    for reg in regions:
        while attempts <= 3:
            try:
                data = sidrapy.get_table(table_code='7435', territorial_level=reg[0], ibge_territorial_code=reg[1],
                                        variable='10681', period='all', header='n')
                data = data[['D1N', 'D2N', 'V']]
                dfs.append(data)
                c.delay_requests(1)
                break
            except:
                attempts += 1


    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat.columns = ['Região', 'Ano', 'Índice de Gini']
    c.convert_type(df_concat, 'Ano', 'int')
    c.convert_type(df_concat, 'Índice de Gini', 'float')

    c.to_excel(df_concat, sheets_path, 'g20.12.xlsx')

except Exception as e:
    errors['Gráfico 20.12'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g20.11--g20.12.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
