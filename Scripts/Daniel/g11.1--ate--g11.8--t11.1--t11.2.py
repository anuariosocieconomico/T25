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
url = 'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Contas_Regionais'

# download do arquivo especiais 2010
try:
    response = c.open_url(url)
    df = pd.DataFrame(response.json())

    # pequisa pela publicação mais recente --> inicia com '2' e possui 4 caracteres
    df = df.loc[
        (df['name'].str.startswith('2')) &
        (df['name'].str.len() == 4),
        ['name', 'path']
    ]
    df['name'] = df['name'].astype(int)
    df.sort_values(by='name', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # obtém o caminho da publicação mais recente e adiciona à url de acesso aos arquivos
    last_year = df['name'][0]
    url_to_get = url + '/' + str(last_year) + '/xls'
    response = c.open_url(url_to_get)
    df = pd.DataFrame(response.json())

    while True:
        try:
            url_to_get_esp = df.loc[
                (df['name'].str.startswith('Especiais_2010')) &
                (df['name'].str.endswith('.zip')),
                'url'
            ].values[0]
            break
        except:
            last_year -= 1
            url_to_get_esp = url + '/' + str(last_year) + '/xls'
            response = c.open_url(url_to_get_esp)
            df = pd.DataFrame(response.json())
            if last_year == 0:
                errors[url + ' (PIB)'] = 'Arquivo não encontrado em anos anteriores'
                raise Exception('Arquivo não encontrado em anos anteriores')

    # downloading e organização do arquivo: especiais 2010
    file = c.open_url(url_to_get_esp)
    c.to_file(dbs_path, 'ibge_especiais.zip', file.content)
except Exception as e:
    errors[url + ' (ESPECIAIS)'] = traceback.format_exc()


# deflator IPEA IPCA
try:
    data = ipeadatapy.timeseries('PRECOS_IPCAG')
    data.rename(columns={'YEAR': 'Ano', 'VALUE ((% a.a.))': 'Valor'}, inplace=True)  # renomeia as colunas
    c.to_excel(data, dbs_path, 'ipeadata_ipca.xlsx')
except Exception as e:
    errors['IPEA IPCA'] = traceback.format_exc()


try:
    base_year = 2015
    current_year = datetime.now().year

    dfs_anexos = []
    for a in [('Anexo 3', 'RREO-Anexo%2003'), ('Anexo 4', 'RREO-Anexo%2004')]:
        dfs_year = []
        for y in range(base_year, current_year + 1):
            url = f'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo?an_exercicio={y}&nr_periodo=6&co_tipo_demonstrativo=RREO&' \
                f'no_anexo={a[1]}&co_esfera=E&id_ente=28'
            response = c.open_url(url)
            time.sleep(1)
            if response.status_code == 200 and len(response.json()['items']) > 1:
                df = pd.DataFrame(response.json()['items'])
                dfs_year.append(df)

        df_concat = pd.concat(dfs_year, ignore_index=True)
        df_concat['Anexo'] = a[0]
        dfs_anexos.append(df_concat)

    df_concat = pd.concat(dfs_anexos, ignore_index=True)
    c.to_csv(df_concat, dbs_path, 'siconfi_DCA.csv')

except Exception as e:
    errors['https://apidatalake.tesouro.gov.br'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# gráfico 11.1
try:
    # tabela ibge
    data_ibge = c.open_file(dbs_path, 'ibge_especiais.zip', 'zip', excel_name='tab03', skiprows=3)
    sheet = list(data_ibge.keys())[0]
    df_ibge = data_ibge[sheet]
    cols = df_ibge.columns.tolist()
    df_ibge.rename(columns={cols[0]: 'Região'}, inplace=True)
    df_ibge = df_ibge.query('Região == "Sergipe"').copy()
    df_ibge = df_ibge.melt(id_vars=['Região'], var_name='Ano', value_name='PIB')
    df_ibge['Ano'] = df_ibge['Ano'].astype(int)
    df_ibge.sort_values('Ano', ascending=True, inplace=True)  # ordena os dados por Ano
    df_ibge.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    df_ibge['PIB Variação'] = ((df_ibge['PIB'] / df_ibge['PIB'].shift(1)) - 1) * 100  # calcula a diferença entre o valor atual e o anterior

    # tabela siconfi
    data_siconfi = c.open_file(dbs_path, 'siconfi_DCA.csv', 'csv').query(
        'Anexo == "Anexo 3" and ' \
        'coluna.str.lower().str.contains("total") and ' \
        'conta.str.lower().str.startswith("receita corrente líquida (iii)")' , engine='python'
    )
    data_siconfi.rename(columns={'exercicio': 'Ano', 'valor': 'RCL'}, inplace=True)
    data_siconfi.sort_values('Ano', ascending=True, inplace=True)  # ordena os dados por Ano
    data_siconfi.reset_index(drop=True, inplace=True)  # reseta o índice

    # define o ano máximo e mínimo para a tabela ipca
    max_year = data_siconfi['Ano'].max()
    min_year = data_siconfi['Ano'].min()

    # tabela ipca
    data_ipca = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= @min_year and Ano <= @max_year', engine='python')
    data_ipca.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    data_ipca.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    data_ipca['Index'] = 100.00
    data_ipca['Diff'] = 0.00

    for row in range(1, len(data_ipca)):
        data_ipca.loc[row,'Diff'] = data_ipca.loc[row - 1, 'Valor'] / data_ipca.loc[row, 'Valor']  # calcula a diferença entre o valor atual e o anterior
        data_ipca.loc[row, 'Index'] = data_ipca.loc[row - 1, 'Index'] / data_ipca.loc[row, 'Diff']  # calcula o índice de preços

    # união da RCL com deflator IPCA
    df_merged = pd.merge(data_siconfi[['Ano', 'RCL']], data_ipca[['Ano', 'Index']], on='Ano', how='left', validate='1:1')
    df_merged['RCL deflacionada'] = (df_merged['RCL'] / df_merged['Index']) * 100
    df_merged.sort_values('Ano', ascending=True, inplace=True)  # ordena os dados por Ano
    df_merged['RCL Variação'] = ((df_merged['RCL deflacionada'] / df_merged['RCL deflacionada'].shift(1)) - 1) * 100  # calcula a diferença entre o valor atual e o anterior

    df_final = pd.merge(df_merged[['Ano', 'RCL Variação']], df_ibge[['Ano', 'PIB Variação']], on='Ano', how='left', validate='1:1')
    df_final.dropna(inplace=True)  # remove linhas com valores NaN
    df_final.rename(columns={'RCL Variação': 'RCL', 'PIB Variação': 'PIB'}, inplace=True)
    df_final = df_final[['Ano', 'PIB', 'RCL']]

    df_final.to_excel(os.path.join(sheets_path, 'g11.1.xlsx'), index=False, sheet_name='g11.1')

except Exception as e:
    errors['Gráfico 11.1'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g11.1--ate--g11.8--t11.1--t11.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
