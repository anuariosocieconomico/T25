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

session = c.create_session_with_retries()
# sidra contas regionais
url = 'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Contas_Regionais'

# download do arquivo especiais 2010
try:
    response = session.get(url, timeout=session.request_timeout, headers=c.headers)
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
    response =session.get(url_to_get, timeout=session.request_timeout, headers=c.headers)
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
            response = session.get(url_to_get_esp, timeout=session.request_timeout, headers=c.headers)
            df = pd.DataFrame(response.json())
            if last_year == 0:
                errors[url + ' (PIB)'] = 'Arquivo não encontrado em anos anteriores'
                raise Exception('Arquivo não encontrado em anos anteriores')

    # downloading e organização do arquivo: especiais 2010
    file = session.get(url_to_get_esp, timeout=session.request_timeout, headers=c.headers)
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


# siconfi RREO anexos 3 e 4
try:
    base_year = 2015
    current_year = datetime.now().year

    dfs_anexos = []
    for a in [('Anexo 3', 'RREO-Anexo%2003'), ('Anexo 4', 'RREO-Anexo%2004')]:
        dfs_year = []
        for y in range(base_year, current_year + 1):
            url = f'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo?an_exercicio={y}&nr_periodo=6&co_tipo_demonstrativo=RREO&' \
                f'no_anexo={a[1]}&co_esfera=E&id_ente=28'
            response = session.get(url, timeout=session.request_timeout, headers=c.headers)
            time.sleep(1)
            if response.status_code == 200 and len(response.json()['items']) > 1:
                df = pd.DataFrame(response.json()['items'])
                dfs_year.append(df)

        df_concat = pd.concat(dfs_year, ignore_index=True)
        df_concat['Anexo'] = a[0]
        dfs_anexos.append(df_concat)

    df_concat = pd.concat(dfs_anexos, ignore_index=True)
    c.to_csv(df_concat, dbs_path, 'siconfi_RREO.csv')

except Exception as e:
    errors['https://apidatalake.tesouro.gov.br (RREO)'] = traceback.format_exc()


# siconfi RREO para g11.7
try:
    base_year = 2015
    current_year = datetime.now().year

    dfs_states = []
    for k, v in c.mapping_states_ibge_code.items():
        dfs_year = []
        for y in range(base_year, current_year + 1):
            url = f'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo?an_exercicio={y}&nr_periodo=6&co_tipo_demonstrativo=RREO&' \
                f'no_anexo=RREO-Anexo%2004&co_esfera=&id_ente={v}'
            response = session.get(url, timeout=session.request_timeout, headers=c.headers)
            time.sleep(1)
            if response.status_code == 200 and len(response.json()['items']) > 1:
                df = pd.DataFrame(response.json()['items'])
                dfs_year.append(df)

        df_concat = pd.concat(dfs_year, ignore_index=True)
        df_concat['UF_ID'] = v
        df_concat['UF'] = k
        dfs_states.append(df_concat)

    df_concat = pd.concat(dfs_states, ignore_index=True)
    c.to_csv(df_concat, 'Scripts\Daniel\Diversos', 'siconfi_RREO_g11.7.csv')

except Exception as e:
    errors['https://apidatalake.tesouro.gov.br (RREO - g11.7)'] = traceback.format_exc()


# siconfi contas anuais DCA
try:
    base_year = 2013
    anexo_13 = 'Anexo%20I-C'
    anexo_all = 'DCA-Anexo%20I-C'
    current_year = datetime.now().year

    dfs_year = []
    for y in range(base_year, current_year + 1):
        ax = anexo_13 if y == 2013 else anexo_all
        url = f'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//dca?an_exercicio={y}&no_anexo={ax}&id_ente=28'
        response = session.get(url, timeout=session.request_timeout, headers=c.headers)
        time.sleep(1)
        if response.status_code == 200 and len(response.json()['items']) > 1:
            df = pd.DataFrame(response.json()['items'])
            dfs_year.append(df)

    df_concat = pd.concat(dfs_year, ignore_index=True)
    c.to_csv(df_concat, dbs_path, 'siconfi_DCA.csv')

except Exception as e:
    errors['https://apidatalake.tesouro.gov.br (DCA)'] = traceback.format_exc()


# siconfi Relatório de Gestão Fiscal - RGF
try:
    base_year = 2015
    poder = ['E', 'L', 'J', 'M', 'D']
    current_year = datetime.now().year

    start_time = time.time()  # Inicia o contador de tempo

    dfs_estados = []
    for k, v in c.mapping_states_ibge_code.items():

        dfs_year = []
        for y in range(base_year, current_year + 1):

            url = f'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rgf?' \
                f'an_exercicio={y}&in_periodicidade=Q&nr_periodo=3&co_tipo_demonstrativo=RGF&no_anexo=RGF-Anexo%2001&co_esfera=E&co_poder=E&id_ente={v}'
            response = session.get(url, timeout=session.request_timeout, headers=c.headers)
            time.sleep(1)
            if response.status_code == 200 and len(response.json()['items']) > 1:
                df = pd.DataFrame(response.json()['items'])
                dfs_year.append(df)

        df_concat = pd.concat(dfs_year, ignore_index=True)
        df_concat['UF'] = k
        df_concat['UF_ID'] = v
        dfs_estados.append(df_concat)
        print('Finalizado o Estado: ', k)

    df_final = pd.concat(dfs_estados, ignore_index=True)

    elapsed_time = time.time() - start_time  # Finaliza o contador de tempo
    print(f"Tempo total de execução: {elapsed_time:.2f} segundos")
    c.to_csv(df_final, 'Scripts\Daniel\Diversos', 'siconfi_RGF.csv')

except Exception as e:
    errors['https://apidatalake.tesouro.gov.br (RGF)'] = traceback.format_exc()


# boletim de arrecadação dos tributos estaduais
try:
    url = 'https://dados.gov.br/dados/api/publico/conjuntos-dados/17df5c58-16d7-431f-9e53-3ff1446ba72c'
    headers = {
        'accept': 'application/json',
        'chave-api-dados-abertos': os.environ.get('DADOS_ABERTOS_API', '')
    }
    response = session.get(url, timeout=session.request_timeout, headers=headers)
    response.raise_for_status()
    data = response.json()
    link = data['recursos'][0]['link']

    sheet = session.get(link, timeout=session.request_timeout, headers=c.headers, verify=False)
    sheet_data = c.open_file(file_path=sheet.content, ext='xls', skiprows=2)

    df = sheet_data[list(sheet_data.keys())[1]]
    df['data'] = pd.to_datetime(df['co_periodo'], format='%Y%m', errors='coerce')
    df['ano'] = df['data'].dt.year
    df['mes'] = df['data'].dt.month

    c.to_excel(df, dbs_path, 'boletim_arrecadacao.xlsx')
except Exception as e:
    errors['https://www.sefaz.se.gov.br/boletim_arrecadacao'] = traceback.format_exc()


# sidra 7358 - estimativa da população
url = 'https://apisidra.ibge.gov.br/values/t/7358/n1/all/n2/2/n3/28/v/all/p/all/c2/6794/c287/100362/c1933/all?formato=json'
try:
    data = session.get(url, timeout=session.request_timeout, headers=c.headers)
    df = pd.DataFrame(data.json())
    df = df[['D6N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df[['Ano', 'Valor']] = df[['Ano', 'Valor']].astype(int)

    c.to_excel(df, dbs_path, 'sidra_7358.xlsx')
except Exception as e:
    errors['Sidra 7358'] = traceback.format_exc()


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
    data_siconfi = c.open_file(dbs_path, 'siconfi_RREO.csv', 'csv')
    data_siconfi = data_siconfi.loc[
        (
            (data_siconfi['Anexo'] == "Anexo 3") &
            (data_siconfi['coluna'].str.lower().str.startswith("total")) &
            (
                (
                    data_siconfi['conta'].str.lower().str.startswith("receita corrente líquida (iii)") &
                    (data_siconfi['exercicio'] <= 2019)
                ) |
                (
                    data_siconfi['conta'].str.lower().str.startswith("receita corrente líquida") &
                    data_siconfi['conta'].str.lower().str.contains("despesa com pessoal")
                )
            )
        )
    ]
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
        data_ipca.loc[row,'Diff'] = 1 + (data_ipca.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
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


# gráfico 11.2
try:
    # tabela siconfi
    data_siconfi = c.open_file(dbs_path, 'siconfi_DCA.csv', 'csv')
    
    pattern = r"(\d+\.\d+\.\d+\.\d+\.\d+\.\d+\.\d+\s*-\s*)(.*)"
    data_siconfi['conta'] = data_siconfi['conta'].apply(
        lambda x: re.search(pattern, x).group(2).strip() if re.search(pattern, x) else x)
    
    pattern = r"(\d+\.\d+\.\d+\.\d+\.\d+\.\d+\.\d+\s*)(.*)"
    data_siconfi['conta'] = data_siconfi['conta'].apply(
        lambda x: re.search(pattern, x).group(2).strip() if re.search(pattern, x) else x)

    data_siconfi = data_siconfi.loc[
        (data_siconfi['conta'] == 'Receitas Correntes') |
        (data_siconfi['conta'].str.contains('Receita Tributária')) | (data_siconfi['conta'].str.contains('Taxas e Contribuições')) |
        (data_siconfi['conta'].str.contains('Cota-Parte do Fundo')) & (data_siconfi['conta'].str.contains('Estados')) |
        (data_siconfi['conta'].str.contains('Transferência')) & (data_siconfi['conta'].str.contains('Recursos Naturais')) & ~(
            data_siconfi['conta'].str.contains('Outras')) |
        (data_siconfi['conta'].str.contains('FUNDEB'))
    ]

    # receita corrente líquida
    df_rcl = data_siconfi.loc[
        # condicional para filtrar as linhas da receita
        ((data_siconfi['conta'].str.contains('Receitas Correntes')) & (data_siconfi['coluna'].str.contains('Receitas'))) |
        # condicional para filtrar as linhas das deduções
        ((data_siconfi['conta'].str.contains('Receitas Correntes')) & (data_siconfi['coluna'].str.contains('Deduções')))
    ]
    # renomeia as colunas para facilitar a manipulação e agregação
    df_rcl.loc[df_rcl['coluna'].str.contains('Receitas'), 'cod_conta'] = 'Receitas'
    df_rcl.loc[df_rcl['coluna'].str.contains('Deduções'), 'cod_conta'] = 'Deduções'
    # agrupa os dados por exercício e código da conta, somando os valores; depois, pivota os dados para ter as contas como colunas
    df_rcl = df_rcl.groupby(['exercicio', 'cod_conta'])['valor'].sum().reset_index()
    df_rcl_pivoted = df_rcl.pivot(index='exercicio', columns='cod_conta', values='valor').reset_index()
    df_rcl_pivoted['RCL'] = df_rcl_pivoted['Receitas'] - df_rcl_pivoted['Deduções']

    # receita tributária líquida
    df_rt = data_siconfi.loc[
        # condicional para filtrar as linhas da receita tributária
        (
            ((data_siconfi['conta'].str.contains('Tributária')) | (data_siconfi['conta'].str.contains('Taxas e Contribuições'))) & 
            (data_siconfi['coluna'].str.contains('Receitas'))
        ) |
        # condicional para filtrar as linhas das deduções tributárias
        (
            ((data_siconfi['conta'].str.contains('Tributária')) | (data_siconfi['conta'].str.contains('Taxas e Contribuições'))) &
            (data_siconfi['coluna'].str.contains('Deduções'))
        )
    ]
    df_rt.loc[df_rt['coluna'].str.contains('Receitas'), 'cod_conta'] = 'Receitas'
    df_rt.loc[df_rt['coluna'].str.contains('Deduções'), 'cod_conta'] = 'Deduções'
    df_rt = df_rt.groupby(['exercicio', 'cod_conta'])['valor'].sum().reset_index()
    df_rt_pivoted = df_rt.pivot(index='exercicio', columns='cod_conta', values='valor').reset_index()
    df_rt_pivoted['RT'] = df_rt_pivoted['Receitas'] - df_rt_pivoted['Deduções']

    # FPE líquido
    df_fpe = data_siconfi.loc[
        # condicional para filtrar as linhas da receita tributária
        (
            (data_siconfi['conta'].str.contains('Cota-Parte do Fundo')) & (data_siconfi['conta'].str.contains('Estados')) &
            (data_siconfi['coluna'].str.contains('Receitas'))
        ) |
        # condicional para filtrar as linhas das deduções tributárias
        (
            (data_siconfi['conta'].str.contains('Cota-Parte do Fundo')) & (data_siconfi['conta'].str.contains('Estados')) &
            (data_siconfi['coluna'].str.contains('Deduções'))
        )
    ]
    df_fpe.loc[df_fpe['coluna'].str.contains('Receitas'), 'cod_conta'] = 'Receitas'
    df_fpe.loc[df_fpe['coluna'].str.contains('Deduções'), 'cod_conta'] = 'Deduções'
    df_fpe = df_fpe.groupby(['exercicio', 'cod_conta'])['valor'].sum().reset_index()
    df_fpe_pivoted = df_fpe.pivot(index='exercicio', columns='cod_conta', values='valor').reset_index()
    df_fpe_pivoted['FPE'] = df_fpe_pivoted['Receitas'] - df_fpe_pivoted['Deduções']

    # receita de exploração de recursos naturais
    df_rn = data_siconfi.loc[
        # condicional para filtrar as linhas da receita tributária
        (
            (data_siconfi['conta'].str.contains('Transferência')) & (data_siconfi['conta'].str.contains('Recursos Naturais')) &
            (data_siconfi['coluna'].str.contains('Receitas'))
        ) |
        # condicional para filtrar as linhas das deduções tributárias
        (
            (data_siconfi['conta'].str.contains('Transferência')) & (data_siconfi['conta'].str.contains('Recursos Naturais')) &
            (data_siconfi['coluna'].str.contains('Deduções'))
        )
    ]
    df_rn.loc[df_rn['coluna'].str.contains('Receitas'), 'cod_conta'] = 'Receitas'
    df_rn.loc[df_rn['coluna'].str.contains('Deduções'), 'cod_conta'] = 'Deduções'
    df_rn = df_rn.groupby(['exercicio', 'cod_conta'])['valor'].sum().reset_index()
    df_rn_pivoted = df_rn.pivot(index='exercicio', columns='cod_conta', values='valor').reset_index()
    df_rn_pivoted['RN'] = df_rn_pivoted['Receitas'] - df_rn_pivoted['Deduções']

    # FUNDEB líquido
    df_fundeb = data_siconfi.loc[
        # condicional para filtrar as linhas da receita tributária
        (
            (data_siconfi['conta'].str.contains('FUNDEB')) &
            (data_siconfi['coluna'].str.contains('Receitas'))
        ) |
        # condicional para filtrar as linhas das deduções tributárias
        (
            (data_siconfi['conta'].str.contains('FUNDEB')) &
            (data_siconfi['coluna'].str.contains('Deduções'))
        )
    ]
    df_fundeb.loc[df_fundeb['coluna'].str.contains('Receitas'), 'cod_conta'] = 'Receitas'
    df_fundeb.loc[df_fundeb['coluna'].str.contains('Deduções'), 'cod_conta'] = 'Deduções'
    df_fundeb = df_fundeb.groupby('exercicio')['valor'].sum().reset_index()
    df_fundeb.rename(columns={'valor': 'FUNDEB'}, inplace=True)

    df_final = df_rcl_pivoted[['exercicio', 'RCL']].merge(
        df_rt_pivoted[['exercicio', 'RT']],
        on='exercicio',
        how='left',
        validate='1:1'
    ).merge(
        df_fpe_pivoted[['exercicio', 'FPE']],
        on='exercicio',
        how='left',
        validate='1:1'
    ).merge(
        df_rn_pivoted[['exercicio', 'RN']],
        on='exercicio',
        how='left',
        validate='1:1'
    ).merge(
        df_fundeb,
        on='exercicio',
        how='left',
        validate='1:1'
    )

    df_final['Receitas tributárias'] = (df_final['RT'] / df_final['RCL']) * 100
    df_final['FPE'] = (df_final['FPE'] / df_final['RCL']) * 100
    df_final['Fundeb'] = (df_final['FUNDEB'] / df_final['RCL']) * 100
    df_final['Receita de exploração de RN'] = (df_final['RN'] / df_final['RCL']) * 100

    df_final.rename(columns={'exercicio': 'Ano'}, inplace=True)
    df_final['Ano'] = df_final['Ano'].astype(str)
    df_final = df_final[['Ano', 'Receitas tributárias', 'FPE', 'Fundeb', 'Receita de exploração de RN']]

    c.to_excel(df_final, sheets_path, 'g11.2.xlsx')

except Exception as e:
    errors['Gráfico 11.2'] = traceback.format_exc()


# tabela 11.1
try:
    # tabela siconfi
    data_siconfi = c.open_file(dbs_path, 'siconfi_DCA.csv', 'csv')

    pattern = r"(\d+\.\d+\.\d+\.\d+\.\d+\.\d+\.\d+\s*-\s*)(.*)"
    data_siconfi['conta'] = data_siconfi['conta'].apply(
        lambda x: re.search(pattern, x).group(2).strip() if re.search(pattern, x) else x)
    
    pattern = r"(\d+\.\d+\.\d+\.\d+\.\d+\.\d+\.\d+\s*)(.*)"
    data_siconfi['conta'] = data_siconfi['conta'].apply(
        lambda x: re.search(pattern, x).group(2).strip() if re.search(pattern, x) else x)

    df_siconfi = data_siconfi.loc[
        # condicional para filtrar as linhas da conta
        (
            ((data_siconfi['conta'].str.contains('Receita Tributária')) | (data_siconfi['conta'].str.contains('Taxas e Contribuições'))) |
            ((data_siconfi['conta'].str.contains('Imposto sobre Op.')) | (data_siconfi['conta'].str.contains('Imposto sobre Operações'))) |
            (data_siconfi['conta'].str.startswith('Imposto sobre a Propriedade de Veículos')) |
            ((data_siconfi['conta'] == 'Transferências da União') | (data_siconfi['conta'] == 'Transferências da União e de suas Entidades')) |
            ((data_siconfi['conta'].str.contains('Cota-Parte do Fundo')) & (data_siconfi['conta'].str.contains('Estados'))) |
            (data_siconfi['conta'].str.contains('FUNDEB')) |
            (
                (data_siconfi['conta'].str.contains('Transferência')) & (data_siconfi['conta'].str.contains('Recursos Naturais')) & 
                ~(data_siconfi['conta'].str.contains('Outras'))
            )
        ) &
        # condicional para filtrar as linhas da coluna
        (~(data_siconfi['coluna'].str.contains('Deduções')) & (data_siconfi['cod_conta'].str.startswith('RO1')))
        , ['exercicio', 'conta', 'coluna', 'valor']
    ].copy()

    df_siconfi['conta_padronizada'] = ''
    df_siconfi['coluna_padronizada'] = ''

    # renomeia as contas para padronizar as receitas tributárias
    df_siconfi.loc[
        (
            (df_siconfi['conta'].str.contains('Receita Tributária')) | (df_siconfi['conta'].str.contains('Taxas e Contribuições')) |
            (df_siconfi['conta'].str.startswith('Imposto sobre a Propriedade de Veículos')) |
            (df_siconfi['conta'].str.contains('Imposto sobre Operações')) | (df_siconfi['conta'].str.contains('Imposto sobre Op.'))
        )
        , 'conta_padronizada'
    ] = 'Receitas Tributárias'

    # renomeia as contas para padronizar as receitas de exploração de recursos naturais
    df_siconfi.loc[
        (df_siconfi['conta'].str.contains('Transferência')) & (df_siconfi['conta'].str.contains('Recursos Naturais'))
        , 'conta_padronizada'
    ] = 'Receitas de Exploração de Recursos Naturais'

    # renomeia as contas para padronizar as transferências federais
    df_siconfi.loc[
        (
            (df_siconfi['conta'] == 'Transferências da União') | (df_siconfi['conta'] == 'Transferências da União e de suas Entidades') |
            (df_siconfi['conta'].str.contains('Cota-Parte do Fundo')) & (df_siconfi['conta'].str.contains('Estados')) |
            (df_siconfi['conta'].str.contains('FUNDEB'))
        )
        , 'conta_padronizada'
    ] = 'Transferências Federais'

    # renomeia as colunas para padronizar as receitas tributárias
    df_siconfi.loc[
        (df_siconfi['conta'].str.contains('Receita Tributária')) | (df_siconfi['conta'].str.contains('Taxas e Contribuições'))
        , 'coluna_padronizada'
    ] = 'Total (1)'

    # renomeia as colunas para padronizar impostos sobre operações
    df_siconfi.loc[
        (df_siconfi['conta'].str.contains('Imposto sobre Op.')) | (df_siconfi['conta'].str.contains('Imposto sobre Operações'))
        , 'coluna_padronizada'
    ] = 'ICMS'

    # renomeia as colunas para padronizar impostos sobre a propriedade de veículos
    df_siconfi.loc[
        (df_siconfi['conta'].str.startswith('Imposto sobre a Propriedade de Veículos'))
        , 'coluna_padronizada'
    ] = 'IPVA'

    # renomeia as colunas para padronizar as transferências federais
    df_siconfi.loc[
        (df_siconfi['conta'] == 'Transferências da União') | (df_siconfi['conta'] == 'Transferências da União e de suas Entidades')
        , 'coluna_padronizada'
    ] = 'Total (2)'

    # renomeia as colunas para padronizar a participação dos estados no FPE
    df_siconfi.loc[
        (df_siconfi['conta'].str.contains('Cota-Parte do Fundo')) & (df_siconfi['conta'].str.contains('Estados'))
        , 'coluna_padronizada'
    ] = 'Fundo de Participação dos Estados'

    # renomeia as colunas para padronizar o FUNDEB
    df_siconfi.loc[
        (df_siconfi['conta'].str.contains('FUNDEB'))
        , 'coluna_padronizada'
    ] = 'Fundeb'

    # renomeia as colunas para padronizar as receitas de exploração de recursos naturais
    df_siconfi.loc[
        (df_siconfi['conta'].str.contains('Transferência')) & (df_siconfi['conta'].str.contains('Recursos Naturais'))
        , 'coluna_padronizada'
    ] = 'Total (3)'

    df_siconfi.rename(columns={'exercicio': 'Ano', 'conta_padronizada': 'Receitas', 'coluna_padronizada': 'Tipo', 'valor': 'Valor'}, inplace=True)
    df_siconfi.sort_values(by=['Receitas', 'Tipo'], ascending=True, inplace=True)  # ordena os dados por Ano
    min_year = df_siconfi['Ano'].min()
    max_year = df_siconfi['Ano'].max()

    # tabela de deflator IPCA
    data_ipca = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= @min_year and Ano <= @max_year', engine='python')
    data_ipca.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    data_ipca.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    data_ipca['Index'] = 100.00
    data_ipca['Diff'] = 0.00

    for row in range(1, len(data_ipca)):
        # data_ipca.loc[row,'Diff'] = data_ipca.loc[row - 1, 'Valor'] / data_ipca.loc[row, 'Valor']  # calcula a diferença entre o valor atual e o anterior
        data_ipca.loc[row,'Diff'] = 1 + (data_ipca.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
        data_ipca.loc[row, 'Index'] = data_ipca.loc[row - 1, 'Index'] / data_ipca.loc[row, 'Diff']  # calcula o índice de preços

    # união da tabela siconfi com deflator IPCA
    df_merged = df_siconfi[['Ano', 'Receitas', 'Tipo', 'Valor']].merge(
        data_ipca[['Ano', 'Index']],
        on='Ano',
        how='left',
        validate='m:1'
    )
    df_merged['Valor Deflacionado'] = (df_merged['Valor'] / df_merged['Index']) * 100  # deflaciona os valores
    df_merged['Variação'] = (df_merged.groupby(['Receitas', 'Tipo'])['Valor Deflacionado'].pct_change()) * 100  # calcula a diferença entre o valor atual e o anterior

    df_final = df_merged[['Ano', 'Receitas', 'Tipo', 'Variação']].copy()
    df_final.rename(columns={'Variação': 'Valor'}, inplace=True)
    df_final.dropna(inplace=True)  # remove linhas com valores NaN

    df_final.to_excel(os.path.join(sheets_path, 't11.1.xlsx'), index=False, sheet_name='t11.1')

except Exception as e:
    errors['Tabela 11.1'] = traceback.format_exc()


# g11.3
try:
    df = c.open_file(dbs_path, 'boletim_arrecadacao.xlsx', 'xls', sheet_name='Sheet1')[
        ['data', 'ano', 'mes', 'id_uf', 'va_icms_primario', 'va_icms_secundario', 'va_icms_terciario']
    ]
    df_grouped = df.groupby(['ano', 'id_uf'], as_index=False)[['va_icms_primario', 'va_icms_secundario', 'va_icms_terciario']].sum()

    # se
    ne_codes = [c.mapping_states_abbreviation[state] for state in c.ne_states]
    df_se = df_grouped.query('id_uf == "SE"').copy()
    df_se.loc[:, 'id_uf'] = 'Sergipe'
    # ne
    df_ne = df_grouped.query('id_uf in @ne_codes').copy()
    df_ne.loc[:, 'id_uf'] = 'Nordeste'
    df_ne = df_ne.groupby(['ano', 'id_uf'], as_index=False)[['va_icms_primario', 'va_icms_secundario', 'va_icms_terciario']].sum()
    # br
    df_br = df_grouped.groupby('ano', as_index=False)[['va_icms_primario', 'va_icms_secundario', 'va_icms_terciario']].sum()
    df_br['id_uf'] = 'Brasil'
    df_br = df_br[['ano', 'id_uf', 'va_icms_primario', 'va_icms_secundario', 'va_icms_terciario']]
    df_br = df_br.groupby(['ano', 'id_uf'], as_index=False)[['va_icms_primario', 'va_icms_secundario', 'va_icms_terciario']].sum()

    df_concat = pd.concat([df_br, df_ne, df_se], ignore_index=True)
    total = df_concat[['va_icms_primario', 'va_icms_secundario', 'va_icms_terciario']].sum(axis=1)
    df_concat['Primário'] = (df_concat['va_icms_primario'] / total) * 100
    df_concat['Secundário'] = (df_concat['va_icms_secundario'] / total) * 100
    df_concat['Terciário'] = (df_concat['va_icms_terciario'] / total) * 100

    df_final = df_concat[['id_uf', 'ano', 'Primário', 'Secundário', 'Terciário']].copy()
    df_final.rename(columns={'ano': 'Ano', 'id_uf': 'Região'}, inplace=True)
    df_final.sort_values(by=['Região', 'Ano'], ascending=[False, True], inplace=True)
    
    df_final.to_excel(os.path.join(sheets_path, 'g11.3.xlsx'), index=False, sheet_name='g11.3')

except Exception as e:
    errors['Gráfico 11.3'] = traceback.format_exc()


# g11.4
try:
    df = c.open_file(dbs_path, 'boletim_arrecadacao.xlsx', 'xls', sheet_name='Sheet1')[
        ['data', 'ano', 'mes', 'id_uf', 'va_icms_primario', 'va_icms_secundario', 'va_icms_terciario']
    ].query('id_uf == "SE"', engine='python')
    df_grouped = df.groupby(['ano', 'id_uf'], as_index=False)[['va_icms_primario', 'va_icms_secundario', 'va_icms_terciario']].sum()
    max_year = df_grouped['ano'].max()
    min_year = df_grouped['ano'].min()

    df_deflator = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= @min_year and Ano <= @max_year', engine='python')
    df_deflator.rename(columns={'Ano': 'ano'}, inplace=True)
    
    # tratamento do deflator
    df_deflator.sort_values('ano', ascending=False, inplace=True)  # ordena os dados por Ano
    df_deflator.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    df_deflator['Index'] = 100.00
    df_deflator['Diff'] = None

    for row in range(1, len(df_deflator)):
        # df_deflator.loc[row,'Diff'] = df_deflator.loc[row - 1, 'Valor'] / df_deflator.loc[row, 'Valor']  # calcula a diferença entre o valor atual e o anterior
        df_deflator.loc[row,'Diff'] = 1 + (df_deflator.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
        df_deflator.loc[row, 'Index'] = df_deflator.loc[row - 1, 'Index'] / df_deflator.loc[row, 'Diff']  # calcula o índice de preços

    df_merged = df_grouped.merge(df_deflator[['ano', 'Index']], on='ano', how='left', validate='1:1').dropna(axis=0)
    df_merged['Prim'] = (df_merged['va_icms_primario'] / df_merged['Index']) * 100
    df_merged['Sec'] = (df_merged['va_icms_secundario'] / df_merged['Index']) * 100
    df_merged['Terc'] = (df_merged['va_icms_terciario'] / df_merged['Index']) * 100

    df_final = df_merged[['ano', 'Prim', 'Sec', 'Terc']].copy()
    df_final.rename(columns={'ano': 'Ano'}, inplace=True)
    df_final.sort_values(by='Ano', ascending=True, inplace=True)

    df_final.to_excel(os.path.join(sheets_path, 'g11.4.xlsx'), index=False, sheet_name='g11.4')

except Exception as e:
    errors['Gráfico 11.4'] = traceback.format_exc()


# t11.2
try:
    df = c.open_file(dbs_path, 'boletim_arrecadacao.xlsx', 'xls', sheet_name='Sheet1')[[
        'data', 'ano', 'id_uf', 'va_icms_terciario', 'va_icms_terciario_atacadista', 'va_icms_terciario_varejista', 'va_icms_terciario_transportes',
        'va_icms_terciario_comunicacao', 'va_icms_terciario_outros', 'va_icms_energia_terciario', 'va_icms_combustiveis_terciario'
    ]].query('id_uf == "SE"', engine='python')
    df_grouped = df.groupby('ano', as_index=False).sum(numeric_only=True)
    max_year = df_grouped['ano'].max()
    min_year = df_grouped['ano'].min()

    df_deflator = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= @min_year and Ano <= @max_year', engine='python')
    df_deflator.rename(columns={'Ano': 'ano'}, inplace=True)
    
    # tratamento do deflator
    df_deflator.sort_values('ano', ascending=False, inplace=True)  # ordena os dados por Ano
    df_deflator.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    df_deflator['Index'] = 100.00
    df_deflator['Diff'] = None

    for row in range(1, len(df_deflator)):
        df_deflator.loc[row,'Diff'] = 1 + (df_deflator.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
        df_deflator.loc[row, 'Index'] = df_deflator.loc[row - 1, 'Index'] / df_deflator.loc[row, 'Diff']  # calcula o índice de preços

    df_merged = df_grouped.merge(df_deflator[['ano', 'Index']], on='ano', how='left', validate='1:1').dropna(axis=0)
    for col in df_merged.columns.to_list()[1:-1]:
        df_merged[col] = (df_merged[col] / df_merged['Index']) * 100
        df_merged[col + ' Diff'] = df_merged[col].pct_change() * 100

    cols_to_melt = [col for col in df_merged.columns.to_list() if col.endswith('Diff')]
    df_melted = df_merged.melt(id_vars=['ano'], value_vars=cols_to_melt, var_name='Atividade', value_name='Valor')
    df_melted['Atividade'] = df_melted['Atividade'].map({
        'va_icms_terciario Diff': 'Total',
        'va_icms_terciario_atacadista Diff': 'Comércio Atacadista',
        'va_icms_terciario_varejista Diff': 'Comércio Varejista',
        'va_icms_terciario_transportes Diff': 'Serviços de Transporte',
        'va_icms_terciario_comunicacao Diff': 'Serviços de Comunicação',
        'va_icms_terciario_outros Diff': 'Outros ICMS',
        'va_icms_energia_terciario Diff': 'Energia Elétrica Terciário',
        'va_icms_combustiveis_terciario Diff': 'Petróleo-Combustível-Lubrificantes Terciário'
    })
    df_melted.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_melted.dropna(axis=0, inplace=True)
    df_melted.sort_values(by=['Atividade', 'ano'], inplace=True)
    df_melted.rename(columns={'ano': 'ANO'}, inplace=True)

    df_melted.to_excel(os.path.join(sheets_path, 't11.2.xlsx'), index=False, sheet_name='t11.2')

except Exception as e:
    errors['Tabela 11.2'] = traceback.format_exc()


# g11.5
try:
    # dados siconfi
    df = c.open_file('Scripts\Daniel\Diversos', 'siconfi_RGF.csv', 'csv').query(
        'coluna.str.lower() == "valor" and conta.str.lower().str.startswith("despesa total com pessoal")' , engine='python'
    )[['exercicio', 'cod_ibge', 'uf', 'UF', 'populacao', 'valor']]
    df.rename(columns={'exercicio': 'Ano'}, inplace=True)
    max_year = df['Ano'].max()
    min_year = df['Ano'].min()

    # dados deflatores
    df_deflator = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= @min_year and Ano <= @max_year', engine='python')
    df_deflator.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    df_deflator.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    df_deflator['Index'] = 100.00
    df_deflator['Diff'] = None

    for row in range(1, len(df_deflator)):
        df_deflator.loc[row,'Diff'] = 1 + (df_deflator.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
        df_deflator.loc[row, 'Index'] = df_deflator.loc[row - 1, 'Index'] / df_deflator.loc[row, 'Diff']  # calcula o índice de preços

    # dados populacionais
    df_pop = c.open_file(dbs_path, 'sidra_7358.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= @min_year and Ano <= @max_year', engine='python')
    df_pop.rename(columns={'Valor': 'Pop'}, inplace=True)

    # dados do nordeste
    df_ne = df.query('UF in @c.ne_states', engine='python').copy()
    df_ne.rename(columns={'UF': 'Região'}, inplace=True)
    df_ne.loc[:, 'Região'] = 'Nordeste'
    df_ne_final = df_ne.groupby(['Ano', 'Região'], as_index=False)['valor'].sum()

    # dados do brasil
    df_br = df.copy()
    df_br.rename(columns={'UF': 'Região'}, inplace=True)
    df_br.loc[:, 'Região'] = 'Brasil'
    df_br_final = df_br.groupby(['Ano', 'Região'], as_index=False)['valor'].sum()

    # dados do sergipe
    df_se = df.query('UF == "Sergipe"', engine='python').copy()
    df_se.rename(columns={'UF': 'Região'}, inplace=True)
    df_se.loc[:, 'Região'] = 'Sergipe'
    df_se_final = df_se.groupby(['Ano', 'Região'], as_index=False)['valor'].sum()

    # unindo as tabelas
    df_siconfi = pd.concat([df_br_final, df_ne_final, df_se_final], ignore_index=True)
    df_merged = df_siconfi.merge(df_deflator[['Ano', 'Index']], on='Ano', how='left', validate='m:1').merge(
        df_pop[['Ano', 'Região', 'Pop']], on=['Ano', 'Região'], how='left', validate='1:1'
    )
    df_merged['Valor'] = (df_merged['valor'] / df_merged['Index']) * 100
    df_merged['Valor'] = df_merged['Valor'] / df_merged['Pop']

    df_final = df_merged[['Ano', 'Região', 'Valor']]
    df_final.to_excel(os.path.join(sheets_path, 'g11.5.xlsx'), index=False, sheet_name='g11.5')

except Exception as e:
    errors['Gráfico 11.5'] = traceback.format_exc()


# g11.6
try:
    # dados siconfi
    df = c.open_file('Scripts\Daniel\Diversos', 'siconfi_RGF.csv', 'csv').query(
        'coluna.str.lower().str.startswith("% sobre a rcl") and conta.str.lower().str.startswith("despesa total com pessoal")' , engine='python'
    )[['exercicio', 'UF', 'valor']]
    df.rename(columns={'exercicio': 'Ano', 'UF': 'Região', 'valor': 'Valor'}, inplace=True)

    # último ano
    df_last_year = df[df['Ano'] == df['Ano'].max()].copy()
    br_mean = df_last_year['Valor'].mean()
    ne_mean = df_last_year.query('Região in @c.ne_states')['Valor'].mean()

    df_last_year['Posição'] = df_last_year['Valor'].rank(method='first', ascending=False)
    df_last_year.sort_values(by='Posição', ascending=True, inplace=True)
    temp_df = pd.DataFrame({'Ano': [df_last_year['Ano'].max()] * 2, 'Região': ['BR', 'NE'], 'Valor': [br_mean, ne_mean], 'Posição': [np.nan, np.nan]})

    df_final_last_year = pd.concat(
        [
            df_last_year.query('`Posição` <= 6 | `Região` == "Sergipe"', engine='python').copy(),
            temp_df
        ],
        ignore_index=True
    )
    # muda o nome das regiões para as siglas, ignorando Brasil e Nordeste
    df_final_last_year['Região'] = df_final_last_year['Região'].apply(lambda x: c.mapping_states_abbreviation[x] if x not in ['BR', 'NE'] else x)

    df_final = df_final_last_year[['Região', 'Valor', 'Posição']]
    df_final.to_excel(os.path.join(sheets_path, 'g11.6a.xlsx'), index=False, sheet_name='g11.6a')

    # média histórica
    df_mean = df.groupby(['Região'], as_index=False)['Valor'].mean()
    df_mean['Posição'] = df_mean['Valor'].rank(method='first', ascending=False)
    df_mean.sort_values(by='Posição', ascending=True, inplace=True)
    
    br_mean = df_mean['Valor'].mean()
    ne_mean = df_mean.query('Região in @c.ne_states')['Valor'].mean()
    temp_df = pd.DataFrame({'Região': ['Brasil', 'Nordeste'], 'Valor': [br_mean, ne_mean], 'Posição': [np.nan, np.nan]})

    df_mean_final = pd.concat(
        [
            df_mean.query('`Posição` <= 6 | `Região` == "Sergipe"', engine='python').copy(),
            temp_df
        ],
        ignore_index=True
    )
    # muda o nome das regiões para as siglas, ignorando Brasil e Nordeste
    df_mean_final['Região'] = df_mean_final['Região'].apply(lambda x: c.mapping_states_abbreviation[x] if x not in ['Brasil', 'Nordeste'] else x)

    df_mean_final.to_excel(os.path.join(sheets_path, 'g11.6b.xlsx'), index=False, sheet_name='g11.6b')

except Exception as e:
    errors['Gráfico 11.6'] = traceback.format_exc()


# g11.7
try:
    # dados siconfi
    df = c.open_file('Scripts\Daniel\Diversos', 'siconfi_RREO_g11.7.csv', 'csv').query(
        'coluna.str.lower().str.startswith("despesas empenhadas até o bimestre") and ' \
        'conta.str.lower().str.startswith("resultado previdenciário") and ' \
        'cod_conta.str.lower().str.endswith("financeiro")',
        engine='python'
    )
    df = df.loc[
        (df['coluna'].str.split(' / ').str[-1] == df['exercicio'].astype(str)) |  # filtra os dados do ano corrente
        (df['coluna'].str.contains('d')),  # filtra os dados do ano corrente
        # ['exercicio', 'UF', 'valor']
    ]
    df.rename(columns={'exercicio': 'Ano'}, inplace=True)
    max_year = df['Ano'].max()
    min_year = df['Ano'].min()

    df_grouped = df.groupby(['Ano', 'UF'], as_index=False)['valor'].sum()

    # deflator
    df_deflator = c.open_file(dbs_path, 'ipeadata_ipca.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= @min_year and Ano <= @max_year', engine='python')
    df_deflator.sort_values('Ano', ascending=False, inplace=True)  # ordena os dados por Ano
    df_deflator.reset_index(drop=True, inplace=True)  # reseta o índice do DataFrame
    df_deflator['Index'] = 100.00
    df_deflator['Diff'] = None

    for row in range(1, len(df_deflator)):
        df_deflator.loc[row,'Diff'] = 1 + (df_deflator.loc[row - 1, 'Valor'] / 100)  # calcula a diferença entre o valor atual e o anterior
        df_deflator.loc[row, 'Index'] = df_deflator.loc[row - 1, 'Index'] / df_deflator.loc[row, 'Diff']  # calcula o índice de preços

    # população
    df_pop = c.open_file(dbs_path, 'sidra_7358.xlsx', 'xls', sheet_name='Sheet1').query('Ano >= @min_year and Ano <= @max_year', engine='python')
    df_pop.rename(columns={'Valor': 'Pop', 'Região': 'UF'}, inplace=True)

    # estratos regionais
    df_br = df_grouped.copy()
    df_br.loc[:, 'UF'] = 'Brasil'
    df_br_grouped = df_br.groupby(['Ano', 'UF'], as_index=False)['valor'].sum()

    df_ne = df_grouped.query('UF in @c.ne_states', engine='python').copy()
    df_ne.loc[:, 'UF'] = 'Nordeste'
    df_ne_grouped = df_ne.groupby(['Ano', 'UF'], as_index=False)['valor'].sum()

    df_se = df_grouped.query('UF == "Sergipe"', engine='python').copy()

    # unindo as tabelas
    df_concat = pd.concat([df_br_grouped, df_ne_grouped, df_se], ignore_index=True)
    df_merged = df_concat.merge(df_deflator[['Ano', 'Index']], how='left', on='Ano', validate='m:1').merge(
        df_pop[['Ano', 'UF', 'Pop']], how='left', on=['Ano', 'UF'], validate='1:1'
    )
    df_merged['Valor'] = (df_merged['valor'] / df_merged['Index']) * 100  # deflaciona os valores
    df_merged['Valor/Pop'] = df_merged['Valor'] / df_merged['Pop']  # calcula o valor por habitante

    df_final = df_merged[['Ano', 'UF', 'Valor/Pop']].pivot(index='Ano', columns='UF', values='Valor/Pop').reset_index()
    df_final.to_excel(os.path.join(sheets_path, 'g11.7.xlsx'), index=False, sheet_name='g11.7')

except Exception as e:
    errors['Gráfico 11.7'] = traceback.format_exc()


# g11.8
try:
    # dados siconfi
    df = c.open_file('Scripts\Daniel\Diversos', 'siconfi_RREO_g11.7.csv', 'csv')
    # filtragem para selecionar as variáveis certas de coluna, conta e cod_conta
    # coluna segue um padrão para todos os anos, pelo menos nesse primeiro momento
    # cod_conta garante que os dados são do plano financeiro, e não previdenciário
    # é preciso coletar dois valores, o resultado e a despesa; o resultado segue um padrão para todos os anos
    # já a despesa, a partir de certo ano a variável muda de nome (total das despesas do fundo em repartição)
    df_filtered = df.loc[
        (df["coluna"].str.lower().str.startswith("despesas empenhadas até o bimestre")) &
        (
            (df["conta"].str.lower().str.startswith("resultado previdenciário")) |
            (df["conta"].str.lower().str.startswith("total das despesas previdenciárias")) |
            (df["conta"].str.lower().str.startswith("total das despesas do fundo em repartição"))
        ) &
        (df["cod_conta"].str.lower().str.endswith("financeiro"))
    ]
    # por fim é realizada mais uma filtragem para coletar as linhas corretas de dados de anos antigos
    # para determinado ano, exemplo 2020, há dados de despesas no ano corrente e em outros anos
    # então a filtragem a seguir garante que apenas os dados do ano corrente sejam coletados; após ' / ' há o ano de referência
    # já em anos mais atuais, há a letra 'd' que indica que o dado é do ano corrente
    df_filtered = df_filtered.loc[
        (df_filtered['coluna'].str.split(' / ').str[-1] == df_filtered['exercicio'].astype(str)) |
        (df_filtered['coluna'].str.contains('(d)'))
    ]
    max_year = df_filtered['exercicio'].max()
    
    # tratamento dos dados
    df_pivoted = df_filtered.copy()
    df_pivoted['Conta Padronizada'] = df_pivoted['conta'].apply(lambda x: 'Resultado' if x.lower().startswith('resultado') else 'Despesas')
    df_pivoted = df_pivoted.pivot(index=['exercicio', 'UF'], columns='Conta Padronizada', values='valor').reset_index()
    df_pivoted['Proporção'] = (df_pivoted['Resultado'] / (df_pivoted['Despesas'] + df_pivoted['Resultado'])) * 100
    df_pivoted.loc[df_pivoted['Despesas'] + df_pivoted['Resultado'] == 0, 'Proporção'] = 1

    # tabela média história dos estados
    df_total = df_pivoted.groupby('UF', as_index=False)['Proporção'].mean()
    df_total['Posição'] = df_total['Proporção'].rank(method='dense', ascending=False)
    df_total = df_total.loc[(df_total['Posição'] <= 6) | (df_total['UF'] == 'Sergipe')].copy()
    df_total.sort_values('Posição', inplace=True)
    df_total['UF'] = df_total['UF'].apply(lambda x: c.mapping_states_abbreviation[x])

    df_br_total = df_pivoted.copy()
    df_br_total.loc[:, 'UF'] = 'BR'
    df_br_total = df_br_total.groupby('UF', as_index=False)['Proporção'].mean()
    df_br_total['Posição'] = np.nan

    df_ne_total = df_pivoted.query('UF in @c.ne_states', engine='python').copy()
    df_ne_total.loc[:, 'UF'] = 'NE'
    df_ne_total = df_ne_total.groupby('UF', as_index=False)['Proporção'].mean()
    df_ne_total['Posição'] = np.nan

    df_total_final = pd.concat([df_total, df_br_total, df_ne_total], ignore_index=True)
    df_total_final.rename(columns={'UF': 'Região', 'Proporção': 'Valor'}, inplace=True)

    df_total_final.to_excel(os.path.join(sheets_path, 'g11.8b.xlsx'), index=False, sheet_name='g11.8b')

    # tabela do último ano
    df_last_year = df_pivoted.query('exercicio == @max_year', engine='python')[['UF', 'Proporção']].copy()
    df_last_year['Posição'] = df_last_year['Proporção'].rank(method='dense', ascending=False)
    df_last_year = df_last_year.loc[(df_last_year['Posição'] <= 6) | (df_last_year['UF'] == 'Sergipe')].copy()
    df_last_year.sort_values('Posição', inplace=True)
    df_last_year['UF'] = df_last_year['UF'].apply(lambda x: c.mapping_states_abbreviation[x])

    df_br_last_year = df_pivoted.query('exercicio == @max_year', engine='python')[['UF', 'Proporção']].copy()
    df_br_last_year.loc[:, 'UF'] = 'BR'
    df_br_last_year = df_br_last_year.groupby('UF', as_index=False)['Proporção'].mean()
    df_br_last_year['Posição'] = np.nan

    df_ne_last_year = df_pivoted.query('exercicio == @max_year and UF in @c.ne_states', engine='python')[['UF', 'Proporção']].copy()
    df_ne_last_year.loc[:, 'UF'] = 'NE'
    df_ne_last_year = df_ne_last_year.groupby('UF', as_index=False)['Proporção'].mean()
    df_ne_last_year['Posição'] = np.nan

    df_last_year_final = pd.concat([df_last_year, df_br_last_year, df_ne_last_year], ignore_index=True)
    df_last_year_final.rename(columns={'UF': 'Região', 'Proporção': 'Valor'}, inplace=True)

    df_last_year_final.to_excel(os.path.join(sheets_path, 'g11.8a.xlsx'), index=False, sheet_name='g11.8a')

except Exception as e:
    errors['Gráfico 11.8'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g11.1--ate--g11.8--t11.1--t11.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
