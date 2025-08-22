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

# # deflator IPEA IPCA
# try:
#     data = ipeadatapy.timeseries('PRECOS_IPCAG')
#     data.rename(columns={'YEAR': 'Ano', 'VALUE ((% a.a.))': 'Valor'}, inplace=True)  # renomeia as colunas
#     c.to_excel(data, dbs_path, 'ipeadata_ipca.xlsx')
# except Exception as e:
#     errors['IPEA IPCA'] = traceback.format_exc()


# sidra 1187 - estimativa da população
url = 'https://apisidra.ibge.gov.br/values/t/1187/n1/all/n2/2/n3/28/v/all/p/all/c2/6794/d/v2513%201?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    c.to_excel(df, dbs_path, 'sidra_1187.xlsx')
except Exception as e:
    errors['Sidra 1187'] = traceback.format_exc()


# sidra 7113 - estimativa da população
url = 'https://apisidra.ibge.gov.br/values/t/7113/n1/all/n2/2/n3/28/v/10267/p/all/c2/6794/c58/2795/d/v10267%201?formato=json'
try:
    data = c.open_url(url)
    df = pd.DataFrame(data.json())
    df = df[['D3N', 'D1N', 'V']].copy()
    df.columns = ['Ano', 'Região', 'Valor']
    df.drop(0, axis='index', inplace=True)  # remove a primeira linha que contém o cabeçalho
    df['Ano'] = df['Ano'].astype(int)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')  # converte a coluna Valor para numérico, tratando erros

    c.to_excel(df, dbs_path, 'sidra_7113.xlsx')
except Exception as e:
    errors['Sidra 7113'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# gráfico 17.1
try:
    # tabela ibge
    data = c.open_file(dbs_path, 'sidra_1187.xlsx', 'xls', sheet_name='Sheet1')
    data['Valor'] = 100 - data['Valor']
    df_analfabetismo = c.open_file(dbs_path, 'sidra_7113.xlsx', 'xls', sheet_name='Sheet1')
    
    df = pd.concat([data, df_analfabetismo], ignore_index=True)

    df['Ano'] = '01/01/' + df['Ano'].astype(str)  # formata a coluna Ano para o formato de data
    df.rename(columns={'Valor': 'Taxa de analfabetismo'}, inplace=True)
    df.sort_values(by=['Região', 'Ano'], inplace=True)  # ordena os dados por Região e Ano

    df_final = df[['Região', 'Ano', 'Taxa de analfabetismo']].copy()

    c.to_excel(df_final, sheets_path, 'g17.1.xlsx')

except Exception as e:
    errors['Gráfico 17.1'] = traceback.format_exc()


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
    df_melted.rename(columns={'ano': 'Ano'}, inplace=True)

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
