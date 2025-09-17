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


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

# url da base contas regionais
url = 'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Contas_Regionais'

# download do arquivo pib pela ótica da renda
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
            url_to_get_final = df.loc[
                (df['name'].str.startswith('Conta_da_Producao_2010')) &
                (df['name'].str.endswith('.zip')),
                'url'
            ].values[0]
            break
        except:
            last_year -= 1
            url_to_get = url + '/' + str(last_year) + '/xls'
            response = c.open_url(url_to_get)
            df = pd.DataFrame(response.json())
            if last_year == 0:
                errors[url + ' (Conta da Produção)'] = 'Arquivo não encontrado em anos anteriores'
                raise Exception('Arquivo não encontrado em anos anteriores')

    # downloading e organização do arquivo pib pela ótica da renda
    file = c.open_url(url_to_get_final)
    c.to_file(dbs_path, 'ibge_conta_producao.zip', file.content)
except Exception as e:
    errors[url + ' (Conta da Produção)'] = traceback.format_exc()

# tabelas sidra figuras g2.3 e g2.4
url = 'https://apisidra.ibge.gov.br/values/t/1612/n3/28/v/112,214,215/p/all/c81/allxt?formato=json'
url2 = 'https://apisidra.ibge.gov.br/values/t/1613/n3/28/v/112,214,215/p/all/c82/allxt?formato=json'
try:
    try:
        dfs = []
        for tb in [
            ('1612', '3', '28', '112,214,215', {'81': 'all'}),
            ('1613', '3', '28', '112,214,215', {'82': 'all'}),
        ]:
            data = sidrapy.get_table(
                table_code=tb[0],
                territorial_level=tb[1], ibge_territorial_code=tb[2],
                variable=tb[3],
                classification=tb[4],
                period='all'
            )
            data.drop(0, axis='index', inplace=True)

            dfs.append(data)

        df = pd.concat(dfs, ignore_index=True)
        # df = df[['D1N', 'D2N', 'V']]
        # mapping = {
        #     'D1N': 'Região',
        #     'D2N': 'Ano',
        #     'V': 'População'
        # }
        # df.rename(columns=mapping, inplace=True)
        c.to_excel(df, dbs_path, 'sidra_tables.xlsx')

    # Este exception é para o caso de não conseguir baixar os dados do sidra
    # Em vez de usar o sidrapy, tenta baixar os dados diretamente do site
    except Exception as e1:
        try:
            tb_1612 = c.open_url(url)
            tb_1613 = c.open_url(url2)

            df_1612 = pd.DataFrame(tb_1612.json())
            df_1613 = pd.DataFrame(tb_1613.json())

            dfs = []
            for i, tb in enumerate([df_1612, df_1613]):
                df = tb[['D3N', 'D2N', 'D4N', 'V']].copy()
                df.rename(columns={'D3N': 'Ano', 'D2N': 'Variável', 'D4N': 'Produto', 'V': 'Valor'}, inplace=True)
                df.drop(0, axis='index', inplace=True)
                df['Valor'] = df['Valor'].astype(str).str.replace('...', '0', regex=False)
                df['Valor'] = df['Valor'].str.replace('-', '0', regex=False)
                df.reset_index(drop=True, inplace=True)
                
                c.convert_type(df, 'Ano', 'int')
                c.convert_type(df, 'Valor', 'int')

                df['Tabela'] = '1612' if i == 0 else '1613'
                df = df.query('Ano >= 2010').copy()

                dfs.append(df)

            df_concat = pd.concat(dfs, ignore_index=True)

            c.to_excel(df_concat, dbs_path, 'sidra_tables.xlsx')

        except Exception as e2:
            errors['SIDRA (g2.3 e g2.4)'] = traceback.format_exc()

except Exception as e:
    errors['SIDRA'] = traceback.format_exc()


# tabelas sidra figuras g2.5 e g2.6
url = 'https://apisidra.ibge.gov.br/values/t/1612/n1/all/n2/2/n3/28/v/214,215/p/all/c81/allxt?formato=json'
url2 = 'https://apisidra.ibge.gov.br/values/t/1613/n1/all/n2/2/n3/28/v/214,215/p/all/c82/allxt?formato=json'
try:
    tb_1612 = c.open_url(url)
    tb_1613 = c.open_url(url2)

    df_1612 = pd.DataFrame(tb_1612.json())
    df_1613 = pd.DataFrame(tb_1613.json())

    dfs = []
    for i, tb in enumerate([df_1612, df_1613]):
        df = tb[['D1N', 'D3N', 'D2N', 'D4N', 'V']].copy()
        df.rename(columns={'D1N': 'Região', 'D3N': 'Ano', 'D2N': 'Variável', 'D4N': 'Produto', 'V': 'Valor'}, inplace=True)
        df.drop(0, axis='index', inplace=True)
        df['Valor'] = df['Valor'].astype(str).str.replace('...', '0', regex=False)
        df['Valor'] = df['Valor'].str.replace('-', '0', regex=False)
        df.reset_index(drop=True, inplace=True)
        
        c.convert_type(df, 'Ano', 'int')
        c.convert_type(df, 'Valor', 'int')

        df['Tabela'] = '1612' if i == 0 else '1613'
        df = df.query('Ano >= 2010').copy()

        dfs.append(df)

    df_concat = pd.concat(dfs, ignore_index=True)

    c.to_excel(df_concat, dbs_path, 'sidra_tables_g2.5--g2.6.xlsx')

except Exception as e:
    errors['SIDRA (g2.5 e g2.6)'] = traceback.format_exc()


# deflator IPEA IPA DI
try:
    data = ipeadatapy.timeseries('IGP_IPA')
    c.to_excel(data, dbs_path, 'ipeadata_ipa_di.xlsx')
except Exception as e:
    errors['IPEA IPA DI'] = traceback.format_exc()

# ************************
# PLANILHA
# ************************

# gráfico 2.1
try:
    data = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name='Tabela17', sheet_name='Tabela17.2', skiprows=1)
    columns = data[data[data.columns[0]] == 'ANO'].values[0]  # extrai os nomes das colunas armazenados em linhas
    columns = [col.strip().replace('\n', ' ') for col in columns]  # remove espaços em branco e quebras de linha
    # COLUNAS = [
    #   'ANO', 'VALOR DO ANO ANTERIOR (1 000 000 R$)', 'ÍNDICE DE VOLUME', 'VALOR A PREÇOS DO ANO ANTERIOR (1 000 000 R$)',
    #   'ÍNDICE DE PREÇO', 'VALOR A PREÇO CORRENTE (1 000 000 R$)'
    # ]

    # extrai os índices das tabelas que contêm os dados de interesse
    # numa mesma aba, há três tabelas dispostas verticalmente
    tables_indexes = data[
        data[data.columns[0]].str.contains("Valor Bruto da Produção") |
        data[data.columns[0]].str.contains("Consumo intermediário") |
        data[data.columns[0]].str.contains("Valor Adicionado Bruto")
    ].index.tolist()

    dfs = []
    for i, index in enumerate(tables_indexes):
        if i < 2:
            df = data.iloc[index:tables_indexes[i + 1]].copy()  # se for até a segunda tabela, extrai as linhas dentro do intervalo
        else:
            df = data.iloc[index:].copy()  # se for a última tabela, extrai as linhas até o final da aba

        df.columns = columns
        df.reset_index(drop=True, inplace=True)
        var = df.iloc[0, 0]  # extrai a atividade econômica
        df[columns[0]] = df[columns[0]].astype(str).str.strip()  # transforma a coluna de anos em string e remove espaços em branco
        df = df.loc[(df[columns[0]].str.startswith('20')) & (df[columns[0]].str.len() == 4)].copy()  # mantém apenas as linhas de interesse

        # converte os valores das colunas para o tipo adequado
        for ii, col in enumerate(df.columns):
            if ii == 0:
                df[col] = df[col].astype(int)
            else:
                df[col] = df[col].astype(float)

        # adiciona a atividade econômica como coluna
        df['Atividade'] = (
            'Valor bruto da produção' if var.startswith('Valor Bruto da Produção') else
            'Consumo intermediário' if var.startswith('Consumo intermediário') else
            'Valor adicionado bruto'
        )

        # deflação dos valores
        df.sort_values(by=columns[0], ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        df['Index'] = 100.00

        for row in range(1, len(df)):
            df.loc[row, 'Index'] = df.loc[row - 1, 'Index'] / df.loc[row - 1, columns[-2]]

        df['Value'] = (df[columns[-1]] / df['Index']) * 100

        dfs.append(df)

    df_concat = pd.concat(dfs, ignore_index=True)
    df_pivoted = df_concat.pivot_table(
        index=columns[0], columns='Atividade', values='Value'
    ).reset_index()
    df_pivoted = df_pivoted[[columns[0], 'Valor bruto da produção', 'Consumo intermediário', 'Valor adicionado bruto']]
    df_pivoted.rename(columns={columns[0]: 'Ano'}, inplace=True)
    df_pivoted['Ano'] = df_pivoted['Ano'].astype(int)

    df_pivoted.to_excel(os.path.join(sheets_path, 'g2.1.xlsx'), index=False, sheet_name='g2.1')

except Exception as e:
    errors['Gráfico 2.1'] = traceback.format_exc()

# gráfico 2.2
try:
    data = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name='Tabela17', sheet_name='Tabela17.2', skiprows=1)
    columns = data[data[data.columns[0]] == 'ANO'].values[0]  # extrai os nomes das colunas armazenados em linhas
    columns = [col.strip().replace('\n', ' ') for col in columns]  # remove espaços em branco e quebras de linha
    # COLUNAS = [
    #   'ANO', 'VALOR DO ANO ANTERIOR (1 000 000 R$)', 'ÍNDICE DE VOLUME', 'VALOR A PREÇOS DO ANO ANTERIOR (1 000 000 R$)',
    #   'ÍNDICE DE PREÇO', 'VALOR A PREÇO CORRENTE (1 000 000 R$)'
    # ]

    # extrai os índices das tabelas que contêm os dados de interesse
    # numa mesma aba, há três tabelas dispostas verticalmente
    tables_indexes = data[
        data[data.columns[0]].str.contains("Valor Adicionado Bruto", na=False)
    ].index.tolist()

    df = data.iloc[tables_indexes[0]:].copy()

    df.columns = columns
    df.reset_index(drop=True, inplace=True)
    var = df.iloc[0, 0]  # extrai a atividade econômica
    df[columns[0]] = df[columns[0]].astype(str).str.strip()  # transforma a coluna de anos em string e remove espaços em branco
    df = df.loc[(df[columns[0]].str.startswith('20')) & (df[columns[0]].str.len() == 4)].copy()  # mantém apenas as linhas de interesse

    # converte os valores das colunas para o tipo adequado
    for ii, col in enumerate(df.columns):
        if ii == 0:
            df[col] = df[col].astype(int)
        else:
            df[col] = df[col].astype(float)

    # deflação dos valores
    df.sort_values(by=columns[0], ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    df['Variação'] = (df[columns[2]] - 1) * 100

    df_final = df[[columns[0], 'Variação']].copy()
    df_final.rename(columns={columns[0]: 'Ano'}, inplace=True)
    df_final['Ano'] = df_final['Ano'].astype(int)
    df_final.dropna(inplace=True)

    df_final.to_excel(os.path.join(sheets_path, 'g2.2.xlsx'), index=False, sheet_name='g2.2')

except Exception as e:
    errors['Gráfico 2.2'] = traceback.format_exc()

# gráfico 2.3
try:
    data = c.open_file(dbs_path, 'sidra_tables.xlsx', 'xls', sheet_name='Sheet1')
    data_deflator = c.open_file(dbs_path, 'ipeadata_ipa_di.xlsx', 'xls', sheet_name='Sheet1')
    max_year = data['Ano'].max()  # ano máximo da tabela
    min_year = max_year - 9  # ano mínimo da tabela

    # tratamento do deflator
    deflator = data_deflator.query('YEAR >= @min_year and YEAR <= @max_year').copy()
    deflator.sort_values(by='YEAR', ascending=False, inplace=True)
    deflator.reset_index(drop=True, inplace=True)
    deflator.rename(columns={'VALUE (-)': 'VALUE'}, inplace=True)
    deflator['Diff'] = None
    deflator['Index'] = 100.00
    for row in range(1, len(deflator)):
        deflator.loc[row, 'Diff'] = (deflator.loc[row - 1, 'VALUE'] - deflator.loc[row, 'VALUE']) / deflator.loc[row, 'VALUE']  # variação percentual
        deflator.loc[row, 'Index'] = deflator.loc[row - 1, 'Index'] / (1 + deflator.loc[row, 'Diff'])  # índice de preços

    # tratamento da tabela
    df = data.query('Tabela == 1612').copy()
    df['Check'] = df.groupby(['Variável', 'Produto'])['Valor'].transform('sum')  # verifica se não há dados para todos os anos da variável por produto
    df = df.loc[df['Check'] > 0].copy()  # mantém apenas as linhas com dados para pelo menos um ano

    df['Mean'] = df.groupby(['Variável', 'Produto'])['Valor'].transform('mean')  # valor médio por produto e variável (considerando todos os anos)
    df['Rank'] = df.groupby('Variável')['Mean'].rank(method='dense', ascending=False)  # ranking das maiores médias; dense para aumentar o ranking no mesmo grupo
    top6 = df.loc[
        (df['Rank'] <= 6) &
        (df['Variável'].str.lower().str.contains('rendimento médio', na=False))
    ].copy()  # seleciona o top 6 produtos com maior rendimento médio anual
    products = top6['Produto'].unique()  # extrai os produtos

    # filtra os dados para os produtos selecionados, mantendo apenas as variáveis que não são rendimento médio
    df_filtered = df.loc[
        (df['Produto'].isin(products)) &
        (~df['Variável'].str.lower().str.contains('rendimento médio', na=False))
    ].copy()

    # dispõe as variáveis em colunas
    df_pivoted = df_filtered.pivot_table(
        index=['Ano', 'Produto'], columns='Variável', values='Valor'
    ).reset_index()
    df_pivoted.sort_values(by=['Ano', 'Produto'], ascending=[False, True], inplace=True)

    # join com o deflator
    df_joined = pd.merge(
        df_pivoted, deflator[['YEAR', 'Index']],
        left_on='Ano', right_on='YEAR', how='left', validate='m:1'
    )
    df_joined.drop(columns=['YEAR'], inplace=True)  # remove a coluna YEAR, pois já está em Ano
    cols = df_joined.columns.tolist()
    df_joined['Valor Ajustado'] = (df_joined[cols[-2]] / df_joined[cols[-1]]) * 100  # deflação dos valores

    # calcula a variação percentual das variáveis em relação ao ano anterior
    cols = df_joined.columns.tolist()
    cols = cols[:3] + [cols[-1]]  # ['Ano', 'Produto', 'Quantidade produzida', 'Valor Ajustado']
    df_last_year = df_joined.query('Ano in [@max_year, @max_year - 1]')[cols].copy()
    df_last_year.sort_values(by=['Produto', 'Ano'], ascending=[True, False], inplace=True)
    df_last_year.reset_index(drop=True, inplace=True)

    # Para cada produto, atribui o valor anterior (menor ano) de 'Quantidade produzida' em uma nova coluna
    LY_cols = df_last_year.columns.tolist()  # ['Ano', 'Produto', 'Quantidade produzida', 'Valor Ajustado']
    df_last_year['QTD_LY'] = df_last_year.groupby('Produto')[LY_cols[-2]].transform(
        lambda x: x.loc[df_last_year.loc[x.index, 'Ano'].idxmin()]
    )
    # Para cada produto, atribui o valor anterior (menor ano) de 'Valor Ajustado' em uma nova coluna
    df_last_year['VAL_LY'] = df_last_year.groupby('Produto')[LY_cols[-1]].transform(
        lambda x: x.loc[df_last_year.loc[x.index, 'Ano'].idxmin()]
    )

    # mantém apenas uma linha por produto
    df_last_year = df_last_year.query('Ano == @max_year').copy()
    df_last_year['QTD_Variation'] = (df_last_year[LY_cols[-2]] - df_last_year['QTD_LY']) / df_last_year['QTD_LY'] * 100
    df_last_year['VAL_Variation'] = (df_last_year[LY_cols[-1]] - df_last_year['VAL_LY']) / df_last_year['VAL_LY'] * 100

    # ajuste final
    df_last_year = df_last_year[[LY_cols[1], 'QTD_Variation', 'VAL_Variation']]
    df_last_year.rename(
        columns={
            'QTD_Variation': f'Quantidade {max_year}/{max_year - 1} - Variação do último ano',
            'VAL_Variation': f'Valor {max_year}/{max_year - 1} - Variação do último ano'
        }, inplace=True
    )

    # calcula a variação percentual das variáveis em relação a toda a série histórica
    df_all_years = df_joined.query('Ano in [@max_year, @min_year]')[cols].copy()
    df_all_years.sort_values(by=['Produto', 'Ano'], ascending=[True, False], inplace=True)
    df_all_years.reset_index(drop=True, inplace=True)

    # Para cada produto, atribui o valor anterior (menor ano) de 'Quantidade produzida' em uma nova coluna
    LY_cols = df_all_years.columns.tolist()  # ['Ano', 'Produto', 'Quantidade produzida', 'Valor Ajustado']
    df_all_years['QTD_LY'] = df_all_years.groupby('Produto')[LY_cols[-2]].transform(
        lambda x: x.loc[df_all_years.loc[x.index, 'Ano'].idxmin()]
    )
    # Para cada produto, atribui o valor anterior (menor ano) de 'Valor Ajustado' em uma nova coluna
    df_all_years['VAL_LY'] = df_all_years.groupby('Produto')[LY_cols[-1]].transform(
        lambda x: x.loc[df_all_years.loc[x.index, 'Ano'].idxmin()]
    )

    # mantém apenas uma linha por produto
    df_all_years = df_all_years.query('Ano == @max_year').copy()
    df_all_years['QTD_Variation'] = (df_all_years[LY_cols[-2]] - df_all_years['QTD_LY']) / df_all_years['QTD_LY'] * 100
    df_all_years['VAL_Variation'] = (df_all_years[LY_cols[-1]] - df_all_years['VAL_LY']) / df_all_years['VAL_LY'] * 100

    # ajuste final
    df_all_years = df_all_years[[LY_cols[1], 'QTD_Variation', 'VAL_Variation']]
    df_all_years.rename(
        columns={
            'QTD_Variation': f'Quantidade {max_year}/{min_year} - Variação em dez anos',
            'VAL_Variation': f'Valor {max_year}/{min_year} - Variação em dez anos'
        }, inplace=True
    )

    # join das duas tabelas
    df_merged = pd.merge(df_all_years, df_last_year, how='left', on='Produto', validate='1:1')
    df_merged = df_merged.melt(id_vars=['Produto'], var_name='Categoria', value_name='Valor')
    df_merged['Período'] = df_merged['Categoria'].str.split(' - ').str[0]
    df_merged['Categoria'] = df_merged['Categoria'].str.split(' - ').str[-1]
    df_merged = df_merged[['Produto', 'Período', 'Categoria', 'Valor']]
    
    df_merged.to_excel(os.path.join(sheets_path, 'g2.3.xlsx'), index=False, sheet_name='g2.3')

except Exception as e:
    errors['Gráfico 2.3'] = traceback.format_exc()


# gráfico 2.4
try:
    data = c.open_file(dbs_path, 'sidra_tables.xlsx', 'xls', sheet_name='Sheet1')
    data_deflator = c.open_file(dbs_path, 'ipeadata_ipa_di.xlsx', 'xls', sheet_name='Sheet1')
    min_year = data['Ano'].min()  # ano mínimo da tabela
    max_year = data['Ano'].max()  # ano máximo da tabela

    # tratamento do deflator
    deflator = data_deflator.query('YEAR >= @min_year and YEAR <= @max_year').copy()
    deflator.sort_values(by='YEAR', ascending=False, inplace=True)
    deflator.reset_index(drop=True, inplace=True)
    deflator.rename(columns={'VALUE (-)': 'VALUE'}, inplace=True)
    deflator['Diff'] = None
    deflator['Index'] = 100.00
    for row in range(1, len(deflator)):
        deflator.loc[row, 'Diff'] = (deflator.loc[row - 1, 'VALUE'] - deflator.loc[row, 'VALUE']) / deflator.loc[row, 'VALUE']  # variação percentual
        deflator.loc[row, 'Index'] = deflator.loc[row - 1, 'Index'] / (1 + deflator.loc[row, 'Diff'])  # índice de preços

    # tratamento da tabela
    df = data.query('Tabela == 1613').copy()
    df['Check'] = df.groupby(['Variável', 'Produto'])['Valor'].transform('sum')  # verifica se não há dados para todos os anos da variável por produto
    df = df.loc[df['Check'] > 0].copy()  # mantém apenas as linhas com dados para pelo menos um ano

    df['Mean'] = df.groupby(['Variável', 'Produto'])['Valor'].transform('mean')  # valor médio por produto e variável (considerando todos os anos)
    df['Rank'] = df.groupby('Variável')['Mean'].rank(method='dense', ascending=False)  # ranking das maiores médias; dense para aumentar o ranking no mesmo grupo
    top6 = df.loc[
        (df['Rank'] <= 6) &
        (df['Variável'].str.lower().str.contains('rendimento médio', na=False))
    ].copy()  # seleciona o top 6 produtos com maior rendimento médio anual
    products = top6['Produto'].unique()  # extrai os produtos

    # filtra os dados para os produtos selecionados, mantendo apenas as variáveis que não são rendimento médio
    df_filtered = df.loc[
        (df['Produto'].isin(products)) &
        (~df['Variável'].str.lower().str.contains('rendimento médio', na=False))
    ].copy()

    # dispõe as variáveis em colunas
    df_pivoted = df_filtered.pivot_table(
        index=['Ano', 'Produto'], columns='Variável', values='Valor'
    ).reset_index()
    df_pivoted.sort_values(by=['Ano', 'Produto'], ascending=[False, True], inplace=True)

    # join com o deflator
    df_joined = pd.merge(
        df_pivoted, deflator[['YEAR', 'Index']],
        left_on='Ano', right_on='YEAR', how='left', validate='m:1'
    )
    df_joined.drop(columns=['YEAR'], inplace=True)  # remove a coluna YEAR, pois já está em Ano
    cols = df_joined.columns.tolist()
    df_joined['Valor Ajustado'] = (df_joined[cols[-2]] / df_joined[cols[-1]]) * 100  # deflação dos valores

    # calcula a variação percentual das variáveis em relação ao ano anterior
    cols = df_joined.columns.tolist()
    cols = cols[:3] + [cols[-1]]  # ['Ano', 'Produto', 'Quantidade produzida', 'Valor Ajustado']
    df_last_year = df_joined.query('Ano in [@max_year, @max_year - 1]')[cols].copy()
    df_last_year.sort_values(by=['Produto', 'Ano'], ascending=[True, False], inplace=True)
    df_last_year.reset_index(drop=True, inplace=True)

    # Para cada produto, atribui o valor anterior (menor ano) de 'Quantidade produzida' em uma nova coluna
    LY_cols = df_last_year.columns.tolist()  # ['Ano', 'Produto', 'Quantidade produzida', 'Valor Ajustado']
    df_last_year['QTD_LY'] = df_last_year.groupby('Produto')[LY_cols[-2]].transform(
        lambda x: x.loc[df_last_year.loc[x.index, 'Ano'].idxmin()]
    )
    # Para cada produto, atribui o valor anterior (menor ano) de 'Valor Ajustado' em uma nova coluna
    df_last_year['VAL_LY'] = df_last_year.groupby('Produto')[LY_cols[-1]].transform(
        lambda x: x.loc[df_last_year.loc[x.index, 'Ano'].idxmin()]
    )

    # mantém apenas uma linha por produto
    df_last_year = df_last_year.query('Ano == @max_year').copy()
    df_last_year['QTD_Variation'] = (df_last_year[LY_cols[-2]] - df_last_year['QTD_LY']) / df_last_year['QTD_LY'] * 100
    df_last_year['VAL_Variation'] = (df_last_year[LY_cols[-1]] - df_last_year['VAL_LY']) / df_last_year['VAL_LY'] * 100

    # ajuste final
    df_last_year = df_last_year[[LY_cols[1], 'QTD_Variation', 'VAL_Variation']]
    df_last_year.rename(columns={
        'QTD_Variation': f'Quantidade {max_year} / {max_year - 1} - Variação do último ano',
        'VAL_Variation': f'Valor {max_year} / {max_year - 1} - Variação do último ano'
    }, inplace=True)

    # calcula a variação percentual das variáveis em relação a toda a série histórica
    df_all_years = df_joined.query('Ano in [@max_year, @min_year]')[cols].copy()
    df_all_years.sort_values(by=['Produto', 'Ano'], ascending=[True, False], inplace=True)
    df_all_years.reset_index(drop=True, inplace=True)

    # Para cada produto, atribui o valor anterior (menor ano) de 'Quantidade produzida' em uma nova coluna
    LY_cols = df_all_years.columns.tolist()  # ['Ano', 'Produto', 'Quantidade produzida', 'Valor Ajustado']
    df_all_years['QTD_LY'] = df_all_years.groupby('Produto')[LY_cols[-2]].transform(
        lambda x: x.loc[df_all_years.loc[x.index, 'Ano'].idxmin()]
    )
    # Para cada produto, atribui o valor anterior (menor ano) de 'Valor Ajustado' em uma nova coluna
    df_all_years['VAL_LY'] = df_all_years.groupby('Produto')[LY_cols[-1]].transform(
        lambda x: x.loc[df_all_years.loc[x.index, 'Ano'].idxmin()]
    )

    # mantém apenas uma linha por produto
    df_all_years = df_all_years.query('Ano == @max_year').copy()
    df_all_years['QTD_Variation'] = (df_all_years[LY_cols[-2]] - df_all_years['QTD_LY']) / df_all_years['QTD_LY'] * 100
    df_all_years['VAL_Variation'] = (df_all_years[LY_cols[-1]] - df_all_years['VAL_LY']) / df_all_years['VAL_LY'] * 100

    # ajuste final
    df_all_years = df_all_years[[LY_cols[1], 'QTD_Variation', 'VAL_Variation']]
    df_all_years.rename(columns={
        'QTD_Variation': f'Quantidade {max_year} / {min_year} - Variação desde 2010',
        'VAL_Variation': f'Valor {max_year} / {min_year} - Variação desde 2010'
    }, inplace=True)

    # join das duas tabelas
    df_merged = pd.merge(df_last_year, df_all_years, how='left', on='Produto', validate='1:1')
    df_merged = df_merged.melt(id_vars=['Produto'], var_name='Categoria', value_name='Valor')
    df_merged['Período'] = df_merged['Categoria'].str.split(' - ').str[0]
    df_merged['Categoria'] = df_merged['Categoria'].str.split(' - ').str[-1]
    df_merged = df_merged[['Produto', 'Período', 'Categoria', 'Valor']]
    df_merged.to_excel(os.path.join(sheets_path, 'g2.4.xlsx'), index=False, sheet_name='g2.4')

except Exception as e:
    errors['Gráfico 2.4'] = traceback.format_exc()


# gráfico 2.5
try:
    data = c.open_file(dbs_path, 'sidra_tables_g2.5--g2.6.xlsx', 'xls', sheet_name='Sheet1')
    data_deflator = c.open_file(dbs_path, 'ipeadata_ipa_di.xlsx', 'xls', sheet_name='Sheet1')
    min_year = data['Ano'].min()  # ano mínimo da tabela
    max_year = data['Ano'].max()  # ano máximo da tabela

    # tratamento do deflator
    deflator = data_deflator.query('YEAR >= @min_year and YEAR <= @max_year').copy()
    deflator.sort_values(by='YEAR', ascending=False, inplace=True)
    deflator.reset_index(drop=True, inplace=True)
    deflator.rename(columns={'VALUE (-)': 'VALUE'}, inplace=True)
    deflator['Diff'] = None
    deflator['Index'] = 100.00
    for row in range(1, len(deflator)):
        deflator.loc[row, 'Diff'] = (deflator.loc[row - 1, 'VALUE'] - deflator.loc[row, 'VALUE']) / deflator.loc[row, 'VALUE']  # variação percentual
        deflator.loc[row, 'Index'] = deflator.loc[row - 1, 'Index'] / (1 + deflator.loc[row, 'Diff'])  # índice de preços

    # tratamento da tabela
    df = data.query('Tabela == 1612').copy()  # filtro da tabela
    deflator.rename(columns={'YEAR': 'Ano'}, inplace=True)  # renomeção para join
    df = df.merge(deflator[['Ano', 'Index']], how='left', on='Ano', validate='m:1')
    df_pivoted = df.pivot(index=['Região', 'Ano', 'Produto', 'Index'], columns='Variável', values='Valor').reset_index()
    cols = df_pivoted.columns.tolist()  # ['Região', 'Ano', 'Produto', 'Index', 'Quantidade produzida', 'Valor da produção']
    # razação entre valor deflacionado ((df_pivoted[cols[-1]] / df_pivoted[cols[-3]]) * 100) e quantidade produzida (df_pivoted[cols[-2]])
    df_pivoted['R$/Q'] = ((df_pivoted[cols[-1]] / df_pivoted[cols[-3]]) * 100) / df_pivoted[cols[-2]]
    df_rank = df_pivoted.query('`Região` == "Sergipe"').copy()  # filtro da região para seleção dos produtos
    df_rank['Mean'] = df_rank.groupby(['Produto'])['R$/Q'].transform('mean')  # valor médio por produto
    df_rank['Rank'] = df_rank['Mean'].rank(method='dense', ascending=False)  # ranking das maiores médias; dense para aumentar o ranking no mesmo grupo

    df_se = df_rank.query('Rank <= 6').copy()  # seleciona o top 6 produtos com maior rendimento médio anual
    products = df_se['Produto'].unique()  # extrai os produtos
    df_filtered = df_pivoted.loc[df_pivoted['Produto'].isin(products)].copy()  # filtra os dados para os produtos selecionados
    df_final = df_filtered[['Ano', 'Região', 'Produto', 'R$/Q']].copy()  # mantém apenas as colunas de interesse
    df_final['Ano'] = df_final['Ano'].astype(int)  # converte a coluna Ano para int

    df_final.sort_values(by=['Ano', 'Região', 'Produto'], ascending=[False, True, True], inplace=True)  # ordena os dados
    df_final.reset_index(drop=True, inplace=True)  # reseta o índice
    df_final.to_excel(os.path.join(sheets_path, 'g2.5.xlsx'), index=False, sheet_name='g2.5')  # salva o resultado em Excel

except Exception as e:
    errors['Gráfico 2.5'] = traceback.format_exc()


# gráfico 2.6
try:
    data = c.open_file(dbs_path, 'sidra_tables_g2.5--g2.6.xlsx', 'xls', sheet_name='Sheet1')
    data_deflator = c.open_file(dbs_path, 'ipeadata_ipa_di.xlsx', 'xls', sheet_name='Sheet1')
    min_year = data['Ano'].min()  # ano mínimo da tabela
    max_year = data['Ano'].max()  # ano máximo da tabela

    # tratamento do deflator
    deflator = data_deflator.query('YEAR >= @min_year and YEAR <= @max_year').copy()
    deflator.sort_values(by='YEAR', ascending=False, inplace=True)
    deflator.reset_index(drop=True, inplace=True)
    deflator.rename(columns={'VALUE (-)': 'VALUE'}, inplace=True)
    deflator['Diff'] = None
    deflator['Index'] = 100.00
    for row in range(1, len(deflator)):
        deflator.loc[row, 'Diff'] = (deflator.loc[row - 1, 'VALUE'] - deflator.loc[row, 'VALUE']) / deflator.loc[row, 'VALUE']  # variação percentual
        deflator.loc[row, 'Index'] = deflator.loc[row - 1, 'Index'] / (1 + deflator.loc[row, 'Diff'])  # índice de preços

    # tratamento da tabela
    df = data.query('Tabela == 1613').copy()  # filtro da tabela
    deflator.rename(columns={'YEAR': 'Ano'}, inplace=True)  # renomeção para join
    df = df.merge(deflator[['Ano', 'Index']], how='left', on='Ano', validate='m:1')
    df_pivoted = df.pivot(index=['Região', 'Ano', 'Produto', 'Index'], columns='Variável', values='Valor').reset_index()
    cols = df_pivoted.columns.tolist()  # ['Região', 'Ano', 'Produto', 'Index', 'Quantidade produzida', 'Valor da produção']
    # razação entre valor deflacionado ((df_pivoted[cols[-1]] / df_pivoted[cols[-3]]) * 100) e quantidade produzida (df_pivoted[cols[-2]])
    df_pivoted['R$/Q'] = ((df_pivoted[cols[-1]] / df_pivoted[cols[-3]]) * 100) / df_pivoted[cols[-2]]
    df_rank = df_pivoted.query('`Região` == "Sergipe"').copy()  # filtro da região para seleção dos produtos
    df_rank['Mean'] = df_rank.groupby(['Produto'])['R$/Q'].transform('mean')  # valor médio por produto
    df_rank['Rank'] = df_rank['Mean'].rank(method='dense', ascending=False)  # ranking das maiores médias; dense para aumentar o ranking no mesmo grupo

    df_se = df_rank.query('Rank <= 6').copy()  # seleciona o top 6 produtos com maior rendimento médio anual
    products = df_se['Produto'].unique()  # extrai os produtos
    df_filtered = df_pivoted.loc[df_pivoted['Produto'].isin(products)].copy()  # filtra os dados para os produtos selecionados
    df_final = df_filtered[['Ano', 'Região', 'Produto', 'R$/Q']].copy()  # mantém apenas as colunas de interesse
    df_final['Ano'] = df_final['Ano'].astype(int)  # converte a coluna Ano para int

    df_final.sort_values(by=['Ano', 'Região', 'Produto'], ascending=[False, True, True], inplace=True)  # ordena os dados
    df_final.reset_index(drop=True, inplace=True)  # reseta o índice
    df_final.to_excel(os.path.join(sheets_path, 'g2.6.xlsx'), index=False, sheet_name='g2.6')  # salva o resultado em Excel

except Exception as e:
    errors['Gráfico 2.6'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g2.1--g2.2--g2.3--g2.4--g2.5--g2.6.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
