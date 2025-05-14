import functions as c
import os
import pandas as pd
import numpy as np
import json
import traceback
import tempfile
import shutil
import sidrapy


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
            url_to_get_pib = df.loc[
                (df['name'].str.startswith('PIB_Otica_Renda')) &
                (df['name'].str.endswith('.xls')),
                'url'
            ].values[0]
            break
        except:
            last_year -= 1
            url_to_get = url + '/' + str(last_year) + '/xls'
            response = c.open_url(url_to_get)
            df = pd.DataFrame(response.json())
            if last_year == 0:
                errors[url + ' (PIB)'] = 'Arquivo não encontrado em anos anteriores'
                raise Exception('Arquivo não encontrado em anos anteriores')

    # downloading e organização do arquivo pib pela ótica da renda
    file = c.open_url(url_to_get_pib)
    c.to_file(dbs_path, 'ibge_pib_otica_renda.xls', file.content)
except Exception as e:
    errors[url + ' (PIB)'] = traceback.format_exc()

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

# dados da população
try:
    dfs = []
    for reg in [('1', 'all'), ('2', '2'), ('3', 'all')]:
        data = sidrapy.get_table(
            table_code='6579',
            territorial_level=reg[0], ibge_territorial_code=reg[1],
            variable='all', period='all'
        )
        data.drop(0, axis='index', inplace=True)

        dfs.append(data)

    df = pd.concat(dfs, ignore_index=True)
    df = df[['D1N', 'D2N', 'V']]
    mapping = {
        'D1N': 'Região',
        'D2N': 'Ano',
        'V': 'População'
    }
    df.rename(columns=mapping, inplace=True)
    c.to_excel(df, dbs_path, 'ibge_populacao.xlsx')

except Exception as e:
    errors[url] = traceback.format_exc()

# ************************
# PLANILHA
# ************************

if os.path.exists(os.path.join(dbs_path, 'ibge_pib_otica_renda.xls')):
    # g1.5, g1.6, g1.7, g1.8
    df = c.open_file(dbs_path, 'ibge_pib_otica_renda.xls', 'xls', skiprows=8)
    tables = ['Tabela1', 'Tabela10', 'Tabela18']

    mapping = {
        'grafico_1-5': 'Salários',
        'grafico_1-6': 'Contribuição social',
        'grafico_1-7': 'Impostos sobre produto, líquidos de subsídios',
        'grafico_1-8': 'Excedente Operacional Bruto (EOB) e Rendimento Misto (RM)'
    }


    # seleção das tabelas e componentes de interesse
    for k, v in mapping.items():
        try:
            dfs = []
            for tb in tables:
                # seleção de linhas não vazias
                # renomeação da coluna
                df_tb = df[tb]
                df_tb = df_tb.iloc[:9]
                df_tb = df_tb.rename(columns={'Unnamed: 0': 'Componente'})

                # reordenação da variável ano para o eixo y
                # seleção das linhas e das colunas de interesse
                df_melted = pd.melt(df_tb, id_vars='Componente', value_vars=df_tb.columns[1:], var_name='Ano',
                                    value_name='Valor')
                df_melted = df_melted.loc[(df_melted['Componente'] == v) &
                                        (df_melted['Ano'].str.endswith('.1'))]

                # remoção do ".1" ao final dos valores de Ano
                # decorrentes da ordenação padrão das variáveis Ano como colunas
                df_melted.loc[:, 'Ano'] = df_melted.loc[:, 'Ano'].apply(lambda x: x[:-2])

                # adição da variável região
                df_melted['Região'] = 'Brasil' if tb.endswith('1') else (
                    'Nordeste' if tb.endswith('10') else 'Sergipe')

                df_melted[df_melted.columns[2]] = df_melted[df_melted.columns[2]] * 100

                # classificação dos dados
                df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].astype('str')
                df_melted[df_melted.columns[1]] = pd.to_datetime(df_melted[df_melted.columns[1]], format='%Y')
                df_melted[df_melted.columns[1]] = df_melted[df_melted.columns[1]].dt.strftime('%d/%m/%Y')
                df_melted[df_melted.columns[2]] = df_melted[df_melted.columns[2]].astype('float64')
                df_melted[df_melted.columns[3]] = df_melted[df_melted.columns[3]].astype('str')

                dfs.append(df_melted)

            # conversão para arquivo csv
            df_concat = pd.concat(dfs, ignore_index=True)
            c.to_excel(df_concat, sheets_path, k[0] + k.split('_')[1].replace('-', '.') + '.xlsx')

        except Exception as e:
            g = k.split('_')[0].capitalize()
            g = g.replace('a', 'á')
            n = k.split('_')[1]
            n = n.replace('-', '.')
            errors[g + ' ' + n] = traceback.format_exc()

# t1.1
try:
    df = c.open_file(dbs_path, 'ibge_especiais.zip', 'zip', excel_name='tab07.xls', skiprows=4)
    # seleção das tabelas de interesse
    tables = ['Tabela7.1', 'Tabela7.10', 'Tabela7.18']
    dfs = []
    for tb in tables:
        df_tb = df[tb].copy()

        # renomeação do coluna de 'Unnamed: 0' para 'Atividade'
        # seleção das linhas de interesse
        df_tb.rename(columns={'Unnamed: 0': 'Atividade'}, inplace=True)
        df_tb = df_tb.iloc[2:].reset_index(drop=True)

        '''
        Originalmente, uma mesma coluna indica o setor e a atividade econômica,
        visualmente separando as categorias por identação na planilha.
        Exemplo:
        
        Indústria
            Indústrias extrativas
            Indústrias de transformação
        Serviços

        A ideia é separar as categorias em colunas distintas, de modo a facilitar a manipulação dos dados.
        Foi encontrado o índice de cada valor para separar as atividades por setor.
        Exemplo:

        1 Indústria
        2    Indústrias extrativas
        3    Indústrias de transformação
        4 Serviços

        Todos os valores entre 1 e 4 serão extraídos e categorizados como 'Indústria'.
        '''

        # ordenação das variáveis de interesse
        vars = ['Agropecuária', 'Indústria', 'Serviços']
        vars_order = df_tb.loc[df_tb['Atividade'].isin(vars), 'Atividade']
        indexes = list(vars_order.items())  # cria uma lista de tuplas (índice, valor)

        df_1 = df_tb.iloc[indexes[0][0]:indexes[1][0], ].copy()  # Conforme exemplo, seleciona as linhas de 1 a 4
        df_2 = df_tb.iloc[indexes[1][0]:indexes[2][0], ].copy()
        df_3 = df_tb.iloc[indexes[2][0]:, ].copy()

        clean_dfs = []
        for frame in [df_1, df_2, df_3]:
            if frame.shape[0] > 1:  # na últma versão do arquivo, o setor 'Agropecuária' não possui atividades, retornando apenas o nome do setor
                for sector in vars:  # verifica a qual setor pertence o frame
                    if not frame[frame['Atividade'] == sector].empty:
                        temp_sector = frame.drop(frame[frame['Atividade'] == sector].index)  # remove a linha de setor
                        temp_sector.drop(temp_sector[temp_sector['Atividade'].str.lower().str.contains('fonte: ibge')].index, inplace=True)  # remove a linha de fonte
                        temp_melted = pd.melt(temp_sector, id_vars='Atividade', value_vars=list(temp_sector.columns[1:]),
                                            var_name='Ano', value_name='Valor')
                        temp_melted['Setor'] = sector

                        clean_dfs.append(temp_melted)
            else:
                for sector in vars:
                    if not frame[frame['Atividade'] == sector].empty:
                        temp_sector = frame.copy()
                        temp_sector.drop(temp_sector[temp_sector['Atividade'].str.lower().str.contains('fonte: ibge')].index, inplace=True)  # remove a linha de fonte
                        temp_sector.loc[:, 'Atividade'] = 'Sem atividades listadas'
                        temp_melted = pd.melt(temp_sector, id_vars='Atividade', value_vars=list(temp_sector.columns[1:]),
                                            var_name='Ano', value_name='Valor')
                        temp_melted['Setor'] = sector

                        clean_dfs.append(temp_melted)

        df_sectors = pd.concat(clean_dfs, ignore_index=True)

        # adição da variável região
        df_sectors['Região'] = 'Brasil' if tb.endswith('7.1') else (
            'Nordeste' if tb.endswith('7.10') else 'Sergipe')

        dfs.append(df_sectors)

    # união dos dfs
    # reordenação das colunas
    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat = df_concat[['Região', 'Setor', 'Atividade', 'Ano', 'Valor']]

    # df_concat[df_concat.columns[-1]] = df_concat[df_concat.columns[-1]] * 100

    # classificação dos dados
    df_concat[df_concat.columns[0:3]] = df_concat[df_concat.columns[0:3]].astype('str')
    df_concat[df_concat.columns[-2]] = pd.to_datetime(df_concat[df_concat.columns[-2]], format='%Y')
    df_concat[df_concat.columns[-2]] = df_concat[df_concat.columns[-2]].dt.strftime('%d/%m/%Y')
    df_concat[df_concat.columns[-1]] = df_concat[df_concat.columns[-1]].astype('float64')

    # conversão em arquivo csv
    c.to_excel(df_concat, sheets_path, 't1.1.xlsx')

except Exception as e:
    errors['Tabela 1.1'] = traceback.format_exc()


# NOVA LEVA DE FIGURAS APÓS CHECAGEM DE SCRIPTS E CRIAÇÃO DOS FALTANTES

# g1.1
try:
    df = c.open_file(dbs_path, 'ibge_especiais.zip', 'zip', excel_name='tab03.xls', skiprows=3)
    
    df_tb = df[list(df.keys())[0]].copy()
    df_tb.rename(columns={'Unnamed: 0': 'UF'}, inplace=True)
    df_tb = df_tb.query('~UF.str.startswith("Fonte")')  # remove a linha de fonte, mantendo apenas os estados
    df_tb['UF'] = df_tb['UF'].str.strip()  # remove espaços em branco no início e no fim

    df_melted = pd.melt(
        df_tb, id_vars='UF', value_vars=list(df_tb.columns[1:]),
        var_name='Ano', value_name='Valor'
    )
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    df_melted.reset_index(drop=True, inplace=True)
    df_melted.sort_values(by=['UF', 'Ano'], ascending=[True, True], inplace=True)
    
    max_year = df_melted['Ano'].max()
    min_year = df_melted['Ano'].min()
    macro_region = ['Centro-Oeste', 'Nordeste', 'Norte', 'Sudeste', 'Sul', 'Brasil']

    dfs = []
    columns = []
    for years in [(max_year, max_year - 1), (max_year, min_year)]:  # estrutura para criar as tabelas de variação anual e variação histórica
        df_diff = df_melted.query('Ano in [@years[0], @years[1]]').copy()  # seleciona útlimo e penúltimo ano
        df_diff['Valor-1'] = df_diff.groupby('UF')['Valor'].shift(1)  # cria coluna com o valor do ano anterior na linha do ano atual
        df_diff['Diff'] = ((df_diff['Valor'] - df_diff['Valor-1']) / df_diff['Valor-1']) * 100  # calcula a diferença percentual
        df_diff = df_diff.query('Ano == @max_year')  # mantém apenas o ano atual
        df_diff['Rank'] = None

        df_diff_states = df_diff.query('UF not in @macro_region').copy()  # seleciona apenas os estados
        df_diff_states['Rank'] = df_diff_states.groupby('Ano')['Diff'].rank(method='first', ascending=False)  # cria coluna de rank
        df_diff_states.sort_values(by='Rank', ascending=True, inplace=True)  # ordena pela coluna de rank

        if df_diff_states.query('UF == "Sergipe"')['Rank'].values[0] <= 6:  # se sergipe estiver nos 6 primeiros
            df_diff_top6 = pd.concat([df_diff_states.query('Rank <= 6'), df_diff.query('UF in ["Brasil", "Nordeste"]')])  # seleciona os 6 primeiros estados e regiões
        else:
            df_diff_top6 = pd.concat([df_diff_states.query('Rank <= 6 or UF == "Sergipe"'), df_diff.query('UF in ["Brasil", "Nordeste"]')])  # senão, adiciona sergipe
        
        df_diff_top6 = df_diff_top6[['UF', 'Diff', 'Rank']]

        if years[1] != min_year:  # se for variação do último ano, define o texto abaixo
            df_diff_top6.rename(columns={'Diff': f'Variação (%) {max_year}'}, inplace=True)
        else:
            df_diff_top6.rename(columns={'Diff': f'Variação (%) {max_year}/{min_year}'}, inplace=True)  # senão, define ao lado

        df_diff_top6['UF'] = df_diff_top6['UF'].map(c.mapping_states_abbreviation)
        dfs.append(df_diff_top6)
        columns.extend(df_diff_top6.columns)

    # transformação do dataframes em listas de tuplas para unir cada tuplca como uma linha única no novo dataframe
    tuple1 = list(dfs[0].itertuples(index=False, name=None))
    tuple2 = list(dfs[1].itertuples(index=False, name=None))
    tuple_final = [a + b for a, b in zip(tuple1, tuple2)]

    df_final = pd.DataFrame(tuple_final, columns=columns)

    df_final.to_excel(os.path.join(sheets_path, 'g1.1.xlsx'), index=False, sheet_name='g1.1')


except Exception as e:
    errors['Gráfico 1.1'] = traceback.format_exc()


# g1.2
try:
    df = c.open_file(dbs_path, 'ibge_especiais.zip', 'zip', excel_name='tab03.xls', skiprows=3)
    
    df_tb = df[list(df.keys())[0]].copy()
    df_tb.rename(columns={'Unnamed: 0': 'UF'}, inplace=True)
    df_tb['UF'] = df_tb['UF'].str.strip()  # remove espaços em branco no início e no fim
    df_tb = df_tb.query('UF in ["Brasil", "Nordeste", "Sergipe"]')  # seleciona linhas de interesse

    df_melted = pd.melt(
        df_tb, id_vars='UF', value_vars=list(df_tb.columns[1:]),
        var_name='Ano', value_name='Valor'
    )
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    df_melted.reset_index(drop=True, inplace=True)
    df_melted.sort_values(by=['UF', 'Ano'], ascending=[True, True], inplace=True)

    dfs = []
    for var in df_melted.UF.unique():
        df_var = df_melted.query('UF == @var').copy()
        # diff() caclula a diferença entre os anos; shift(1) busca o valor do ano anterior
        df_var['Diff'] = (df_var['Valor'].diff() /  df_var['Valor'].shift(1)) * 100 # calcula a diferença entre os anos
        df_var.dropna(inplace=True)  # remove os valores nulos]

        dfs.append(df_var[['UF', 'Ano', 'Diff']])  # seleciona apenas as colunas de interesse

    mapping = {
        'Brasil': 'BR Percentual',
        'Sergipe': 'SE Percentual',
        'Nordeste': 'NE Percentual'
    }
    df_concat = pd.concat(dfs, ignore_index=True)  # une os dataframes
    df_concat['UF'] = df_concat['UF'].map(mapping)  # renomeia os estados
    df_concat['Ano'] = df_concat['Ano'].astype(int)  # transforma a coluna ano em string

    df_pivoted = df_concat.pivot(index='Ano', columns='UF', values='Diff')  # transforma o dataframe em uma tabela dinâmica
    df_pivoted.reset_index(inplace=True)  # reseta o índice

    df_pivoted.to_excel(os.path.join(sheets_path, 'g1.2.xlsx'), index=False, sheet_name='g1.2')

except Exception as e:
    errors['Gráfico 1.2'] = traceback.format_exc()

# g1.3
try:
    
    data = c.open_file(dbs_path, 'ibge_especiais.zip', 'zip', excel_name='tab01.xls', skiprows=3)
    deflator = c.open_file(dbs_path, 'ibge_especiais.zip', 'zip', excel_name='tab03.xls', skiprows=3)
    population = c.open_file(dbs_path, 'ibge_populacao.xlsx', 'xls', sheet_name='Sheet1')

    # tratamentos das bases de dados
    # pib
    df_data = data[list(data.keys())[0]].copy()
    df_data.rename(columns={'Unnamed: 0': 'UF'}, inplace=True)
    df_data['UF'] = df_data['UF'].str.strip()
    df_data = df_data.query('~UF.str.startswith("Fonte")')
    df_data = df_data.melt(
        id_vars='UF', value_vars=list(df_data.columns[1:]),
        var_name='Ano', value_name='Valor'
    )

    # deflator
    df_deflator = deflator[list(deflator.keys())[0]].copy()
    df_deflator.rename(columns={'Unnamed: 0': 'UF'}, inplace=True)
    df_deflator['UF'] = df_deflator['UF'].str.strip()
    df_deflator = df_deflator.query('~UF.str.startswith("Fonte")').copy()

    population.rename(columns={'Região': 'UF'}, inplace=True)

    # variação do PIB
    df_diff = df_deflator.melt(
        id_vars='UF', value_vars=list(df_deflator.columns[1:]),
        var_name='Ano', value_name='Valor'
    )

    diff_list = []
    for var in df_diff.UF.unique():
        df_var = df_diff.query('UF == @var').copy()
        df_var.sort_values(by='Ano', ascending=False, inplace=True)
        df_var.reset_index(drop=True, inplace=True)
        # diff() calcula a diferença entre os anos; shift(1) busca o valor do ano anterior
        df_var['Diff'] = df_var['Valor'].shift(1) - df_var['Valor']
        df_var['Diff %'] = df_var['Diff'] / df_var['Valor']
        df_var['Index'] = None
        
        for i in range(len(df_var)):
            if i == 0:
                df_var.iloc[i, -1] = 100.00
            else:
                df_var.iloc[i, -1] = df_var.iloc[i - 1, -1] / (1 + df_var.iloc[i, -2])
        
        diff_list.append(df_var[['UF', 'Ano', 'Index']])

    # merge do index deflator com a base de dados e merge com a população
    df_final = df_data.merge(pd.concat(diff_list), on=['UF', 'Ano'], how='left', validate='1:1')
    df_final = df_final.merge(population, on=['UF', 'Ano'], how='left', validate='1:1')
    df_final.sort_values(by=['UF', 'Ano'], ascending=[True, True], inplace=True)

    # preenchimento dos dados de população
    # para os estados que não possuem dados de população,
    # o valor é preenchido com o valor do ano segunte ou o valor do ano anterior
    dfs = []
    for var in df_final.UF.unique():
        df_var = df_final.query('UF == @var').copy()
        df_var['Pop'] = df_var['População'].ffill().bfill()
        dfs.append(df_var[['UF', 'Ano', 'Pop']])

    df_final = df_final.merge(pd.concat(dfs), on=['UF', 'Ano'], how='left', validate='1:1')

    df_final['PIB'] = (df_final['Valor'] / df_final['Index']) * 100  # calcula o PIB deflacionado
    df_final['PIB/População'] = (df_final['PIB'] * 1_000_000) / df_final['Pop']
    df_final['Rank'] = None

    # rank do PIB per capita
    to_exclude = ['Norte', 'Centro-Oeste', 'Sul', 'Sudeste', 'Nordeste', 'Brasil']  # estados/regi~es que devem ser excluídos do rank
    df_rank_last_year = df_final.query('Ano == @df_final.Ano.max() and UF not in @to_exclude').copy()
    df_rank_last_year['Rank'] = df_rank_last_year.groupby('Ano')['PIB/População'].rank(method='first', ascending=False)
    df_rank_last_year.sort_values(by='Rank', ascending=True, inplace=True)
    df_rank_last_year = df_rank_last_year[['UF', 'PIB/População', 'Rank']]
    df_rank_last_year.rename(columns={'PIB/População': f'PIB {df_final.Ano.max()} Deflacionado'}, inplace=True)

    # tratamento para ranqueament e seleção de regões
    df_aux = df_final.query('Ano == @df_final.Ano.max() and UF in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_aux.rename(columns={'PIB/População': f'PIB {df_final.Ano.max()} Deflacionado'}, inplace=True)
    df_aux = df_aux[['UF', f'PIB {df_final.Ano.max()} Deflacionado', 'Rank']]

    # top 6 estados
    if df_rank_last_year.query('UF == "Sergipe"')['Rank'].values[0] <= 6:  # se sergipe estiver nos 6 primeiros
        df_rank_last_year_top6 = pd.concat([
            df_rank_last_year.query('Rank <= 6'),
            df_aux.query('UF in ["Brasil", "Nordeste"]')
        ])
    else:
        df_rank_last_year_top6 = pd.concat([
            df_rank_last_year.query('Rank <= 6 or UF == "Sergipe"'),
            df_aux.query('UF in ["Brasil", "Nordeste"]')
        ])

    df_rank_last_year_top6['UF'] = df_rank_last_year_top6['UF'].map(c.mapping_states_abbreviation)

    # rank da variação do PIB per capita
    to_exclude = ['Norte', 'Centro-Oeste', 'Sul', 'Sudeste']  # regiões que devem ser excluídos do rank
    df_rank_diff = df_final.query('Ano in [@df_final.Ano.max(), @df_final.Ano.min()] and UF not in @to_exclude').copy()
    df_rank_diff = df_rank_diff.pivot(index='UF', columns='Ano', values='PIB/População')
    df_rank_diff.reset_index(inplace=True)
    df_rank_diff['Diff'] = (df_rank_diff[df_final.Ano.max()] - df_rank_diff[df_final.Ano.min()]) / df_rank_diff[df_final.Ano.min()]
    df_rank_diff['Rank'] = None

    df_states = df_rank_diff.query('UF not in ["Brasil", "Nordeste"]').copy()
    df_states['Rank'] = df_states['Diff'].rank(method='first', ascending=False)
    df_states.sort_values(by='Rank', ascending=True, inplace=True)
    df_states = df_states[['UF', 'Diff', 'Rank']]
    df_states.rename(columns={'Diff': f'Variação (%) {df_final.Ano.max()}/{df_final.Ano.min()}'}, inplace=True)

    df_aux = df_rank_diff.query('UF in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_aux.rename(columns={'Diff': f'Variação (%) {df_final.Ano.max()}/{df_final.Ano.min()}'}, inplace=True)
    df_aux = df_aux[['UF', f'Variação (%) {df_final.Ano.max()}/{df_final.Ano.min()}', 'Rank']]

    if df_states.query('UF == "Sergipe"')['Rank'].values[0] <= 6:  # se sergipe estiver nos 6 primeiros
        df_all_top6 = pd.concat([
            df_states.query('Rank <= 6'),
            df_aux.query('UF in ["Brasil", "Nordeste"]')
        ])
    else:
        df_all_top6 = pd.concat([
            df_states.query('Rank <= 6 or UF == "Sergipe"'),
            df_aux.query('UF in ["Brasil", "Nordeste"]')
        ])

    df_all_top6['UF'] = df_all_top6['UF'].map(c.mapping_states_abbreviation)

    # transformação do dataframes em listas de tuplas para unir cada tuplca como uma linha única no novo dataframe
    tuple1 = list(df_rank_last_year_top6.itertuples(index=False, name=None))
    tuple2 = list(df_all_top6.itertuples(index=False, name=None))
    tuple_final = [a + b for a, b in zip(tuple1, tuple2)]

    columns = df_rank_last_year_top6.columns.tolist() + df_all_top6.columns.tolist()
    df_final = pd.DataFrame(tuple_final, columns=columns)

    df_final.to_excel(os.path.join(sheets_path, 'g1.3.xlsx'), index=False, sheet_name='g1.3')

except Exception as e:
    errors['Gráfico 1.3'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g1.5--g1.6--g1.7--g1.8--t1.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
