import functions as c
import os
import pandas as pd
import numpy as np
import json
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

# url
url = 'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Contas_Regionais'

# download do arquivo pib pela ótica da renda
try:
    response = c.open_url(url)
    df = pd.DataFrame(response.json())

    # pequisa pela publicação mais recente --> inicia com '2' e possui 4 caracteres
    df = df.loc[(df['name'].str.startswith('2')) &
                (df['name'].str.len() == 4),
                ['name', 'path']].sort_values(by='name', ascending=False).reset_index(drop=True)

    # obtém o caminho da publicação mais recente e adiciona à url de acesso aos arquivos
    url_to_get = df['path'][0][-5:] + '/xls'
    response = c.open_url(url + url_to_get)
    df = pd.DataFrame(response.json())
    url_to_get_pib = df.loc[df['name'].str.startswith('PIB_Otica_Renda'), 'url'].values[0]

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
    df = df.loc[(df['name'].str.startswith('2')) &
                (df['name'].str.len() == 4),
                ['name', 'path']].sort_values(by='name', ascending=False).reset_index(drop=True)

    # obtém o caminho da publicação mais recente e adiciona à url de acesso aos arquivos
    url_to_get = df['path'][0][-5:] + '/xls'
    response = c.open_url(url + url_to_get)
    df = pd.DataFrame(response.json())
    url_to_get_esp = df.loc[(df['name'].str.startswith('Especiais_2010')) &
                            (df['name'].str.endswith('.zip')), 'url'].values[0]

    # downloading e organização do arquivo: especiais 2010
    file = c.open_url(url_to_get_esp)
    c.to_file(dbs_path, 'ibge_especiais.zip', file.content)
except Exception as e:
    errors[url + ' (ESPECIAIS)'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

if os.path.exists(dbs_path + '/ibge_pib_otica_renda.xls'):
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

    #     # reordenação da variável ano para o eixo y
    #     df_melted = pd.melt(df_tb, id_vars='Atividade', value_vars=list(df_tb.columns[1:]),
    #                         var_name='Ano', value_name='Valor')

    #     # classificação dos dados por setor econômico

    #     '''
    #     Originalmente, os dados referentes ao setor e às atividades estão armazenados na mesma coluna.
    #     A cada ocorrência do valor 'Agropecuária', por exemplo, será coletado o seu índice,
    #     ponto de início da seleção dos valores.
    #     O índice, então, é somado à quantia de atividades deste setor (neste caso 3) mais 1,
    #     ponto de término da seleção dos valores.
    #     A mesma operação é realizada para os outros dois setores econômicos.
    #     '''

    #     df_melted['Setor'] = ''

    #     vars = ['Agropecuária', 'Indústria', 'Serviços']
    #     vars_order = df_melted.loc[df_melted['Atividade'].isin(vars), 'Atividade']

    #     agro_index = df_melted.loc[df_melted['Atividade'] == 'Agropecuária'].index
    #     ind_index = df_melted.loc[df_melted['Atividade'] == 'Indústria'].index
    #     serv_index = df_melted.loc[df_melted['Atividade'] == 'Serviços'].index

    #     for agro, ind, serv in zip(agro_index, ind_index, serv_index):
    #         df_melted.iloc[agro:agro + 4, -1] = 'Agropecuária'
    #         df_melted.iloc[ind:ind + 5, -1] = 'Indústria'
    #         df_melted.iloc[serv:serv + 12, -1] = 'Serviços'

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

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g1.5--g1.6--g1.7--g1.8--t1.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)

