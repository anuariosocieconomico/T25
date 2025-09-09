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

session = c.create_session_with_retries()
# contas da produção
try:
    year = datetime.now().year
    while year >= 2020:
        try:
            # url da base contas regionais
            url = f'https://servicodados.ibge.gov.br/api/v1/downloads/estatisticas?caminho=Contas_Regionais/{year}/xls'
            response = session.get(url, timeout=session.request_timeout, headers=c.headers)
            content = pd.DataFrame(response.json())
            link = content.query(
                'name.str.lower().str.startswith("conta_da_producao_2010") and name.str.lower().str.endswith(".zip")'
            )['url'].values[0]
            response = session.get(link, timeout=session.request_timeout, headers=c.headers)
            c.to_file(dbs_path, 'ibge_conta_producao.zip', response.content)
            break
        
        except:
            year -= 1
    
    if response.status_code == 200:
        print(f'Download da base "Contas da Produção" realizado com sucesso para o ano de {year}.')

except Exception as e:
    errors[url + ' (Conta da Produção)'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

# gráfico 6.1
try:
    data = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name='Tabela17', sheet_name='Tabela17.7', skiprows=1)
    indexes = data[data[data.columns[0]] == 'ANO'].index.tolist()  # extrai os índices das linhas que contêm os anos
    variables = data.iloc[[i - 3 for i in indexes], 0].to_list()  # extrai as variáveis correspondentes a cada ano
    # valores previsto: ['Valor Bruto da Produção 2010-2022', 'Consumo intermediário 2010-2022', 'Valor Adicionado Bruto 2010-2022']

    # tratamento dos dados
    dfs = []
    for i in range(len(indexes)):
        if i < 2:
            df = data.iloc[indexes[i]:indexes[i + 1]].copy()
        else:
            df = data.iloc[indexes[i]:].copy()

        columns = df.iloc[0].to_list()  # define a primeira linha como cabeçalho
        cols = [col.split('\n')[0].strip() if '\n' in col else col.strip() for col in columns]  # remove quebras de linha
        df.columns = cols  # renomeia as colunas
        # valores previstos: ['ANO', 'VALOR DO ANO ANTERIOR', 'ÍNDICE DE VOLUME', 'VALOR A PREÇOS DO ANO ANTERIOR', 'ÍNDICE DE PREÇO', 'VALOR A PREÇO CORRENTE']

        df[cols[0]] = df[cols[0]].astype(str)  # converte a primeira coluna para string
        df_filtered = df[
            (df[cols[0]].str.startswith('20')) & (df[cols[0]].str.len() == 4)
        ].copy()

        df_filtered[cols[0]] = df_filtered[cols[0]].astype(int)  # converte a primeira coluna para inteiro
        df_filtered[cols[1:]] = df_filtered[cols[1:]].astype(float)  # converte a segunda coluna para float
        df_filtered['Variável'] = variables[i]  # adiciona a variável correspondente

        # deflação
        df_filtered.sort_values(by=cols[0], ascending=False, inplace=True)  # ordena pelo ano
        df_filtered.reset_index(drop=True, inplace=True)
        df_filtered['Index'] = 100.00  # cria a coluna de índice
        
        for row in range(1, len(df_filtered)):
            df_filtered.loc[row, 'Index'] = df_filtered.loc[row - 1, 'Index'] / df_filtered.loc[row -1, cols[-2]]

        dfs.append(df_filtered)

    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat['Valor Ajustado'] = (df_concat[cols[-1]] / df_concat['Index']) * 100.00  # calcula o valor ajustado

    # organização da tabela
    df_concat = df_concat[[cols[0], 'Variável', 'Valor Ajustado']].copy()
    df_concat['Variável'] = df_concat['Variável'].apply(lambda x:
        'Valor bruto da produção' if 'valor bruto da produção' in x.lower().strip() else
        'Consumo intermediário' if 'consumo intermediário' in x.lower().strip() else
        'Valor bruto adicionado' if 'valor adicionado bruto' in x.lower().strip() else x
    )
    df_pivot = df_concat.pivot(index=cols[0], columns='Variável', values='Valor Ajustado').reset_index()
    df_pivot = df_pivot[[cols[0], 'Valor bruto da produção', 'Consumo intermediário', 'Valor bruto adicionado']].copy()

    df_pivot.rename(columns={'ANO': 'Ano'}, inplace=True)  # renomeia a coluna de ano
    df_pivot['Ano'] = df_pivot['Ano'].astype(int)  # converte a coluna de ano para inteiro

    df_pivot.to_excel(os.path.join(sheets_path, 'g6.1.xlsx'), index=False, sheet_name='g6.1')

except Exception as e:
    errors['Gráfico 6.1'] = traceback.format_exc()


# gráfico 6.2
try:
    data = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name='Tabela17', sheet_name='Tabela17.7', skiprows=1)
    indexes = data[data[data.columns[0]] == 'ANO'].index.tolist()  # extrai os índices das linhas que contêm os anos
    variables = data.iloc[[i - 3 for i in indexes], 0].to_list()  # extrai as variáveis correspondentes a cada ano
    # valores previsto: ['Valor Bruto da Produção 2010-2022', 'Consumo intermediário 2010-2022', 'Valor Adicionado Bruto 2010-2022']

    # tratamento dos dados
    dfs = []
    for i in range(len(indexes)):
        if i < 2:
            df = data.iloc[indexes[i]:indexes[i + 1]].copy()
        else:
            df = data.iloc[indexes[i]:].copy()

        columns = df.iloc[0].to_list()  # define a primeira linha como cabeçalho
        cols = [col.split('\n')[0].strip() if '\n' in col else col.strip() for col in columns]  # remove quebras de linha
        df.columns = cols  # renomeia as colunas
        # valores previstos: ['ANO', 'VALOR DO ANO ANTERIOR', 'ÍNDICE DE VOLUME', 'VALOR A PREÇOS DO ANO ANTERIOR', 'ÍNDICE DE PREÇO', 'VALOR A PREÇO CORRENTE']

        df[cols[0]] = df[cols[0]].astype(str)  # converte a primeira coluna para string
        df_filtered = df[
            (df[cols[0]].str.startswith('20')) & (df[cols[0]].str.len() == 4)
        ].copy()

        df_filtered[cols[0]] = df_filtered[cols[0]].astype(int)  # converte a primeira coluna para inteiro
        df_filtered[cols[1:]] = df_filtered[cols[1:]].astype(float)  # converte a segunda coluna para float
        df_filtered['Variável'] = variables[i]  # adiciona a variável correspondente

        # deflação
        df_filtered.sort_values(by=cols[0], ascending=False, inplace=True)  # ordena pelo ano
        df_filtered.reset_index(drop=True, inplace=True)
        df_filtered['Index'] = 100.00  # cria a coluna de índice
        
        for row in range(1, len(df_filtered)):
            df_filtered.loc[row, 'Index'] = df_filtered.loc[row - 1, 'Index'] / df_filtered.loc[row -1, cols[-2]]

        dfs.append(df_filtered)

    df_concat = pd.concat(dfs, ignore_index=True).query('`Variável`.str.lower().str.contains("valor adicionado bruto")', engine='python')
    df_concat['variação'] = (df_concat[cols[2]] - 1) * 100.00  # calcula o valor ajustado

    # organização da tabela
    df_concat = df_concat[[cols[0], 'variação']].copy()
    df_concat.dropna(axis=0, inplace=True)  # remove as linhas com valores nulos

    df_concat.rename(columns={'ANO': 'Ano'}, inplace=True)  # renomeia a coluna de ano
    df_concat['Ano'] = df_concat['Ano'].astype(int)  # converte a coluna de ano para inteiro
    df_concat.sort_values(by='Ano', ascending=True, inplace=True)  # ordena pelo ano

    df_concat.to_excel(os.path.join(sheets_path, 'g6.2.xlsx'), index=False, sheet_name='g6.2')

except Exception as e:
    errors['Gráfico 6.2'] = traceback.format_exc()


# gráfico 6.3
try:
    dbs = []
    for sheet in [('Tabela9', 'Tabela9.7', 'NE'), ('Tabela17', 'Tabela17.7', 'SE'), ('Tabela33', 'Tabela33.7', 'BR')]:
        data = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name=sheet[0], sheet_name=sheet[1], skiprows=1)
        indexes = data[data[data.columns[0]] == 'ANO'].index.tolist()  # extrai os índices das linhas que contêm os anos
        variables = data.iloc[[i - 3 for i in indexes], 0].to_list()  # extrai as variáveis correspondentes a cada ano
        # valores previsto: ['Valor Bruto da Produção 2010-2022', 'Consumo intermediário 2010-2022', 'Valor Adicionado Bruto 2010-2022']

        # tratamento dos dados
        dfs = []
        for i in range(len(indexes)):
            if i < 2:
                df = data.iloc[indexes[i]:indexes[i + 1]].copy()
            else:
                df = data.iloc[indexes[i]:].copy()

            columns = df.iloc[0].to_list()  # define a primeira linha como cabeçalho
            cols = [col.split('\n')[0].strip() if '\n' in col else col.strip() for col in columns]  # remove quebras de linha
            df.columns = cols  # renomeia as colunas
            # valores previstos: ['ANO', 'VALOR DO ANO ANTERIOR', 'ÍNDICE DE VOLUME', 'VALOR A PREÇOS DO ANO ANTERIOR', 'ÍNDICE DE PREÇO', 'VALOR A PREÇO CORRENTE']

            df[cols[0]] = df[cols[0]].astype(str)  # converte a primeira coluna para string
            df_filtered = df[
                (df[cols[0]].str.startswith('20')) & (df[cols[0]].str.len() == 4)
            ].copy()

            df_filtered[cols[0]] = df_filtered[cols[0]].astype(int)  # converte a primeira coluna para inteiro
            df_filtered[cols[1:]] = df_filtered[cols[1:]].astype(float)  # converte a segunda coluna para float
            df_filtered['Variável'] = variables[i]  # adiciona a variável correspondente
            df_filtered['Região'] = sheet[2]  # adiciona a região correspondente

            dfs.append(df_filtered)

        df_concat = pd.concat(dfs, ignore_index=True)
        dbs.append(df_concat.query("Variável.str.lower().str.strip().str.contains('valor bruto da produção')").copy())  # exclui o valor adicionado bruto, que será tratado separadamente

    df_concat_final = pd.concat(dbs, ignore_index=True)
    df_final = df_concat_final.loc[df_concat_final['Região'] != 'SE', [cols[0], cols[-1], 'Região']].merge(
        df_concat_final.loc[df_concat_final['Região'] == 'SE', [cols[0], cols[-1]]], how='left', on=cols[0], validate='m:1'
    )
    # ['ANO', 'VALOR A PREÇO CORRENTE_x', 'Região', 'VALOR A PREÇO CORRENTE_y']
    cols2 = df_final.columns.tolist()  # atualiza as colunas
    df_final['Razão'] = np.where(df_final[cols2[1]] != 0, (df_final[cols2[-1]] / df_final[cols2[1]]) * 100, np.nan)  # calcula a razão entre os valores
    df_final['Região'] = df_final['Região'].map({'NE': 'Se/Ne', 'BR': 'Se/Br (direita)'})  # renomeia as regiões
    df_final.rename(columns={cols2[0]: 'Ano'}, inplace=True)  # renomeia as colunas
    df_final = df_final.pivot(index='Ano', columns='Região', values='Razão').reset_index()  # pivota a tabela
    df_final['Ano'] = df_final['Ano'].astype(int)  # converte a coluna de ano para inteiro

    df_final.to_excel(os.path.join(sheets_path, 'g6.3.xlsx'), index=False, sheet_name='6.3')

except Exception as e:
    errors['Gráfico 6.3'] = traceback.format_exc()


# gráfico 6.4
try:
    dbs = []
    for sheet in [('Tabela9', 'Tabela9.7', 'NE'), ('Tabela17', 'Tabela17.7', 'SE'), ('Tabela33', 'Tabela33.7', 'BR')]:
        data = c.open_file(dbs_path, 'ibge_conta_producao.zip', 'zip', excel_name=sheet[0], sheet_name=sheet[1], skiprows=1)
        indexes = data[data[data.columns[0]] == 'ANO'].index.tolist()  # extrai os índices das linhas que contêm os anos
        variables = data.iloc[[i - 3 for i in indexes], 0].to_list()  # extrai as variáveis correspondentes a cada ano
        # valores previsto: ['Valor Bruto da Produção 2010-2022', 'Consumo intermediário 2010-2022', 'Valor Adicionado Bruto 2010-2022']

        # tratamento dos dados
        dfs = []
        for i in range(len(indexes)):
            if i < 2:
                df = data.iloc[indexes[i]:indexes[i + 1]].copy()
            else:
                df = data.iloc[indexes[i]:].copy()

            columns = df.iloc[0].to_list()  # define a primeira linha como cabeçalho
            cols = [col.split('\n')[0].strip() if '\n' in col else col.strip() for col in columns]  # remove quebras de linha
            df.columns = cols  # renomeia as colunas
            # valores previstos: ['ANO', 'VALOR DO ANO ANTERIOR', 'ÍNDICE DE VOLUME', 'VALOR A PREÇOS DO ANO ANTERIOR', 'ÍNDICE DE PREÇO', 'VALOR A PREÇO CORRENTE']

            df[cols[0]] = df[cols[0]].astype(str)  # converte a primeira coluna para string
            df_filtered = df[
                (df[cols[0]].str.startswith('20')) & (df[cols[0]].str.len() == 4)
            ].copy()

            df_filtered[cols[0]] = df_filtered[cols[0]].astype(int)  # converte a primeira coluna para inteiro
            df_filtered[cols[1:]] = df_filtered[cols[1:]].astype(float)  # converte a segunda coluna para float
            df_filtered['Variável'] = variables[i]  # adiciona a variável correspondente
            df_filtered['Região'] = sheet[2]  # adiciona a região correspondente

            dfs.append(df_filtered)

        df_concat = pd.concat(dfs, ignore_index=True)
        dbs.append(df_concat.query("Variável.str.lower().str.strip().str.contains('valor adicionado bruto')").copy())  # exclui o valor adicionado bruto, que será tratado separadamente

    df_concat_final = pd.concat(dbs, ignore_index=True)
    df_final = df_concat_final.loc[df_concat_final['Região'] != 'SE', [cols[0], cols[-1], 'Região']].merge(
        df_concat_final.loc[df_concat_final['Região'] == 'SE', [cols[0], cols[-1]]], how='left', on=cols[0], validate='m:1'
    )
    # ['ANO', 'VALOR A PREÇO CORRENTE_x', 'Região', 'VALOR A PREÇO CORRENTE_y']
    cols2 = df_final.columns.tolist()  # atualiza as colunas
    df_final['Razão'] = np.where(df_final[cols2[1]] != 0, (df_final[cols2[-1]] / df_final[cols2[1]]) * 100, np.nan)  # calcula a razão entre os valores
    df_final['Região'] = df_final['Região'].map({'NE': 'Se/Ne', 'BR': 'Se/Br (direita)'})  # renomeia as regiões
    df_final.rename(columns={cols2[0]: 'Ano'}, inplace=True)  # renomeia as colunas
    df_final = df_final.pivot(index='Ano', columns='Região', values='Razão').reset_index()  # pivota a tabela
    df_final['Ano'] = df_final['Ano'].astype(int)  # converte a coluna de ano para inteiro

    df_final.to_excel(os.path.join(sheets_path, 'g6.4.xlsx'), index=False, sheet_name='g6.4')

except Exception as e:
    errors['Gráfico 6.4'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g6.1--g6.2--g6.3--g6.4.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
