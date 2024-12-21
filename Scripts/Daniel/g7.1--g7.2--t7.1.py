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
url = 'https://www.epe.gov.br/pt/publicacoes-dados-abertos/publicacoes/anuario-estatistico-de-energia-eletrica'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    # caminho xpath de acesso ao arquivo para download
    xpath = '/html/body/form/div[3]/div[2]/section/div[2]/div/span/div[2]/div[1]/div[5]/div/div/div/div[' \
            '2]/div/table/tbody/tr/td[2]/a/@href'

    # request ao site e extração dos elementos da html
    html = c.get_html(url)
    html_urls = html.xpath(xpath).getall()
    url_to_get = [item for item in html_urls if item.endswith('.xlsx')][0]
    url_to_get = 'https://www.epe.gov.br' + url_to_get
    file = c.open_url(url_to_get)

    # salva os dados em arquivo local
    c.to_file(dbs_path, 'epe-anuario-energia.xlsx', file.content)

# registro do erro em caso de atualizações
except Exception as e:
    errors[url] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

try:
    # organização do arquivo
    # seleção das colunas e das linhas de interesse
    df = c.open_file(dbs_path, 'epe-anuario-energia.xlsx',
                     'xls', sheet_name='Tabela 3.63', skiprows=8)
    df = df.iloc[:, 1:12]
    df = df.loc[df[' '].isin(['Brasil', 'Nordeste', 'Sergipe'])]

    # reordenação da variável ano para o eixo y
    # renomeação do rótulo da coluna de ' ' para 'Região'
    # ordenação alfabética da coluna 'Região' e cronológica da coluna 'Ano'
    df_melted = df.melt(id_vars=' ', value_vars=list(df.columns[1:]), var_name='Ano',
                        value_name='Valor')
    df_melted.rename(columns={' ': 'Região'}, inplace=True)
    df_melted.sort_values(by=['Região', 'Ano'], ascending=[True, True], inplace=True)

    # classificação dos valores em cada coluna
    df_melted[df_melted.columns[0]] = df_melted[df_melted.columns[0]].astype('str')
    df_melted[df_melted.columns[1]] = pd.to_datetime(df_melted[df_melted.columns[1]], format='%Y')
    df_melted[df_melted.columns[1]] = df_melted[df_melted.columns[1]].dt.strftime('%d/%m/%Y')
    df_melted[df_melted.columns[2]] = df_melted[df_melted.columns[2]].astype('float64')

    # tratamento do título da figura para nomeação do arquivo csv
    c.to_excel(df_melted, sheets_path, 'g7.1.xlsx')

except Exception as e:
    errors['Gráfico 7.1'] = traceback.format_exc()

# g7.2
try:
    # organização do arquivo
    # seleção das colunas e das linhas de interesse
    df = c.open_file(dbs_path, 'epe-anuario-energia.xlsx',
                     'xls', sheet_name='Tabela 4.16', skiprows=8)
    df.rename(columns={' ': 'Consumo'}, inplace=True)
    df_filtered = df.iloc[0:4, 1:-3]  # seleciona linhas de interesse e ignora colunas vazias e de variações
    df_melted = df_filtered.melt(id_vars='Consumo', value_vars=df_filtered.columns[1:],
                                 var_name='Ano', value_name='Valor')  # verticalizam os anos, que são as colunas

    dfs = []
    for var in df_melted.Consumo.unique():
        df_temp = df_melted.query('Consumo == @var').copy()
        df_temp.sort_values(by='Ano', inplace=True)
        df_temp['Diff'] = df_temp['Valor'].pct_change() * 100
        dfs.append(df_temp)

    df_concat = pd.concat(dfs, ignore_index=True)
    # df_concat.dropna(axis='index', inplace=True, ignore_index=True)
    df_concat.dropna(axis='index', inplace=True)
    df_pivoted = df_concat.pivot(columns='Consumo', index='Ano', values='Diff').reset_index()
    df_pivoted.rename(columns={'Consumo (GWh)': 'Total'}, inplace=True)
    df_pivoted = df_pivoted[['Ano', 'Residencial', 'Industrial', 'Comercial', 'Total']]
    c.convert_type(df_pivoted, 'Ano', 'int')
    for col in df_pivoted.columns[1:]:
        c.convert_type(df_pivoted, col, 'float')
    c.to_excel(df_pivoted, sheets_path, 'g7.2.xlsx')

except Exception as e:
    errors['Gráfico 7.2'] = traceback.format_exc()

# t7.1
try:
    # organização do arquivo
    # seleção das colunas e das linhas de interesse
    df = c.open_file(dbs_path, 'epe-anuario-energia.xlsx',
                     'xls', sheet_name=None, skiprows=8)  # abre todas as abas do arquivo

    # abas de interesse e variáveis de diferenciação
    tbs = [('Tabela 2.1', 'Capacidade instalada', 'MW'), ('Tabela 2.4', 'Geração elétrica', 'GWh'),
           ('Tabela 3.13', 'Consumo cativo', 'GWh'), ('Tabela 3.14', 'Consumo livre', 'GWh'),
           ('Tabela 4.16', 'Sergipe', ''), ('Tabela 4.1', 'Brasil', ''), ('Tabela 3.10', 'Nordeste', '')]

    # há dois tipos de organização das tabelas; as separei para facilitar o tratamento
    dfs = {
        'Indicador': [],
        'Participacao': []
    }
    for tb in tbs:
        temp_df = df[tb[0]].iloc[:, 1:-3]  # remove colunas vazias e com taxas de variação
        temp_df.rename(columns={' ': 'Var'}, inplace=True)  # altera a label da coluna que armazena as vars
        if tb[0] in ['Tabela 2.1', 'Tabela 2.4', 'Tabela 3.13', 'Tabela 3.14']:  # tabelas do primeiro tipo
            temp_df = temp_df.query('Var == "Sergipe"').copy()
            temp_df['Indicador'] = tb[1]
            temp_df['Unidade'] = tb[2]
            dfs['Indicador'].append(temp_df)

        else:  # tabelas do segundo tipo
            if tb[0] == 'Tabela 3.10':  # para esta tabela há uma pequena diferença em relação às demais
                i = temp_df.query('Var == "Nordeste"').index.values[0]  # procura o index da var
                temp_df = temp_df.iloc[i + 1:i + 4].copy()  # coleta os três próximos registros; vars de interesse
            else:  # nessas, há uma aba para cada região
                temp_df = temp_df.iloc[1:4, :].copy()
            temp_df['Região'] = tb[1]
            dfs['Participacao'].append(temp_df)

    df_ind = pd.concat(dfs['Indicador'], ignore_index=True)  # união das tabelas tipo 1; indicadores
    df_part = pd.concat(dfs['Participacao'], ignore_index=True)  # união das tabelas tipo 2; regiões e categorias

    # verticalização das tabelas
    df_ind_melted = df_ind.melt(id_vars=['Indicador', 'Unidade'], value_vars=df_ind.columns[1:-2],
                                var_name='Ano', value_name='Valor')
    df_part_melted = df_part.melt(id_vars=['Var', 'Região'], value_vars=df_part.columns[1:-1],
                                  var_name='Ano', value_name='Valor')

    vals = []
    df_sergipe = df_part_melted.query('Região == "Sergipe"').copy()
    df_sergipe.sort_values(by=['Ano', 'Var'], inplace=True)
    df_sergipe.reset_index(drop=True, inplace=True)
    for reg in [r for r in df_part_melted['Região'].unique() if r != 'Sergipe']:
        df_reg = df_part_melted.query('Região == @reg').copy()
        df_reg.sort_values(by=['Ano', 'Var'], inplace=True)
        df_reg.reset_index(drop=True, inplace=True)
        for i, row in df_sergipe.iterrows():
            temp_vals = [
                f'Participação do consumo {row.loc["Var"]} em Sergipe no total do {reg}',
                '%',
                row.loc['Ano'],
                (df_sergipe.iloc[i, -1] / df_reg.iloc[i, -1]) * 100
            ]
            vals.append(temp_vals)

    df = pd.concat([df_ind_melted, pd.DataFrame(vals, columns=df_ind_melted.columns)], ignore_index=True)
    df = df[['Ano', 'Indicador', 'Unidade', 'Valor']].copy()
    df.sort_values(by=['Ano', 'Indicador', 'Unidade'], inplace=True)
    c.convert_type(df, 'Ano', 'int')
    c.to_excel(df, sheets_path, 't7.1.xlsx')

except Exception as e:
    errors['Tabela 7.1'] = traceback.format_exc()

# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g7.1.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
