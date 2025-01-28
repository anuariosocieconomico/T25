import functions as c
import os
import datetime
import pandas as pd
from bs4 import BeautifulSoup
import io
import numpy as np
import json
import traceback

# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = os.path.abspath(os.path.join('Scripts', 'Daniel', 'Diversos'))
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}

# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    # DOWNLOAD DA BASE DE DADOS SICONFI --------------------------------------------------------------------------------
    startY = 2015
    endY = datetime.datetime.now().year

    estados_ibge = {
        "Acre": 12,
        "Alagoas": 27,
        "Amapá": 16,
        "Amazonas": 13,
        "Bahia": 29,
        "Ceará": 23,
        "Distrito Federal": 53,
        "Espírito Santo": 32,
        "Goiás": 52,
        "Maranhão": 21,
        "Mato Grosso": 51,
        "Mato Grosso do Sul": 50,
        "Minas Gerais": 31,
        "Pará": 15,
        "Paraíba": 25,
        "Paraná": 41,
        "Pernambuco": 26,
        "Piauí": 22,
        "Rio de Janeiro": 33,
        "Rio Grande do Norte": 24,
        "Rio Grande do Sul": 43,
        "Rondônia": 11,
        "Roraima": 14,
        "Santa Catarina": 42,
        "São Paulo": 35,
        "Sergipe": 28,
        "Tocantins": 17
    }

    dfs = []
    faileds = {}
    for k, v in estados_ibge.items():  # estrutura para percorrer os estados
        print(f'A baixar dados do estado: {k}\n')
        for y in range(startY, endY + 1):  # estrutura para percorrer os anos

            url = f'https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo?an_exercicio={y}&nr_periodo=6' \
                  f'&co_tipo_demonstrativo=RREO&no_anexo=RREO-Anexo%2002&co_esfera=E&id_ente={v}'

            response = c.open_url(url)  # acesso à página
            if response.status_code == 200 and len(response.json()['items']) > 1:
                df = pd.DataFrame(response.json()['items'])
                df = df[['exercicio', 'instituicao', 'populacao', 'coluna', 'cod_conta', 'conta', 'valor']].copy()
                df['uf'] = k

                dfs.append(df)
                print(f'{k} ano {y} baixado com sucesso!')
            else:
                if k not in faileds:
                    faileds[k] = [y]
                else:
                    faileds[k].append(y)

    df_state = pd.concat(dfs, ignore_index=True)  # df com todos os anos de determinado estado
    c.to_csv(df_state, dbs_path, 'siconfi-database.csv')
    if faileds:
        errors['Falhas de requisição'] = faileds
except:
    errors['https://apidatalake.tesouro.gov.br/ords/siconfi/'] = traceback.format_exc()

# DOWNLOAD DA BASE DE DADOS IPCA ---------------------------------------------------------------------------------------
url = 'http://www.ipeadata.gov.br/ExibeSerie.aspx?serid=1410807112&module=M'
try:
    response = c.open_url(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    tbs = pd.read_html(io.StringIO(str(soup)), thousands='.', decimal=',')  # leitura de todas as tabelas da html
    df = [tb for tb in tbs if tb.shape[0] > 30][0]  # seleciona a tabela que houver + 30 linhas
    df = df.iloc[1:].copy()  # remoção de ruído na linha 0
    df.columns = ['Ano', 'IPCA']  # renomeação das colunas

    # cálculo dos índices
    df.sort_values(by='Ano', ascending=False, inplace=True)  # ordenação descendente dos anos
    df['Index'] = 0.0  # inicialização da coluna de índice
    df['IPCA'] = df['IPCA'].astype(float) / 100  # conversão da porcentagem para fração
    df.iloc[0, -1] = 100  # atribuição do valor 100 ao primeiro ano da série

    for i in range(len(df)):
        if i == 0:
            continue
        else:
            df.iloc[i, -1] = df.iloc[i-1, -1] / (1 + df.iloc[i-1, -2])

    c.to_csv(df, dbs_path, 'tb-ipca.csv')
except:
    errors[url] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

try:
    # carregamento da base de dados siconfi; filtragem das variáveis de interesse
    df_siconfi = pd.read_csv(
        os.path.join(dbs_path, 'siconfi-database.csv'),
        dtype={'valor':float, 'população': int, 'exercicio':int}
        , decimal=','
    )
    df_siconfi = df_siconfi.query(
        'coluna.str.startswith("DESPESAS LIQUIDADAS ATÉ O BIMESTRE") & conta == "Segurança Pública" & '
        'cod_conta == "RREO2TotalDespesas"'
    ).copy()

    # carregamento da base de dados ipca
    df_ipca = pd.read_csv(
        os.path.join(dbs_path, 'tb-ipca.csv'),
        dtype={'Ano':int, 'IPCA':float, 'Index': float}
        , decimal=','
    )

    # união das planilhas; cálculo do valor real
    df_merged = df_siconfi.merge(df_ipca, left_on='exercicio', right_on='Ano', how='left', validate='m:1')
    df_merged['Nominal'] = df_merged['valor'] / df_merged['populacao']
    df_merged['Real'] = (df_merged['Nominal'] / df_merged['Index']) * 100

    # ranqueamento dos estados por ano
    df_merged['Rank'] = df_merged.groupby('exercicio')['Real'].rank(ascending=False, method='first')
    df_merged.sort_values(by=['exercicio', 'Rank'], ascending=[False, True], inplace=True)

    # seleção dos dados de sergipe; agrupamento dos dados para Nordeste e Brasil
    estados_nordeste = ['Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba', 'Pernambuco', 'Piauí',
                        'Rio Grande do Norte', 'Sergipe']

    df_se = df_merged.query('uf == "Sergipe"').copy()
    df_se['Região'] = 'Sergipe'
    df_se['Rank'] = df_se['Rank'].astype(int)
    df_se = df_se[['Região', 'Ano', 'Real', 'Rank']]
    df_se.sort_values(by='Ano', inplace=True)

    df_ne = df_merged.query('uf.isin(@estados_nordeste)').copy()
    df_ne['Região'] = 'Nordeste'
    df_ne = df_ne.groupby(['Região', 'Ano'], as_index=False)['Real'].mean()
    df_ne['Rank'] = np.nan

    df_br = df_merged.copy()
    df_br['Região'] = 'Brasil'
    df_br = df_br.groupby(['Região', 'Ano'], as_index=False)['Real'].mean()
    df_br['Rank'] = np.nan

    # união das variáveis
    df = pd.concat([df_se, df_ne, df_br], ignore_index=True)
    df['Variável'] = 'Gastos públicos com segurança'
    df['Ano'] = pd.to_datetime(df['Ano'], format='%Y').dt.strftime('%d/%m/%Y')
    df = df[['Região', 'Variável', 'Ano', 'Real', 'Rank']].copy()
    df.rename(columns={'Real': 'Valor', 'Rank': 'Posição relativamente às demais UF'}, inplace=True)
    c.from_form_to_file(df, file_name_to_save='g19.14.xlsx')
except:
    errors['Gráfico 19.14'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g19.14.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))
