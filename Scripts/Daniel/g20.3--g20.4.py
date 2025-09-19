import time
import functions as c
import os
import json
import traceback
import tempfile
import shutil
import requests
import pandas as pd
import numpy as np


# obtém o caminho desse arquivo de comandos para adicionar os diretórios que armazenará as bases de dados e planilhas
dbs_path = tempfile.mkdtemp()
sheets_path = c.sheets_dir
errors_path = c.errors_dir

# inicializa o dicionário para armazenar informações sobre possíveis erros durante a execução
errors = {}

# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

"""
Fonte antiga das figuras
"""
# # url
# url = 'http://ivs.ipea.gov.br/ivs_componentes/grid/'

# # tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
# try:
#     driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium
#     driver.get(url)  # acessa a página

#     if driver.get_tag('/html/body/center[1]/h1').text == '404 Not Found':
#         driver.quit()
#         errors[url + ' (IDHM)'] = 'Página não carregada'
#         raise Exception('Página não carregada')

#     driver.wait('/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/button')
#     time.sleep(2)
#     driver.random_click()

#     # territoriedade
#     driver.click([
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/button',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[3]/a/label',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[4]/a/label',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[5]/a/label',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/button'
#     ])

#     # macrorregião
#     driver.click([
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/button',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/ul/li[4]/a/label',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/button'
#     ])

#     # índices
#     driver.click([
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/button',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/ul/li[3]/a/label',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/button'
#     ])

#     driver.click('/html/body/div[3]/div[2]/div[1]/fieldset/div/div/div[13]/button[2]')

#     # baixa o arquivo
#     driver.wait(
#         '/html/body/div[3]/div[3]/div/div[3]/div[2]/div/table/thead/tr[2]/th[4]/div/table/tbody/tr/td[2]/button')
#     driver.click('/html/body/div[3]/div[3]/div/div[5]/div/table/tbody/tr/td[1]/table/tbody/tr/td/div')
#     time.sleep(3)

#     data = c.open_file(dbs_path, os.listdir(dbs_path)[0], ext='xls')
#     df = data[list(data.keys())[0]]
#     df.loc[(df['Nome da Região'] == '-') & (df['Nome da UF'] == '-'), 'Nome da UF'] = 'Brasil'
#     df.loc[(df['Nome da Região'] == 'NORDESTE') & (df['Nome da UF'] == '-'), 'Nome da UF'] = 'Nordeste'
#     df.drop(df.columns[:-3], axis='columns', inplace=True)
#     df.columns = ['Região', 'Ano', 'IVS']
#     c.convert_type(df, 'IVS', 'float')

#     df.sort_values(by=['Região', 'Ano'], inplace=True)

#     c.to_csv(df, dbs_path, 'ipea (IVS).csv')

#     driver.quit()

# # registro do erro em caso de atualizações
# except Exception as e:
#     errors[url + ' (IVS)'] = traceback.format_exc()


"""
Nova fonte das figuras
"""

url = "https://ivs.ipea.gov.br/api/api/getgrid"
headers = {
    "sec-ch-ua-platform": "\"Windows\"",
    "Referer": "https://ivs.ipea.gov.br/",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "DNT": "1",
    "Content-Type": "application/json"
}
data = {
    "indicadores": {
        "variaveis": [
            {
                "id_variavel": 1,
                "sigla": "ivs_c",
                "exibir_na_consulta": True,
                "ordem": 1,
                "minimo": "0",
                "maximo": "1",
                "var_desag": None,
                "cod_desag": None,
                "fk_fonte": 1,
                "fk_arquivo": None,
                "numerador": "-",
                "denominador": "-",
                "decimais": 3,
                "id_tema": 36,
                "fonte": [
                    {
                        "id_fonte": 1,
                        "nome": "Censo",
                        "sigla": "censo",
                        "descricao": "Censo Demográfico - IBGE",
                        "fk_arquivo": None,
                        "pivot": {
                            "fk_variavel": 1,
                            "fk_fonte": 1
                        }
                    }
                ],
                "lang_var": {
                    "id_lang_var": 1,
                    "nome_curto": "IVS",
                    "nome_longo": "Índice de Vulnerabilidade Social",
                    "nome_perfil": "IVS",
                    "descricao": "Índice de Vulnerabilidade Social. Média aritmética dos índices das dimensões: IVS Infraestrutura Urbana, IVS Capital Humano e IVS Renda e Trabalho.",
                    "lang": "pt-br",
                    "fk_variavel": 1
                },
                "pivot": {
                    "fk_tema": 36,
                    "fk_variavel": 1
                }
            }
        ],
        "indices": [],
        "subtemas": [],
        "allSelected": False
    },
    "pagination": {
        "current_page": 1,
        "last_page": 0,
        "per_page": 29,
        "total": 0,
        "from": 0,
        "to": 0
    },
    "ordenation": {
        "default": "asc"
    },
    "territorios": [
        {"sigla": "pais", "selectedAll": True, "delete": False, "itens": []},
        {"sigla": "regiao", "selectedAll": False, "delete": False, "itens": [2]},
        {"sigla": "estado", "selectedAll": True, "delete": False, "itens": []},
        {"sigla": "municipio", "selectedAll": False, "delete": False, "itens": [-1]},
        {"sigla": "rm", "selectedAll": False, "delete": False, "itens": [-1]},
        {"sigla": "udh", "selectedAll": False, "delete": False, "itens": [-1]},
        {"sigla": "outros_territorios", "selectedAll": False, "delete": False, "itens": [-1]}
    ],
    "desagregacao": [],
    "ano": [1, 13, 14],
    "multiselectTerritorio": {
        "pais": {"sigla": "pais", "selected": []},
        "regiao": {"sigla": "regiao", "selected": []},
        "estado": {"sigla": "estado", "selected": []},
        "municipio": {"sigla": "municipio", "selected": []},
        "rm": {"sigla": "rm", "selected": []},
        "udh": {"sigla": "udh", "selected": []},
        "outros_territorios": {"selected": [], "sigla": "outros_territorios"}
    },
    "exportacao": False
}

max_attempts = 3
timeout = 60
response = None
for attempt in range(max_attempts):
    try:
        response = requests.post(url, headers=headers, json=data, timeout=timeout)
        if response.status_code == 200:
            break
    except Exception as e:
        time.sleep(2 * (attempt + 1))
else:
    errors[url + ' (EXTRAÇÃO)'] = traceback.format_exc()

try:
    result = response.json()
    columns = pd.DataFrame(result.get("columns", [])).query('title.str.lower().str.contains("ivs")')[['id', 'year']].values.tolist()
    id_year_dict = {col[0]: col[1] for col in columns}
    
    df = pd.DataFrame(result.get("data", []))
    df.rename(columns={str(k): int(v) for k, v in id_year_dict.items() if str(k) in df.columns}, inplace=True)
    df.drop('id', axis='columns', inplace=True)
    df.rename(columns={df.columns[0]: "Região"}, inplace=True)
    for col in df.columns[1:]:
        df[col] = df[col].str.replace(',', '.')
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(axis=1, how='all', inplace=True)

    c.to_csv(df, dbs_path, 'ipea (IVS).csv')

except:
    errors[url + ' (TRANSFORMAÇÃO)'] = traceback.format_exc()


# Nova request baseada na curl fornecida (IVS PNAD)
url_pnad = "https://ivs.ipea.gov.br/api/api/getgrid"
headers_pnad = {
    "sec-ch-ua-platform": "\"Windows\"",
    "Referer": "https://ivs.ipea.gov.br/",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "DNT": "1",
    "Content-Type": "application/json"
}
data_pnad = {
    "indicadores": {
        "variaveis": [
            {
                "id_variavel": 86,
                "sigla": "ivs_p",
                "exibir_na_consulta": True,
                "ordem": 102,
                "minimo": "0.0",
                "maximo": "1.0",
                "var_desag": None,
                "cod_desag": None,
                "fk_fonte": 2,
                "fk_arquivo": None,
                "numerador": "-",
                "denominador": "-",
                "decimais": 3,
                "id_tema": 36,
                "fonte": [
                    {
                        "id_fonte": 2,
                        "nome": "PNAD",
                        "sigla": "pnad",
                        "descricao": "Pesquisa Nacional por Amostra de Domicílios - IBGE",
                        "fk_arquivo": None,
                        "pivot": {
                            "fk_variavel": 86,
                            "fk_fonte": 2
                        }
                    }
                ],
                "lang_var": {
                    "id_lang_var": 86,
                    "nome_curto": "IVS",
                    "nome_longo": "Índice de Vulnerabilidade Social",
                    "nome_perfil": "IVS",
                    "descricao": "Índice de Vulnerabilidade Social. Média aritmética dos índices das dimensões: IVS Infraestrutura Urbana, IVS Capital Humano e IVS Renda e Trabalho.",
                    "lang": "pt-br",
                    "fk_variavel": 86
                },
                "pivot": {
                    "fk_tema": 36,
                    "fk_variavel": 86
                }
            }
        ],
        "indices": [],
        "subtemas": [],
        "allSelected": False
    },
    "pagination": {
        "current_page": 1,
        "last_page": 0,
        "per_page": 29,
        "total": 0,
        "from": 0,
        "to": 0
    },
    "ordenation": {
        "default": "asc"
    },
    "territorios": [
        {"sigla": "pais", "selectedAll": True, "delete": False, "itens": []},
        {"sigla": "regiao", "selectedAll": False, "delete": False, "itens": [2]},
        {"sigla": "estado", "selectedAll": True, "delete": False, "itens": []},
        {"sigla": "municipio", "selectedAll": False, "delete": False, "itens": [-1]},
        {"sigla": "rm", "selectedAll": False, "delete": False, "itens": [-1]},
        {"sigla": "udh", "selectedAll": False, "delete": False, "itens": [-1]},
        {"sigla": "outros_territorios", "selectedAll": False, "delete": False, "itens": [-1]}
    ],
    "desagregacao": [],
    "ano": [2,3,4,5,6,7,8,9,10,11,12,15],
    "multiselectTerritorio": {
        "pais": {"sigla": "pais", "selected": []},
        "regiao": {"sigla": "regiao", "selected": []},
        "estado": {"sigla": "estado", "selected": []},
        "municipio": {"sigla": "municipio", "selected": []},
        "rm": {"sigla": "rm", "selected": []},
        "udh": {"sigla": "udh", "selected": []},
        "outros_territorios": {"selected": [], "sigla": "outros_territorios"}
    },
    "exportacao": False
}

max_attempts_pnad = 3
timeout_pnad = 60
response_pnad = None
for attempt in range(max_attempts_pnad):
    try:
        response_pnad = requests.post(url_pnad, headers=headers_pnad, json=data_pnad, timeout=timeout_pnad)
        if response_pnad.status_code == 200:
            break
    except Exception:
        time.sleep(2 * (attempt + 1))
else:
    errors[url_pnad + ' (EXTRAÇÃO PNAD)'] = traceback.format_exc()

try:
    result_pnad = response_pnad.json()
    columns_pnad = pd.DataFrame(result_pnad.get("columns", [])).query('title.str.lower().str.contains("ivs")')[['id', 'year']].values.tolist()
    id_year_dict_pnad = {col[0]: col[1] for col in columns_pnad}

    df_pnad = pd.DataFrame(result_pnad.get("data", []))
    df_pnad.rename(columns={str(k): int(v) for k, v in id_year_dict_pnad.items() if str(k) in df_pnad.columns}, inplace=True)
    df_pnad.drop('id', axis='columns', inplace=True)
    df_pnad.rename(columns={df_pnad.columns[0]: "Região"}, inplace=True)
    for col in df_pnad.columns[1:]:
        df_pnad[col] = df_pnad[col].str.replace(',', '.')
        df_pnad[col] = pd.to_numeric(df_pnad[col], errors='coerce')
    df_pnad.dropna(axis=1, how='all', inplace=True)

    c.to_csv(df_pnad, dbs_path, 'ipea (IVS PNAD).csv')

except Exception:
    errors[url_pnad + ' (TRANSFORMAÇÃO PNAD)'] = traceback.format_exc()



# ************************
# PLANILHA
# ************************

'''
COMANDO IGNORADO PORQUE A PLANILHA G20.3 FOI AUTOMATIZADA POR RODRIGO, ELE A ESTRUTUROU DE FORMA DIFERENTE.
SE ADICIONAR ESSA AO GITHUB RETORNARÁ ERRO NA FIGURA DO DASHBOARD!
'''

# g20.3
try:
    data = c.open_file(dbs_path, 'ipea (IVS).csv', 'csv')

    # ranqueamento do ano mais recente
    max_year = data.columns[1:].astype(int).max()
    df = data[[data.columns[0], str(max_year)]]
    df['dummy'] = df[str(max_year)]
    df.loc[df['Região'].isin(['Brasil', 'Nordeste']), 'dummy'] = np.nan
    df['Colocação'] = df['dummy'].rank(ascending=False, method='first')
    df.sort_values(by=['Colocação', 'Região'], inplace=True)
    max_rank = df['Colocação'].max()

    df_ranked = df.query('`Colocação` <= 6 or `Região` in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_ranked.rename(columns={str(max_year): 'Valor'}, inplace=True)
    df_ranked['Ano'] = '31/12/' + str(max_year)
    df_ranked['Variável'] = 'IVS'
    df_final = df_ranked[['Região', 'Variável', 'Ano', 'Valor', 'Colocação']].copy()
    df_final['Colocação'] = df_final['Colocação'].apply(lambda x: f'{int(x)}º' if pd.notna(x) else x)

    c.to_excel(df_final, sheets_path, 'g20.3a.xlsx')

    # ranqueamento da variação dos anos
    df_melted = data.melt(id_vars=['Região'], var_name='Ano', value_name='IVS')
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    max_year = df_melted['Ano'].max()
    min_year = df_melted['Ano'].min()
    df_melted.sort_values(by=['Ano', 'Região'], inplace=True)
    df_melted['Valor'] = df_melted.groupby('Região')['IVS'].diff()
    df_melted['dummy'] = df_melted['Valor']
    df_melted.loc[df_melted['Região'].isin(['Brasil', 'Nordeste']), 'dummy'] = np.nan
    df_melted['Colocação'] = df_melted.groupby('Ano')['dummy'].rank(ascending=False, method='first')
    df_melted = df_melted[df_melted['Valor'].notnull()]
    df_melted.sort_values(by=['Colocação', 'Região'], inplace=True)
    df_melted['Variável'] = f'Diferença {max_year}-{min_year}'

    df_final = df_melted.query('`Colocação` <= 6 or `Região` in ["Brasil", "Nordeste", "Sergipe"]')[['Região', 'Variável', 'Valor', 'Colocação']].copy()
    df_final['Colocação'] = df_final['Colocação'].apply(lambda x: f'{int(x)}º' if pd.notna(x) else x)

    c.to_excel(df_final, sheets_path, 'g20.3b.xlsx')

except Exception as e:
    errors['Gráfico 20.3'] = traceback.format_exc()

# g20.4
try:
    data = c.open_file(dbs_path, 'ipea (IVS PNAD).csv', 'csv').query('Região in ["Brasil", "Nordeste", "Sergipe"]')
    df = data.melt(id_vars=['Região'], var_name='Ano', value_name='Valor')
    df['Ano'] = '31/12/' + df['Ano'].astype(str)
    df['Variável'] = 'IVS'
    df.sort_values(by=['Região', 'Ano'], inplace=True)

    c.to_excel(df[['Região', 'Variável', 'Ano', 'Valor']], sheets_path, 'g20.4.xlsx')

except Exception as e:
    errors['Gráfico 20.4'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g20.3--g20.4.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
