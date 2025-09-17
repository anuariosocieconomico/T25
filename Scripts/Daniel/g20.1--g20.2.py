import time
import functions as c
import os
import json
import traceback
import tempfile
import shutil
import pandas as pd
import numpy as np
import requests


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
FONTE ANTIGA DA FIGURA
"""
# # VARIÁVEL IDHM
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

#     # seleciona a territoriedade
#     driver.click([
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/button',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[3]/a/label',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[4]/a/label',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[5]/a/label',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/button'
#     ])

#     # seleciona macrorregiões
#     driver.click([
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/button',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/ul/li[4]/a/label',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/button'
#     ])

#     # seleciona índices
#     driver.click([
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/button',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/ul/li[4]/a/label',
#         '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/button'
#     ])

#     # abre a tabela
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
#     df.columns = ['Região', 'Ano', 'IDHM']
#     c.convert_type(df, 'IDHM', 'float')

#     df.sort_values(by=['Região', 'Ano'], inplace=True)

#     c.to_csv(df, dbs_path, 'ipea (IDHM).csv')

#     driver.quit()

# # registro do erro em caso de atualizações
# except Exception as e:
#     errors[url + ' (IDHM)'] = traceback.format_exc()

"""
NOVA FONTE DA FIGURA
"""
url = "https://ivs.ipea.gov.br/api/api/getgrid"
headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "DNT": "1",
    "Origin": "https://ivs.ipea.gov.br",
    "Referer": "https://ivs.ipea.gov.br/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

payload = {
    "indicadores": {
        "variaveis": [
            {
                "id_variavel": 21,
                "sigla": "idhm_c",
                "exibir_na_consulta": True,
                "ordem": 95,
                "minimo": "0.0",
                "maximo": "1.0",
                "var_desag": None,
                "cod_desag": None,
                "fk_fonte": 1,
                "fk_arquivo": None,
                "numerador": "-",
                "denominador": "-",
                "decimais": 3,
                "id_tema": 46,
                "fonte": [
                    {
                        "id_fonte": 1,
                        "nome": "Censo",
                        "sigla": "censo",
                        "descricao": "Censo Demográfico - IBGE",
                        "fk_arquivo": None,
                        "pivot": {
                            "fk_variavel": 21,
                            "fk_fonte": 1
                        }
                    }
                ],
                "lang_var": {
                    "id_lang_var": 21,
                    "nome_curto": "IDHM",
                    "nome_longo": "Índice de Desenvolvimento Humano Municipal",
                    "nome_perfil": "IDHM",
                    "descricao": "Índice de Desenvolvimento Humano Municipal. Média geométrica dos índices das dimensões Renda, Educação e Longevidade, com pesos iguais.",
                    "lang": "pt-br",
                    "fk_variavel": 21
                },
                "pivot": {
                    "fk_tema": 46,
                    "fk_variavel": 21
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
        "total": 29,
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

try:
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    result = response.json()
    columns = pd.DataFrame(result.get("columns", [])).query('title.str.lower().str.contains("idhm")')[['id', 'year']].values.tolist()
    id_year_dict = {col[0]: col[1] for col in columns}
    
    df = pd.DataFrame(result.get("data", []))
    df.rename(columns={str(k): int(v) for k, v in id_year_dict.items() if str(k) in df.columns}, inplace=True)
    df.drop('id', axis='columns', inplace=True)
    df.rename(columns={df.columns[0]: "Região"}, inplace=True)
    for col in df.columns[1:]:
        df[col] = df[col].str.replace(',', '.')
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(axis=1, how='all', inplace=True)

    c.to_csv(df, dbs_path, "ipea (IDHM).csv")

except Exception as e:
    errors[url + " (IDHM API)"] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

'''
COMANDO IGNORADO PORQUE A PLANILHA G20.1 FOI AUTOMATIZADA POR RODRIGO, ELE A ESTRUTUROU DE FORMA DIFERENTE.
SE ADICIONAR ESSA AO GITHUB RETORNARÁ ERRO NA FIGURA DO DASHBOARD!
'''

# g20.1
try:
    data = c.open_file(dbs_path, 'ipea (IDHM).csv', 'csv')

    # ranqueamento do ano mais recente
    max_year = data.columns[1:].astype(int).max()
    df = data[[data.columns[0], str(max_year)]]
    df['dummy'] = df[str(max_year)]
    df.loc[df['Região'].isin(['Brasil', 'Nordeste']), 'dummy'] = np.nan
    df['Colocação'] = df['dummy'].rank(ascending=False, method='first')
    df.sort_values(by=['Colocação', 'Região'], inplace=True)
    max_rank = df['Colocação'].max()

    df_ranked = df.query('`Colocação` >= @max_rank - 5 or `Região` in ["Brasil", "Nordeste", "Sergipe"]').copy()
    df_ranked.rename(columns={str(max_year): 'Valor'}, inplace=True)
    df_ranked['Ano'] = '31/12/' + str(max_year)
    df_ranked['Variável'] = 'IDHM'
    df_final = df_ranked[['Região', 'Variável', 'Ano', 'Valor', 'Colocação']].copy()
    df_final['Colocação'] = df_final['Colocação'].apply(lambda x: f'{int(x)}º' if pd.notna(x) else x)

    c.to_excel(df_final, sheets_path, 'g20.1a.xlsx')

    # ranqueamento da variação dos anos
    df_melted = data.melt(id_vars=['Região'], var_name='Ano', value_name='IDHM')
    df_melted['Ano'] = df_melted['Ano'].astype(int)
    max_year = df_melted['Ano'].max()
    min_year = df_melted['Ano'].min()
    df_melted.sort_values(by=['Ano', 'Região'], inplace=True)
    df_melted['Valor'] = df_melted.groupby('Região')['IDHM'].diff()
    df_melted['dummy'] = df_melted['Valor']
    df_melted.loc[df_melted['Região'].isin(['Brasil', 'Nordeste']), 'dummy'] = np.nan
    df_melted['Colocação'] = df_melted.groupby('Ano')['dummy'].rank(ascending=False, method='first')
    df_melted = df_melted[df_melted['Valor'].notnull()]
    df_melted.sort_values(by=['Colocação', 'Região'], inplace=True)
    df_melted['Variável'] = f'Diferença {max_year}-{min_year}'

    df_final = df_melted.query('`Colocação` <= 6 or `Região` in ["Brasil", "Nordeste", "Sergipe"]')[['Região', 'Variável', 'Valor', 'Colocação']].copy()
    df_final['Colocação'] = df_final['Colocação'].apply(lambda x: f'{int(x)}º' if pd.notna(x) else x)

    c.to_excel(df_final, sheets_path, 'g20.1b.xlsx')

except Exception as e:
    errors['Gráfico 20.1'] = traceback.format_exc()

# g20.2
try:
    data = c.open_file(dbs_path, 'ipea (IDHM).csv', 'csv').query('Região in ["Brasil", "Nordeste", "Sergipe"]')
    df = data.melt(id_vars=['Região'], var_name='Ano', value_name='Valor')
    df['Ano'] = '31/12/' + df['Ano'].astype(str)
    df['Variável'] = 'IDHM'
    df.sort_values(by=['Região', 'Ano'], inplace=True)

    c.to_excel(df[['Região', 'Variável', 'Ano', 'Valor']], sheets_path, 'g20.2.xlsx')

except Exception as e:
    errors['Gráfico 20.2'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g20.1--g20.2.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
