import time
import functions as c
import os
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

# VARIÁVEL IDHM
url = 'http://ivs.ipea.gov.br/ivs_componentes/grid/'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium
    driver.get(url)  # acessa a página

    if driver.get_tag('/html/body/center[1]/h1').text == '404 Not Found':
        driver.quit()
        errors[url + ' (IDHM)'] = 'Página não carregada'
        raise Exception('Página não carregada')

    driver.wait('/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/button')
    time.sleep(2)
    driver.random_click()

    # seleciona a territoriedade
    driver.click([
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/button',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[3]/a/label',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[4]/a/label',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[5]/a/label',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/button'
    ])

    # seleciona macrorregiões
    driver.click([
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/button',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/ul/li[4]/a/label',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/button'
    ])

    # seleciona índices
    driver.click([
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/button',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/ul/li[4]/a/label',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/button'
    ])

    # abre a tabela
    driver.click('/html/body/div[3]/div[2]/div[1]/fieldset/div/div/div[13]/button[2]')

    # baixa o arquivo
    driver.wait(
        '/html/body/div[3]/div[3]/div/div[3]/div[2]/div/table/thead/tr[2]/th[4]/div/table/tbody/tr/td[2]/button')
    driver.click('/html/body/div[3]/div[3]/div/div[5]/div/table/tbody/tr/td[1]/table/tbody/tr/td/div')
    time.sleep(3)

    data = c.open_file(dbs_path, os.listdir(dbs_path)[0], ext='xls')
    df = data[list(data.keys())[0]]
    df.loc[(df['Nome da Região'] == '-') & (df['Nome da UF'] == '-'), 'Nome da UF'] = 'Brasil'
    df.loc[(df['Nome da Região'] == 'NORDESTE') & (df['Nome da UF'] == '-'), 'Nome da UF'] = 'Nordeste'
    df.drop(df.columns[:-3], axis='columns', inplace=True)
    df.columns = ['Região', 'Ano', 'IDHM']
    c.convert_type(df, 'IDHM', 'float')

    df.sort_values(by=['Região', 'Ano'], inplace=True)

    c.to_csv(df, dbs_path, 'ipea (IDHM).csv')

    driver.quit()

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (IDHM)'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

'''
COMANDO IGNORADO PORQUE A PLANILHA G20.1 FOI AUTOMATIZADA POR RODRIGO, ELE A ESTRUTUROU DE FORMA DIFERENTE.
SE ADICIONAR ESSA AO GITHUB RETORNARÁ ERRO NA FIGURA DO DASHBOARD!
'''

# # g20.1
# try:
#     data = c.open_file(dbs_path, 'ipea (IDHM).csv', 'csv')
#
#     # seleção dos dados referentes ao primeiro e ao último ano da série temporal
#     df = data.loc[(data['Ano'] == data['Ano'].min()) | (data['Ano'] == data['Ano'].max())].copy()
#
#     # pivoting para horizontalizar o idhm referente a cada período, para posterior cálculo
#     df_pivoted = df.pivot(index='Região', columns='Ano', values='IDHM').reset_index().copy()
#
#     df_pivoted.columns = ['Região'] + [f'IDHM/{str(col)}' for col in df_pivoted.columns[1:]]
#
#     df_pivoted['Variação'] = df_pivoted[df_pivoted.columns[-1]].astype('float') \
#                              - df_pivoted[df_pivoted.columns[-2]].astype('float')
#
#     df_pivoted[df_pivoted.columns[1:]] = df_pivoted[df_pivoted.columns[1:]].astype('float')
#     df_pivoted.sort_values(by=['Variação', 'Região'], ascending=[False, True], inplace=True)
#
#     c.to_excel(df_pivoted, sheets_path, 'g20.1.xlsx')
#
# except Exception as e:
#     errors['Gráfico 20.1'] = traceback.format_exc()

# g20.2
try:
    data = c.open_file(dbs_path, 'ipea (IDHM).csv', 'csv')

    data.sort_values(by=['Região', 'Ano'], inplace=True)

    c.convert_type(data, 'IDHM', 'float')
    c.convert_type(data, 'Ano', 'datetime')

    df = data.loc[data['Região'].isin(['Brasil', 'Nordeste', 'Sergipe'])]

    c.to_excel(df, sheets_path, 'g20.2.xlsx')

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
