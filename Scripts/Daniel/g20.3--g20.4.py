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

# url
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

    # territoriedade
    driver.click([
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/button',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[3]/a/label',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[4]/a/label',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/ul/li[5]/a/label',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[1]/span/div/button'
    ])

    # macrorregião
    driver.click([
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/button',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/ul/li[4]/a/label',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[1]/div[2]/span/div/button'
    ])

    # índices
    driver.click([
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/button',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/ul/li[3]/a/label',
        '/html/body/div[3]/div[2]/div[1]/fieldset/div/div/p[2]/div[1]/span/div/button'
    ])

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
    df.columns = ['Região', 'Ano', 'IVS']
    c.convert_type(df, 'IVS', 'float')

    df.sort_values(by=['Região', 'Ano'], inplace=True)

    c.to_csv(df, dbs_path, 'ipea (IVS).csv')

    driver.quit()

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (IVS)'] = traceback.format_exc()


# ************************
# PLANILHA
# ************************

'''
COMANDO IGNORADO PORQUE A PLANILHA G20.3 FOI AUTOMATIZADA POR RODRIGO, ELE A ESTRUTUROU DE FORMA DIFERENTE.
SE ADICIONAR ESSA AO GITHUB RETORNARÁ ERRO NA FIGURA DO DASHBOARD!
'''

# # g20.3
# try:
#     data = c.open_file(dbs_path, 'ipea (IVS).csv', 'csv')
#
#     # filtragem dos valores referentes ao ano inicial e final da série temporal; pivoting para posterior cálculo
#     df = data.loc[
#         (data['Ano'] == data['Ano'].max()) |
#         (data['Ano'] == data['Ano'].min())
#         ].pivot(index='Região', columns='Ano', values='IVS').reset_index().sort_values(
#         by=data['Ano'].max(), ascending=False).reset_index(drop=True).copy()
#
#     df['Variação'] = df[df.columns[-1]].astype('float') - df[df.columns[-2]].astype('float')
#     df.columns = [col if isinstance(col, str) else f'IVS/{str(col)}' for col in df.columns]
#     df[df.columns[1:]] = df[df.columns[1:]].astype('float')
#     df = df.sort_values(by=['Variação', 'Região'], ascending=[True, False])
#
#     c.to_excel(df, sheets_path, 'g20.3.xlsx')
#
# except Exception as e:
#     errors['Gráfico 20.3'] = traceback.format_exc()

# g20.4
try:
    data = c.open_file(dbs_path, 'ipea (IVS).csv', 'csv')

    df = data.loc[data['Região'].isin(['Brasil', 'Nordeste', 'Sergipe'])].copy()

    c.convert_type(df, 'Ano', 'datetime')
    c.convert_type(df, 'IVS', 'float')

    c.to_excel(df, sheets_path, 'g20.4.xlsx')

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
