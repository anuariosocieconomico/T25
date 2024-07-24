import functions as c
import os
import pandas as pd
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

# VARIÁVEL - ÓBITOS POR CAUSAS EVITÁVEIS - FAIXA ETÁRIA 0 A 4
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?sim/cnv/evita10uf.def'
driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[2]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[6]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]'
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=True)

    # abre a tabela
    driver.click('/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]')

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    file_name = os.listdir(dbs_path)[0]

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (PRIMEIRA BASE)'] = traceback.format_exc()

# VARIÁVEL - POPULAÇÃO DA MESMA FAIXA ETÁRIA
url = 'http://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge/cnv/projpopuf.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[2]/div/div[1]/select/option[2]',
        '/html/body/div/div/center/div/form/div[2]/div/div[2]/select/option[4]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]',
        '/html/body/div/div/center/div/form/div[2]/div/div[3]/select/option[1]'
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=True)

    # abre a tabela
    driver.click([
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/img',
        '/html/body/div/div/center/div/form/div[4]/div[1]/div/div[5]/select[2]/option[2]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[1]/div[1]/input[1]',
        '/html/body/div/div/center/div/form/div[4]/div[2]/div[2]/input[1]'
    ])

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.download('/html/body/div/div/div[3]/table/tbody/tr/td[1]/a')

    # verifica se a primeira base foi realmente baixada
    file_name2 = [f for f in os.listdir(dbs_path) if f != file_name][0]

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (SEGUNDA BASE)'] = traceback.format_exc()

driver.quit()

# ************************
# PLANILHA
# ************************

try:
    df_obt = c.open_file(dbs_path, file_name, 'csv', sep=';', encoding='cp1252', skiprows=3)
    lin = df_obt.query(r'`Região/Unidade da Federação` == "Total"').index.values[0]
    df_obt = df_obt.iloc[:lin + 1].copy()

    df_pop = c.open_file(dbs_path, file_name2, 'csv', sep=';', encoding='cp1252', skiprows=4)
    lin = df_pop.query(r'`Região/Unidade da Federação` == "Total"').index.values[0]
    df_pop = df_pop.iloc[:lin + 1].copy()

    cols = df_obt.columns[1:-1]
    df_obt_melted = df_obt.melt('Região/Unidade da Federação', cols, 'Ano', 'Valor')
    df_obt_melted.sort_values(by='Ano', inplace=True)

    cols = df_pop.columns[1:]
    df_pop_melted = df_pop.melt('Região/Unidade da Federação', cols, 'Ano', 'Valor')
    df_pop_melted.sort_values(by='Ano', inplace=True)

    # as duas tabelas não possuem uma série temporal com mesmo ano de início e fim
    # aqui é verificado o ano mínimo e máximo em comum entre ambas as tabelas para padronização
    min_year = max(df_obt_melted.Ano.unique()[0], df_pop_melted.Ano.unique()[0])
    max_year = min(df_obt_melted.Ano.unique()[-1], df_pop_melted.Ano.unique()[-1])

    # filtragem dos valores com base nos anos limites
    df_obt_melted = df_obt_melted.loc[(df_obt_melted['Ano'] >= min_year) & (df_obt_melted['Ano'] <= max_year)]
    df_pop_melted = df_pop_melted.loc[(df_pop_melted['Ano'] >= min_year) & (df_pop_melted['Ano'] <= max_year)]

    # tratamento para união das tabelas e posterior cálculo
    df_obt_melted['Var'] = 'óbitos'
    df_pop_melted['Var'] = 'pop'
    df_concat = pd.concat([df_obt_melted, df_pop_melted], ignore_index=True)
    df_pivot = df_concat.pivot(index=['Região/Unidade da Federação', 'Ano'], columns='Var', values='Valor').reset_index()
    df_pivot.loc[:, 'Região/Unidade da Federação'] = df_pivot['Região/Unidade da Federação'].apply(
        lambda x: x.split()[-1] if not x.startswith('Região') else x
    )

    # sergipe, nordeste e brasil
    df_united = df_pivot.loc[df_pivot['Região/Unidade da Federação'].isin(['Sergipe', 'Região Nordeste', 'Total'])].copy()
    df_united.loc[df_united['Região/Unidade da Federação'] == 'Total', 'Região/Unidade da Federação'] = 'Brasil'
    df_united.loc[df_united['Região/Unidade da Federação'] == 'Região Nordeste', 'Região/Unidade da Federação'] = 'Nordeste'

    df_united['Taxa de óbitos'] = (df_united['óbitos'] / df_united['pop']) * 100
    df_united.drop(['óbitos', 'pop'], axis='columns', inplace=True)

    df_united['Região/Unidade da Federação'] = df_united['Região/Unidade da Federação'].astype('str')
    df_united['Ano'] = pd.to_datetime(df_united['Ano'], format='%Y')
    df_united['Ano'] = df_united['Ano'].dt.strftime('%d/%m/%Y')
    df_united['Taxa de óbitos'] = df_united['Taxa de óbitos'].astype('float64')

    df_united.sort_values(by=['Região/Unidade da Federação', 'Ano'], inplace=True)
    df_united.rename(columns={'Região/Unidade da Federação': 'Região'}, inplace=True)
    c.to_excel(df_united, sheets_path, 'g18.7.xlsx')

except Exception as e:
    errors['Gráfico 18.7'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g18.7.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
