import functions as c
import os
import pandas as pd
import json
from bs4 import BeautifulSoup
from io import StringIO
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
url = 'http://www.ans.gov.br/anstabnet/cgi-bin/dh?dados/tabnet_res.def'
driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click([
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[1]/option[18]',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[2]/option[2]',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[3]/option[1]',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[3]/option[1]'
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=True)

    # abre a tabela
    driver.click([
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/table[2]/tbody/tr[20]/td/input',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/table[2]/tbody/tr[20]/td/p[2]/input[1]'
    ])

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.wait('/html/body/table[2]/thead/tr[2]/td[2]/table/tbody/tr/td[1]/a')

    # extrai a tabela da html (ao baixar diretamente no botão, a tabela retornada era incompleta)
    html = driver.browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    df = pd.read_html(StringIO(str(soup)), skiprows=1, header=2, thousands='.')[0]
    df = df.iloc[:-1]
    for col in df.columns[1:]:
        df[col] = df[col].astype(int)
    c.to_csv(df, dbs_path, 'base1.csv')
    file_name = 'base1.csv'

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (PRIMEIRA BASE)'] = traceback.format_exc()

# url
url = 'http://www.ans.gov.br/anstabnet/cgi-bin/dh?dados/tabnet_br.def'

# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click([
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[1]/option[11]',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[2]/option[2]',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[3]/option[1]',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[3]/option[1]'
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=False, prefix='Dez')

    # abre a tabela
    driver.click([
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/table[2]/tbody/tr[15]/td/input',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/table[2]/tbody/tr[15]/td/p[2]/input[1]'
    ])

    # altera para a última aba aberta e baixa o arquivo
    driver.change_window()
    driver.wait('/html/body/table[2]/thead/tr[2]/td[2]/table/tbody/tr/td[1]/a')

    # extrai a tabela da html (ao baixar diretamente no botão, a tabela retornada era incompleta)
    html = driver.browser.page_source
    soup = BeautifulSoup(html, 'html.parser')
    df = pd.read_html(StringIO(str(soup)), skiprows=1, thousands='.')[0]
    df = df.iloc[:-1]
    for col in df.columns[1:]:
        df[col] = df[col].astype(int)
    c.to_csv(df, dbs_path, 'base2.csv')
    file_name2 = 'base2.csv'

# registro do erro em caso de atualizações
except Exception as e:
    errors[url + ' (SEGUNDA BASE)'] = traceback.format_exc()

driver.quit()

# ************************
# PLANILHA
# ************************

try:
    files = [file_name, file_name2]
    dfs = []
    for f in files:
        df = c.open_file(dbs_path, f, 'csv')
        cols = df.columns[1:]
        if f == file_name2:
            # na tabela 2 os períodos estão armazenados no formato Dec/2020, por exemplo
            df_melted = df.melt(id_vars='UF', value_vars=cols, var_name='Ano', value_name='Valor')
            df_melted['Ano'] = df_melted['Ano'].apply(lambda x: '20' + x.split('/')[1])
            df_melted['Variável'] = 'beneficiários'
            df_melted = df_melted[['UF', 'Ano', 'Variável', 'Valor']]
            df_melted.sort_values(by=['UF', 'Ano'], inplace=True)
        else:
            df_melted = df.melt(id_vars='UF', value_vars=cols, var_name='Ano', value_name='Valor')
            df_melted['Variável'] = 'atendimentos'
            df_melted = df_melted[['UF', 'Ano', 'Variável', 'Valor']]
            df_melted.sort_values(by=['UF', 'Ano'], inplace=True)

        df_melted['UF'] = df_melted['UF'].astype('str')
        df_melted['Ano'] = pd.to_datetime(df_melted['Ano'], format='%Y')
        df_melted['Ano'] = df_melted['Ano'].dt.strftime('%d/%m/%Y')
        df_melted['Valor'] = df_melted['Valor'].astype('int64')

        dfs.append(df_melted)

    df_atend = dfs[0]
    df_benef = dfs[1]

    # verifica entre as duas tabelas qual possui o ano base mais recente e adota como padrão para ambas
    min_year = max(df_atend.Ano.unique()[0], df_benef.Ano.unique()[0])
    df_atend = df_atend.loc[df_atend['Ano'] >= min_year]
    df_benef = df_benef.loc[df_benef['Ano'] >= min_year]

    ne_states = ['Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba',
                 'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe']

    # Variável Atendimentos
    # agrupamento de dados para obter valores referentes a Nordeste
    df_atend_ne = df_atend.loc[df_atend['UF'].isin(ne_states)]
    assert len(df_atend_ne.UF.unique()) == len(ne_states)
    df_atend_ne.loc[:, 'UF'] = 'Nordeste'
    df_atend_ne_sum = df_atend_ne.groupby(['UF', 'Ano', 'Variável'])['Valor'].sum().reset_index()

    # seleção de Sergipe e Brasil (Total)
    df_atend_se_br = df_atend.loc[df_atend['UF'].isin(['Sergipe', 'TOTAL'])]
    assert len(df_atend_se_br.UF.unique()) == 2
    df_atend_se_br.loc[df_atend_se_br['UF'] == 'TOTAL', 'UF'] = 'Brasil'

    # união dos dados
    df_atend_all = pd.concat([df_atend_se_br, df_atend_ne_sum], ignore_index=True)
    df_atend_all.sort_values(by=['UF', 'Ano', 'Variável'], inplace=True)

    # Variável Beneficiários
    # agrupamento de dados para obter valores referentes a Nordeste
    df_benef_ne = df_benef.loc[df_benef['UF'].isin(ne_states)]
    assert len(df_benef_ne.UF.unique()) == len(ne_states)
    df_benef_ne.loc[:, 'UF'] = 'Nordeste'
    df_benef_ne_sum = df_benef_ne.groupby(['UF', 'Ano', 'Variável'])['Valor'].sum().reset_index()

    # seleção de Sergipe e Brasil (Total)
    df_benef_se_br = df_benef.loc[df_benef['UF'].isin(['Sergipe', 'TOTAL'])]
    assert len(df_benef_se_br.UF.unique()) == 2
    df_benef_se_br.loc[df_benef_se_br['UF'] == 'TOTAL', 'UF'] = 'Brasil'

    # união dos dados
    df_benef_all = pd.concat([df_benef_se_br, df_benef_ne_sum], ignore_index=True)
    df_benef_all.sort_values(by=['UF', 'Ano', 'Variável'], inplace=True)

    # união de ambas as tabelas
    df_united = pd.concat([df_atend_all, df_benef_all], ignore_index=True)
    df_united_pivoted = pd.pivot(df_united, values='Valor', index=['UF', 'Ano'], columns='Variável').reset_index()
    df_united_pivoted['Taxa de atendimento'] = (df_united_pivoted.iloc[:, -2] / df_united_pivoted.iloc[:, -1]) * 100
    df_united_pivoted = df_united_pivoted[['UF', 'Ano', 'Taxa de atendimento']]
    df_united_pivoted.rename(columns={'UF': 'Região'})
    df_united_pivoted['Taxa de atendimento'] = df_united_pivoted['Taxa de atendimento'].astype('float64')

    c.to_excel(df_united_pivoted, sheets_path, 'g18.4.xlsx')

except Exception as e:
    errors['Gráfico 18.4'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g18.4.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
