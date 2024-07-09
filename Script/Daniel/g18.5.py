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
url = 'http://www.ans.gov.br/anstabnet/cgi-bin/dh?dados/tabnet_rec.def'
driver = c.Google(visible=False, rep=dbs_path)  # instância do objeto driver do Selenium
# tenta baixar o arquivo conforme os comandos anteriores; caso haja alguma atualização no site, registra-se o erro
try:
    driver.get(url)  # acessa a página

    # seleciona as opções da tabela
    driver.click([
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[1]/option[8]',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[2]/option[2]',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[3]/option',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/select[3]/option'
    ])

    # seleciona todos os períodos disponíveis
    driver.periods('select', 'A', 'option', all_periods=True)

    # abre a tabela
    driver.click([
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/table[2]/tbody/tr[9]/td/input',
        '/html/body/table[2]/thead/tr[2]/td[2]/center/form/table[2]/tbody/tr[9]/td/p[2]/input[1]'
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
    #####
    # VARIÁVEL - RECLAMAÇÕES - PRIMEIRA BASE
    df_rec = c.open_file(dbs_path, file_name, 'csv')
    cols = df_rec.columns[1:]
    df_rec_melted = df_rec.melt(id_vars='UF', value_vars=cols, var_name='Ano', value_name='Valor')
    df_rec_melted['Ano'] = df_rec_melted['Ano'].apply(lambda x: '20' + x.split('/')[1])

    # soma dos dados de cada mês para encontrar valor anual, já que a segunda tabela só possui dados anuais
    df_rec_grouped = df_rec_melted.groupby(['UF', 'Ano'])['Valor'].sum().reset_index()
    df_rec_grouped.sort_values(by=['UF', 'Ano'], inplace=True)

    #####
    # VARIÁVEL - BENEFICIÁRIOS - SEGUNDA BASE
    df_benef = c.open_file(dbs_path, file_name2, 'csv')
    cols = df_benef.columns[1:]
    df_benef_melted = df_benef.melt(id_vars='UF', value_vars=cols, var_name='Ano', value_name='Valor')
    df_benef_melted['Ano'] = df_benef_melted['Ano'].apply(lambda x: '20' + x.split('/')[1])
    df_benef_melted.sort_values(by=['UF', 'Ano'], inplace=True)

    # as duas tabelas que compõem a figura não possuem um ano base em comum, nem um ano final
    # aqui é verificado qual o ano base é mais recente para adotar como padrão em ambas as tabelas; idem ano final
    base_year = max(df_rec_grouped.Ano.unique()[0], df_benef_melted.Ano.unique()[0])
    max_year = min(df_rec_grouped.Ano.unique()[-1], df_benef_melted.Ano.unique()[-1])
    df_rec_grouped = df_rec_grouped.loc[(df_rec_grouped['Ano'] >= base_year) & (df_rec_grouped['Ano'] <= max_year)]
    df_rec_grouped['Var'] = 'reclamações'

    df_benef_melted = df_benef_melted.loc[(df_benef_melted['Ano'] >= base_year) & (df_benef_melted['Ano'] <= max_year)]
    df_benef_melted['Var'] = 'beneficiários'

    df_concat = pd.concat([df_rec_grouped, df_benef_melted], ignore_index=True)
    df_pivoted = df_concat.pivot(index=['UF', 'Ano'], columns='Var', values='Valor').reset_index()

    # filtragem dos dados de interesse
    ne_states = ['Alagoas', 'Bahia', 'Ceará', 'Maranhão', 'Paraíba',
                 'Pernambuco', 'Piauí', 'Rio Grande do Norte', 'Sergipe']

    ne_data = df_pivoted.loc[df_pivoted['UF'].isin(ne_states)].copy()
    ne_data['UF'] = 'Nordeste'
    ne_data_grouped = ne_data.groupby(['UF', 'Ano'])[['beneficiários', 'reclamações']].sum().reset_index()

    br_se_data = df_pivoted.loc[df_pivoted['UF'].isin(['TOTAL', 'Sergipe'])]
    br_se_data.loc[br_se_data['UF'] == 'TOTAL', 'UF'] = 'Brasil'

    # união dos dados
    df_united = pd.concat([ne_data_grouped, br_se_data], ignore_index=True)
    df_united.rename(columns={'UF': 'Região'}, inplace=True)
    df_united.sort_values(by=['Região', 'Ano'], inplace=True)

    # cálculo da taxa
    df_united['Taxa de reclamações'] = (df_united['reclamações'] / df_united['beneficiários']) * 100
    df_united.drop(['beneficiários', 'reclamações'], axis='columns', inplace=True)

    df_united['Região'] = df_united['Região'].astype('str')
    df_united['Ano'] = pd.to_datetime(df_united['Ano'], format='%Y')
    df_united['Ano'] = df_united['Ano'].dt.strftime('%d/%m/%Y')
    df_united['Taxa de reclamações'] = df_united['Taxa de reclamações'].astype('float64')

    c.to_excel(df_united, sheets_path, 'g18.5.xlsx')

except Exception as e:
    errors['Gráfico 18.5'] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g18.5.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
