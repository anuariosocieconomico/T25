import functions as c
import os
import pandas as pd
import datetime
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
# DOWNLOAD DA BASE DE DADOS E ELABORAÇÃO DA PLANILHA
# ************************

siconfi_url = 'https://apidatalake.tesouro.gov.br'

try:
    # definição dos anos de referência
    base_year = 2015
    current_year = datetime.datetime.now().year

    dfs = []
    for y in range(base_year, current_year + 1):

        siconfi_url = f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rgf?an_exercicio={y}&" \
                      f"in_periodicidade=Q&nr_periodo=3&co_tipo_demonstrativo=RGF&no_anexo=RGF-Anexo%2002&" \
                      f"co_esfera=E&co_poder=E&id_ente=28"

        response = c.open_url(siconfi_url)
        if response.status_code == 200 and len(response.json()['items']) > 1:

            # seleção das colunas de interesse
            # seleção do quadrimestre de interesse
            # seleção das contas de interesse
            df = pd.DataFrame(response.json()['items'])
            df = df.loc[:, ['exercicio', 'instituicao', 'uf', 'coluna', 'conta', 'valor']]
            df = df.loc[df['coluna'] == 'Até o 3º Quadrimestre']
            df = df.loc[(df['conta'].str.startswith('DÍVIDA CONSOLIDADA LÍQUIDA (DCL)')) |
                        (df['conta'].str.startswith('RECEITA CORRENTE LÍQUIDA - RCL')) |
                        (df['conta'].str.startswith('% da DCL sobre a RCL'))]

            dfs.append(df)

    # união dos dfs
    df_concat = pd.concat(dfs, ignore_index=True)

    df_concat['conta'] = df_concat['conta'].apply(
        lambda x: '% da DCL sobre a RCL' if x.startswith('% da DCL sobre a RCL') else (
            'DÍVIDA CONSOLIDADA LÍQUIDA' if x.startswith('DÍVIDA CONSOLIDADA LÍQUIDA') else
            'RECEITA CORRENTE LÍQUIDA'))

    # ordenação dos valores
    df_concat.sort_values(by=['uf', 'exercicio', 'conta'], ascending=[True] * 3, inplace=True)
    cols = df_concat.columns
    df_concat.drop([cols[1], cols[2], cols[3]], axis='columns', inplace=True)

    # classificação dos dados
    df_concat[df_concat.columns[0]] = pd.to_datetime(df_concat[df_concat.columns[0]], format='%Y')
    df_concat[df_concat.columns[0]] = df_concat[df_concat.columns[0]].dt.strftime('%d/%m/%Y')
    df_concat[['conta']] = df_concat[['conta']].astype('str')
    df_concat['valor'] = df_concat['valor'].astype('float64')

    # conversão em arquivo csv
    c.to_excel(df_concat, sheets_path, 'g11.11.xlsx')

except Exception as e:
    errors[siconfi_url] = traceback.format_exc()


# geração do arquivo de erro caso ocorra algum
# se a chave do dicionário for url, o erro se refere à tentativa de download da base de dados
# se a chave do dicionário for o nome da figura, o erro se refere à tentativa de estruturar a tabela
if errors:
    with open(os.path.join(errors_path, 'script--g11.11.txt'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(errors, indent=4, ensure_ascii=False))

# remove os arquivos baixados
shutil.rmtree(dbs_path)
