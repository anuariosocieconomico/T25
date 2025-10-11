"""
SCRIPT DE CONSOLIDAÇÃO - PAM TEMPORÁRIA + PAM PERMANENTE

Consolida os arquivos consolidados de PAM temporária e permanente em um único arquivo.
Salva o resultado na pasta Mart.
"""

import traceback
import functions as c
import os
import pandas as pd


# caminhos
raw_path = c.raw_path
mart_path = c.mart_path
error_path = c.error_path
os.makedirs(mart_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)

# arquivos de origem
temporaria_path = os.path.join(raw_path, 'producao_agricola_municipal_temporaria', 'raw_producao_agricola_municipal_temporaria_consolidado.parquet')
permanente_path = os.path.join(raw_path, 'producao_agricola_municipal_permanente', 'raw_producao_agricola_municipal_permanente_consolidado.parquet')
ipca_path = os.path.join(raw_path, 'ipca.parquet')

# arquivo de destino
consolidated_path = os.path.join(mart_path, 'producao_agricola_municipal_consolidado.parquet')

# carrega as bases
print('Carregando bases...')
data_frames = []

try:

    df_temp = pd.read_parquet(temporaria_path)
    df_temp['Tipo_Lavoura'] = 'Temporária'
    data_frames.append(df_temp)
    print(f'PAM Temporária: {len(df_temp)} registros')


    df_perm = pd.read_parquet(permanente_path)
    df_perm['Tipo_Lavoura'] = 'Permanente'
    # renomeia a coluna para padronizar
    if 'Produto das lavouras permanentes' in df_perm.columns:
        df_perm.rename(columns={'Produto das lavouras permanentes': 'Produto'}, inplace=True)
    data_frames.append(df_perm)
    print(f'PAM Permanente: {len(df_perm)} registros')


    df_ipca = pd.read_parquet(ipca_path)
    df_ipca_annual = df_ipca.loc[df_ipca['Mes'].dt.month == 12].copy()  # filtra apenas dezembro de cada ano
    df_ipca_annual.rename(columns={'Mes': 'Ano'}, inplace=True)  # renomeia para Ano, para facilitar o merge
    
    print(f'IPCA: {len(df_ipca)} registros')

# consolida as bases

    print('Consolidando...')
    df_concat = pd.concat(data_frames, ignore_index=True).query('not `Variável`.str.contains("percentual do total")', engine='python')  # filtra apenas as variáveis de produção
    df_concat['Município'] = df_concat['Município'].str.replace(' (SE)', '', regex=False)  # remove a sigla do estado
    df_concat['Ano'] = pd.to_datetime('01/12/' + df_concat['Ano'].astype(str), format='%d/%m/%Y', errors='coerce')  # padroniza a data para 1º de dezembro do ano, para facilitar o merge com o IPCA

    # pivotiza as variáveis em colunas para calcular o ranking e inflação
    variables = df_concat['Variável'].unique()
    rows = [col for col in df_concat.columns if col != 'Valor' and col != 'Variável']
    df_pivoted = df_concat.pivot(index=rows, columns='Variável', values='Valor').reset_index()

    # trata a inflação
    df_merged = pd.merge(df_pivoted, df_ipca_annual[['Ano', 'indice_ipca']], how='left', on='Ano', validate='m:1')
    ano_mais_recente = df_merged.loc[df_merged['indice_ipca'].notnull(), 'Ano'].max()  # ano mais recente com índice disponível
    max_ipca = df_merged.loc[df_merged['Ano'] == ano_mais_recente, 'indice_ipca'].max()  # índice do ano mais recente
    df_merged['Inflação'] = max_ipca / df_merged['indice_ipca']  # fator de correção para valores reais
    df_merged['Valor real da produção'] = df_merged['Valor da produção'] * df_merged['Inflação']  # ajusta o valor pela inflação
    df_merged.rename(columns={'Valor da produção': 'Valor nominal da produção'}, inplace=True)  # renomeia a coluna original

    # calcula o ranking dentro do ano
    # desconsidera o valor da produção, adicionando à lista o valor real e nominal
    for col in [v for v in variables if v != 'Valor da produção'] + ['Valor real da produção', 'Valor nominal da produção']:
        df_merged[f'Ranking {col}'] = df_merged.groupby(['Tipo_Lavoura', 'Produto', 'Ano'])[col].rank(method='min', ascending=False)

    # coluna adicional e ranking
    df_merged['Valor médio real unitário'] = df_merged['Valor real da produção'] / df_merged['Quantidade produzida']
    df_merged['Ranking Valor médio real unitário'] = df_merged.groupby(['Tipo_Lavoura', 'Produto', 'Ano'])['Valor médio real unitário'].rank(method='min', ascending=False)

    # ajustes finais
    pivoted_columns = [v for v in variables if v != 'Valor da produção'] + ['Valor nominal da produção', 'Valor real da produção', 'Valor médio real unitário']
    ranked_columns = [f'Ranking {v}' for v in variables if v != 'Valor da produção'] + ['Ranking Valor nominal da produção', 'Ranking Valor real da produção', 'Ranking Valor médio real unitário']
    df_final = df_merged[['Ano', 'Município', 'Tipo_Lavoura', 'Unidade de Medida', 'Produto'] + pivoted_columns + ranked_columns].copy()
    df_final.rename(columns={'Tipo_Lavoura': 'Lavoura'}, inplace=True)  # padroniza o nome da coluna
    max_year = df_final['Ano'].dt.year.max()
    df_final = df_final.query('Ano.dt.year >= @max_year - 4', engine='python')  # filtra a partir de 2004
    df_final['Ano'] = '31/12/' + df_final['Ano'].dt.year.astype(str)  # padroniza a data para o final do ano

    # salva o arquivo consolidado e tratado
    df_final.to_parquet(consolidated_path, engine='pyarrow', compression='snappy', index=False)
    print(f'Consolidado salvo: {len(df_final)} registros totais')
    print(f'Arquivo: {consolidated_path}')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_mart_pam_consolidado.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em mart_pam_consolidado.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)