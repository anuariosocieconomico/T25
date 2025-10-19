import traceback
import functions as c
import os
import pandas as pd
import numpy as np
import re


# caminhos
raw_path = c.raw_path
mart_path = c.mart_path
error_path = c.error_path
os.makedirs(mart_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)

# arquivos de origem
pib_path = os.path.join(raw_path, 'pib', 'raw_pib_consolidado.parquet')
deflator_path = os.path.join(raw_path, 'SCN10_DIPIBG10.parquet')

# arquivo de destino
consolidated_path = os.path.join(mart_path, 'pib.parquet')

# carrega as bases
print('Carregando bases...')

try:
    
    # tratamento do PIB
    df_pib = pd.read_parquet(pib_path)[['Município', 'Variável', 'Ano', 'Valor', 'Unidade de Medida']]
    df_pib['Ano'] = pd.to_datetime({'year': df_pib['Ano'].astype(int), 'month': 12, 'day': 31})
    df_pib['Município'] = df_pib['Município'].str.replace(' (SE)', '', regex=False)  # remove sufixo de Sergipe, se existir
    mask = df_pib['Variável'].str.lower().str.contains(
        'microrregião|mesorregião|grande|brasil', regex=True, na=False
    )
    df_pib = df_pib.loc[~mask].copy()  # remove variáveis que contenham microrregião, mesorregião, grande região ou Brasil
    df_pib['Variável'] = df_pib['Variável'].str.replace('a preços correntes', '', regex=False).str.strip()

    # df para join com deflator
    pib = df_pib.loc[df_pib['Unidade de Medida'].str.lower() == 'mil reais'].copy()
    pib.drop(columns=['Unidade de Medida'], inplace=True)
    # pib.rename(columns={'Valor': 'Mil Reais'}, inplace=True)
    max_year = pib['Ano'].dt.year.max()

    # tratamento do deflator
    df_deflator = pd.read_parquet(deflator_path)
    col = [col for col in df_deflator.columns if 'value' in col.lower()]  # identifica a coluna do deflator
    df_deflator.rename(columns={col[0]: 'Taxa', 'YEAR': 'Ano'}, inplace=True)
    df_deflator['Indice'] = 100
    df_deflator = df_deflator.loc[(df_deflator['Ano'] >= 1995) & (df_deflator['Ano'] <= max_year), ['Ano', 'Taxa', 'Indice']].copy()
    df_deflator.reset_index(drop=True, inplace=True)
    
    # calcula o índice do deflator
    for i in range(len(df_deflator) - 1):
        df_deflator.loc[i + 1, 'Indice'] = (df_deflator.loc[i + 1, 'Taxa'] / 100 + 1) * df_deflator.loc[i, 'Indice']

    df_deflator['Deflator'] = df_deflator['Indice'].iloc[-1] / df_deflator['Indice']
    df_deflator['Ano'] = pd.to_datetime({'year': df_deflator['Ano'].astype(str), 'month': 12, 'day': 31})
    df_deflator = df_deflator[['Ano', 'Deflator']]

    # join entre PIB e deflator
    pib_merged = pib.merge(df_deflator, on='Ano', how='left')
    pib_merged['Valor corrigido'] = pib_merged['Valor'] * pib_merged['Deflator']
    pib_merged.drop(columns=['Deflator'], inplace=True)
    pib_merged['Ranking valor'] = pib_merged.groupby(['Variável', 'Ano'])['Valor'].rank(ascending=False, method='min')
    pib_merged.sort_values(by=['Município', 'Variável', 'Ano'], inplace=True)
    pib_merged.reset_index(drop=True, inplace=True)
    pib_merged['Variação nominal'] = pib_merged.groupby(['Município', 'Variável'])['Valor'].pct_change() * 100
    pib_merged['Variação real'] = pib_merged.groupby(['Município', 'Variável'])['Valor corrigido'].pct_change() * 100
    pib_merged['Ranking variação'] = pib_merged.groupby(['Variável', 'Ano'])['Variação nominal'].rank(ascending=False, method='min')

    pib_merged.sort_values(by=['Município', 'Variável', 'Ano'], inplace=True)
    pib_merged.reset_index(drop=True, inplace=True)
    pib_merged['Base nominal'] = pib_merged.groupby(['Município', 'Variável'])['Valor'].transform('first')
    pib_merged['Base real'] = pib_merged.groupby(['Município', 'Variável'])['Valor corrigido'].transform('first')
    pib_merged['Índice nominal'] = (pib_merged['Valor'] / pib_merged['Base nominal']) * 100
    pib_merged['Índice real'] = (pib_merged['Valor corrigido'] / pib_merged['Base real']) * 100
    pib_merged.drop(columns=['Base nominal', 'Base real'], inplace=True)

    df_participacao = df_pib.loc[df_pib['Unidade de Medida'].str.lower() == '%'].copy()
    df_participacao.rename(columns={'Valor': 'Participação'}, inplace=True)
    df_participacao['Variável'] = df_participacao['Variável'].str.lower().str.replace('participação do ', '', regex=False).str.strip()
    df_participacao['Variável'] = df_participacao['Variável'].str.lower().str.replace('participação dos ', '', regex=False).str.strip()
    df_participacao.drop(columns=['Unidade de Medida'], inplace=True)

    df_se = df_participacao.loc[df_participacao['Variável'].str.lower().str.contains('federação')].copy()
    df_se['Variável'] = df_se['Variável'].str.capitalize()
    df_se['Variável'] = df_se['Variável'].str.replace(
        r' no valor adicionado bruto .*| no produto .*| nos impostos, .*', 
        '', 
        regex=True
    )
    df_se['Variável'] = df_se['Variável'].str.replace('social,', 'social', regex=False)
    df_se.rename(columns={'Participação': 'Participação SE'}, inplace=True)

    df_participacao_mun = df_participacao.loc[~df_participacao['Variável'].str.lower().str.contains('federação')].copy()
    df_participacao_mun['Variável'] = df_participacao_mun['Variável'].str.capitalize()
    df_participacao_mun['Variável'] = df_participacao_mun['Variável'].str.replace(
        r' no valor .*', 
        '', 
        regex=True
    )
    df_participacao_mun['Variável'] = df_participacao_mun['Variável'].str.replace('social,', 'social', regex=False)
    df_participacao_mun.rename(columns={'Participação': 'Participação no VAB do município'}, inplace=True)

    df_participacao_final = df_se.merge(df_participacao_mun, how='outer')
    df_participacao_final['Variável'] = df_participacao_final['Variável'].str.replace('interno bruto', 'Interno Bruto', regex=False)

    max_year = pib_merged['Ano'].dt.year.max()
    min_year = pib_merged['Ano'].dt.year.min()

    pib_merged = pib_merged.merge(
        df_participacao_final, 
        how='left', 
        on=['Município', 'Variável', 'Ano']
    )
    principais = ['Produto Interno Bruto', 'Valor adicionado bruto total', 'Impostos, líquidos de subsídios, sobre produtos']
    pib_merged = pib_merged.assign(
        Tipo=np.where(pib_merged['Variável'].isin(principais), 'Principal', 'Componentes')
    )

    cols = list(pib_merged.columns)
    tipo_col = cols.pop(cols.index('Tipo'))
    var_idx = cols.index('Variável')
    cols.insert(var_idx + 1, tipo_col)
    pib_merged = pib_merged[cols]

    pib_merged['Ano'] = pib_merged['Ano'].dt.strftime('%d/%m/%Y')

    # salva o arquivo consolidado e tratado
    pib_merged.to_parquet(consolidated_path, engine='pyarrow', compression='snappy', index=False)
    print(f'Consolidado salvo: {len(pib_merged)} registros totais')
    print(f'Arquivo: {consolidated_path}')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_mart_pib_consolidado.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em mart_pib_consolidado.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)