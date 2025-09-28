"""
SCRIPT DE CONSOLIDAÇÃO - PAM TEMPORÁRIA + PAM PERMANENTE

Consolida os arquivos consolidados de PAM temporária e permanente em um único arquivo.
Salva o resultado na pasta Mart.
"""

import functions as c
import os
import pandas as pd


# caminhos
raw_path = c.raw_path
mart_path = c.mart_path
os.makedirs(mart_path, exist_ok=True)

# arquivos de origem
temporaria_path = os.path.join(raw_path, 'producao_agricola_municipal_temporaria', 'raw_producao_agricola_municipal_temporaria_consolidado.parquet')
permanente_path = os.path.join(raw_path, 'producao_agricola_municipal_permanente', 'raw_producao_agricola_municipal_permanente_consolidado.parquet')

# arquivo de destino
consolidated_path = os.path.join(mart_path, 'producao_agricola_municipal_consolidado.parquet')

# carrega as bases
print('Carregando bases...')
data_frames = []

if os.path.exists(temporaria_path):
    df_temp = pd.read_parquet(temporaria_path)
    df_temp['Tipo_Lavoura'] = 'Temporária'
    data_frames.append(df_temp)
    print(f'PAM Temporária: {len(df_temp)} registros')
else:
    print('ERRO: Base PAM temporária não encontrada')

if os.path.exists(permanente_path):
    df_perm = pd.read_parquet(permanente_path)
    df_perm['Tipo_Lavoura'] = 'Permanente'
    # renomeia a coluna para padronizar
    if 'Produto das lavouras permanentes' in df_perm.columns:
        df_perm.rename(columns={'Produto das lavouras permanentes': 'Produto'}, inplace=True)
    data_frames.append(df_perm)
    print(f'PAM Permanente: {len(df_perm)} registros')
else:
    print('ERRO: Base PAM permanente não encontrada')

# consolida as bases
if data_frames:
    print('Consolidando...')
    df_final = pd.concat(data_frames, ignore_index=True)
    
    # salva o arquivo consolidado
    df_final.to_parquet(consolidated_path, engine='pyarrow', compression='snappy', index=False)
    print(f'Consolidado salvo: {len(df_final)} registros totais')
    print(f'Arquivo: {consolidated_path}')
else:
    print('ERRO: Nenhuma base encontrada para consolidar')

print('Concluído!')