import traceback
import functions as c
import os
import pandas as pd


# caminhos
raw_path = c.raw_path
mart_path = c.mart_path
error_path = c.error_path
file_name = 'raw_frota_veiculos.parquet'
os.makedirs(mart_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)

try:
    # lendo dados
    df = pd.read_parquet(os.path.join(raw_path, file_name))
    df.drop(columns=['UF'], inplace=True)
    df.rename(columns={'MUNICIPIO': 'Município', 'ANO': 'Ano'}, inplace=True)
    # garantir que mês e ano sejam inteiros e construir a data com dia 31 para evitar erro quando o mês não tem zero à esquerda
    df['Ano'] = pd.to_datetime({'year': df['Ano'].astype(int), 'month': df['MES'].astype(int), 'day': 31})
    # renomeando colunas
    cols = [col for col in df.columns if col not in ['Município', 'Ano', 'MES']]
    df = df[['Município', 'Ano'] + cols]

    # pivotando
    df_melted = df.melt(id_vars=['Município', 'Ano'], var_name='Veículo', value_name='Valor')
    df_melted.sort_values(by=['Município', 'Veículo', 'Ano', 'Valor'], ascending=[True, True, True, False], inplace=True)
    df_melted['Ranking'] = df_melted.groupby(['Veículo', 'Ano'])['Valor'].rank(method='min', ascending=False)
    mapping = {
        "TOTAL": "Total de veículos",
        "MOTOCICLETA": "Motocicleta",
        "AUTOMOVEL": "Automóvel",
        "MOTONETA": "Motoneta",
        "CAMINHONETE": "Caminhonete",
        "MICRO-ONIBUS": "Micro-ônibus",
        "CAMINHAO": "Caminhão",
        "CAMIONETA": "Camioneta",
        "REBOQUE": "Reboque",
        "CICLOMOTOR": "Ciclomotor",
        "ONIBUS": "Ônibus",
        "UTILITARIO": "Utilitário",
        "BONDE": "Bonde",
        "CAMINHAO TRATOR": "Caminhão Trator",
        "CHASSI PLATAF": "Chassi Plataforma",
        "QUADRICICLO": "Quadriciclo",
        "SEMI-REBOQUE": "Semi-reboque",
        "SIDE-CAR": "Side-car",
        "OUTROS": "Outros",
        "TRATOR ESTEI": "Trator Esteira",
        "TRATOR RODAS": "Trator Rodas",
        "TRICICLO": "Triciclo",
    }
    df_melted['Veículo'] = df_melted['Veículo'].map(mapping)
    df_melted = df_melted[['Município', 'Veículo', 'Ano', 'Valor', 'Ranking']]

    # salvando dados
    df_melted.to_parquet(os.path.join(mart_path, 'frota_veiculos.parquet'), index=False)

    # Frota atual
    df_frota_atual = df_melted[df_melted['Ano'].dt.year == df_melted['Ano'].dt.year.max()]
    df_frota_atual.drop(columns=['Ano', 'Ranking'], inplace=True)
    df_frota_atual = df_frota_atual.pivot(index='Município', columns='Veículo', values='Valor').reset_index()
    
    rename_map = {
        "Caminhão Trator": "Caminhão trator",
        "Chassi Plataforma": "Chassi plataforma",
        "Trator Esteira": "Trator esteira",
        "Trator Rodas": "Trator rodas"
    }
    df_frota_atual = df_frota_atual.rename(columns=rename_map)

    final_cols = [
        'Município',
        'Total de veículos',
        'Automóvel',
        'Bonde',
        'Caminhão',
        'Caminhão trator',
        'Caminhonete',
        'Camioneta',
        'Chassi plataforma',
        'Ciclomotor',
        'Micro-ônibus',
        'Motocicleta',
        'Motoneta',
        'Ônibus',
        'Quadriciclo',
        'Reboque',
        'Semi-reboque',
        'Side-car',
        'Outros',
        'Trator esteira',
        'Trator rodas',
        'Triciclo',
        'Utilitário'
    ]

    df_frota_atual = df_frota_atual.reindex(columns=final_cols)
    df_frota_atual.to_parquet(os.path.join(mart_path, 'frota_veiculos_atual.parquet'), index=False)

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_mart_frota_veiculos.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em mart_frota_veiculos.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)