import traceback
import functions as c
import os
import pandas as pd


# caminhos
raw_path = c.raw_path
mart_path = c.mart_path
error_path = c.error_path
file_name = 'raw_comercio_exterior.parquet'
os.makedirs(mart_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)

try:
    df = pd.read_parquet(os.path.join(raw_path, file_name))
    metrics = [col for col in df.columns if col.startswith('metric')]
    df[metrics] = df[metrics].apply(pd.to_numeric, errors='coerce')
    df.drop(columns=['coNcmSecrom', 'headingCode', 'chapterCode'], inplace=True)
    df.rename(columns={
        'noMunMinsgUf': 'Município', 'year': 'Ano', 'monthNumber': 'Mês', 'country': 'País',
        'metricFOB': 'Valor US$ FOB', 'metricKG': 'Quilograma Líquido'
    }, inplace=True)
    df['Data'] = pd.to_datetime(df['Ano'] + '-' + df['Mês'] + '-01', format='%Y-%m-%d')
    df['Município'] = df['Município'].str.replace(' - SE', '').str.strip()

    # tabela anual
    anual = df.groupby(['Município', 'Ano', 'Fluxo'], as_index=False).agg({
        'Valor US$ FOB': 'sum'
    })
    anual = anual.pivot_table(index=['Município', 'Ano'], columns='Fluxo', values='Valor US$ FOB', fill_value=0).reset_index()
    anual['Saldo'] = anual.get('Exportação', 0) - anual.get('Importação', 0)
    anual['Ranking Importação'] = anual.groupby('Ano')['Importação'].rank(method='min', ascending=False)
    anual['Ranking Exportação'] = anual.groupby('Ano')['Exportação'].rank(method='min', ascending=False)
    anual['Ranking Saldo'] = anual.groupby('Ano')['Saldo'].rank(method='min', ascending=False)
    anual['Ano'] = '31/12/' + anual['Ano'].astype(str)
    anual.sort_values(['Ano', 'Ranking Saldo'], inplace=True)

    # tabela mensal
    mensal = df.groupby(['Município', 'Data', 'Fluxo'], as_index=False).agg({
        'Valor US$ FOB': 'sum'
    })
    mensal = mensal.pivot_table(index=['Município', 'Data'], columns='Fluxo', values='Valor US$ FOB', fill_value=0).reset_index()
    mensal['Saldo'] = mensal.get('Exportação', 0) - mensal.get('Importação', 0)
    mensal.rename(columns={'Data': 'Mês'}, inplace=True)
    mensal['Ranking Importação'] = mensal.groupby(['Mês'])['Importação'].rank(method='min', ascending=False)
    mensal['Ranking Exportação'] = mensal.groupby(['Mês'])['Exportação'].rank(method='min', ascending=False)
    mensal['Ranking Saldo'] = mensal.groupby(['Mês'])['Saldo'].rank(method='min', ascending=False)
    mensal.sort_values(['Mês', 'Ranking Saldo'], inplace=True)
    mensal['Mês'] = mensal['Mês'].dt.strftime('%d/%m/%Y')

    # salva
    anual.to_parquet(os.path.join(mart_path, 'comercio_exterior_anual.parquet'), index=False)
    mensal.to_parquet(os.path.join(mart_path, 'comercio_exterior_mensal.parquet'), index=False)    

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_mart_comercio_exterior.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em mart_comercio_exterior.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)