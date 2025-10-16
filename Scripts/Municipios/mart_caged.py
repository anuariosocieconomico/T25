import traceback
import functions as c
import os
import pandas as pd


# caminhos
raw_path = c.raw_path
mart_path = c.mart_path
error_path = c.error_path
file_name = 'raw_caged.parquet'
os.makedirs(mart_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)

try:
    df = pd.read_parquet(os.path.join(raw_path, file_name)).query("UF == 'SE'")
    df.reset_index(drop=True, inplace=True)
    df_melted = df.melt(id_vars=['UF', 'COD', 'Município'], var_name='Variável', value_name='Valor')
    
    # formato da data: "Admissões - Janeiro/2020"
    df_melted['Período'] = df_melted['Variável'].str.split(' - ').str[-1]  # extrai "Janeiro/2020"
    df_melted['Ano'] = df_melted['Período'].str.split('/').str[-1]  # extrai "2020"
    df_melted['Mês'] = df_melted['Período'].str.split('/').str[0]  # extrai "Janeiro"
    df_melted['Mês'] = df_melted['Mês'].str.lower().map({
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4, 'maio': 5, 'junho': 6,
        'julho': 7, 'agosto': 8, 'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12,
        'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
        'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12
        # mapeia abreviações também
    })
    df_melted.dropna(subset=['Mês', 'Ano'], inplace=True)  # remove linhas onde mês ou ano não foram identificados, que são colunas agregadas do ano
    df_melted['Data'] = pd.to_datetime(
        {'year': df_melted['Ano'].astype(int), 'month': df_melted['Mês'].astype(int), 'day': 1}
    )
    df_melted['Variável'] = df_melted['Variável'].str.split(' - ').str[0]  # extrai "Admissões"
    df_melted = df_melted[['Município', 'Data', 'Variável', 'Valor']]
    df_melted.reset_index(drop=True, inplace=True)

    df_pivoted = df_melted.pivot(index=['Município', 'Data'], columns='Variável', values='Valor').reset_index()
    df_pivoted.columns.name = None  # remove o nome da coluna
    col = [col for col in df_pivoted.columns if 'variação' in col.lower()]  # identifica a coluna de variação
    df_pivoted.rename(columns={col[0]: 'Variação mensal'}, inplace=True)  # renomeia a coluna de variação, se existir
    df_pivoted = df_pivoted[['Município', 'Data', 'Estoque', 'Admissões', 'Desligamentos', 'Saldos', 'Variação mensal']]  # reordena as colunas
    # calcula a variação em 12 meses, considerando o mês atual e o mesmo mês do ano anterior
    df_pivoted['Variação em 12 meses'] = df_pivoted.groupby('Município')['Estoque'].pct_change(periods=12) * 100
    # calcula o ranking
    df_pivoted['Ranking Estoque'] = df_pivoted.groupby('Data')['Estoque'].rank(ascending=False, method='min')
    df_pivoted['Ranking Admissões'] = df_pivoted.groupby('Data')['Admissões'].rank(ascending=False, method='min')
    df_pivoted['Ranking Desligamentos'] = df_pivoted.groupby('Data')['Desligamentos'].rank(ascending=False, method='min')
    df_pivoted['Ranking Saldos'] = df_pivoted.groupby('Data')['Saldos'].rank(ascending=False, method='min')
    df_pivoted['Ranking Variação mensal'] = df_pivoted.groupby('Data')['Variação mensal'].rank(ascending=False, method='min')
    df_pivoted['Ranking Variação em 12 meses'] = df_pivoted.groupby('Data')['Variação em 12 meses'].rank(ascending=False, method='min')
    df_pivoted.rename(columns={'Data': 'Mês'}, inplace=True)  # renomeia a coluna de data para mês
    df_pivoted['Mês'] = df_pivoted['Mês'].dt.strftime('%d/%m/%Y')  # formata a coluna de mês para "YYYY-MM"

    # salva o arquivo parquet
    file_path = os.path.join(mart_path, 'caged.parquet')
    df_pivoted.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
    print(f'Arquivo caged salvo em: {file_path}')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_mart_caged.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em mart_caged.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)