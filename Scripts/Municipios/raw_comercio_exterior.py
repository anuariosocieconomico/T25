import datetime
import traceback
import functions as c
import os
import pandas as pd
import requests


# obtém o caminho desse arquivo de comandos para armazenar diretamente no diretório raw
raw_path = c.raw_path
error_path = c.error_path
os.makedirs(raw_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)
db_name = 'comercio_exterior'  # nome base da base de dados
file_name = 'raw_' + db_name + '.parquet'
file_path = os.path.join(raw_path, file_name)


# ************************
# DOWNLOAD DA BASE DE DADOS
# ************************

try:
    # Get current month in YYYY-MM format
    mes_final = datetime.datetime.now().strftime("%Y-%m")

    url = "https://api-comexstat.mdic.gov.br/cities"
    
    # Create session with retries and timeout
    session = c.create_session_with_retries()

    def get_data(flow_type, mes_final):
        """
        Function to get trade data from the API
        
        Args:
            flow_type (str): Type of flow (e.g., "import", "export")
            mes_final (str): End month in YYYY-MM format
        
        Returns:
            pandas.DataFrame: DataFrame containing the API response data
        """
        
        payload = {
            "flow": flow_type,
            "monthDetail": True,
            "period": {
                "from": "2019-01",
                "to": mes_final
            },
            "filters": [
                {
                    "filter": "state",
                    "values": [31]
                }
            ],
            "details": [
                "country",
                "city", 
                "section",
                "chapter",
                "heading"
            ],
            "metrics": [
                "metricFOB",
                "metricKG"
            ]
        }
        
        # Make POST request using session with retries and timeout
        headers = {'Content-Type': 'application/json'}
        response = session.post(url, json=payload, headers=headers, timeout=session.request_timeout, verify=False)
        
        # Check if request was successful
        response.raise_for_status()
        
        # Parse JSON response
        response_data = response.json()
        
        # Convert to DataFrame
        df = pd.json_normalize(response_data["data"]["list"])
        
        return df

    dfs = []
    for flow_type, fluxo in [("import", "Importação"), ("export", "Exportação")]:
        df = get_data(flow_type, mes_final)
        df['Fluxo'] = fluxo
        dfs.append(df)

    df_concat = pd.concat(dfs, ignore_index=True)
    df_concat.to_parquet(file_path, index=False)
    print(f'Dados de comércio exterior salvos em {file_path}.')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_raw_censo_demografico.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em raw_comercio_exterior.py em {pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao baixar ou processar os dados. Verifique o log em "Doc/Municipios/log_raw_comercio_exterior.txt".')

