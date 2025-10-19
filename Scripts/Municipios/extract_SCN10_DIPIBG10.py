import traceback
import pandas as pd
import requests
from datetime import datetime, date
import json
import functions as c
import os
import ipeadatapy

raw_path = c.raw_path
error_path = c.error_path
os.makedirs(raw_path, exist_ok=True)
os.makedirs(error_path, exist_ok=True)
ipca_filename = 'SCN10_DIPIBG10.parquet'
session = c.create_session_with_retries()

try:
    data = ipeadatapy.timeseries('SCN10_DIPIBG10')
    data.to_parquet(os.path.join(raw_path, ipca_filename), index=True)
    print('SCN10_DIPIBG10 extraído com sucesso!')

except:
    error = traceback.format_exc()
    with open(os.path.join(error_path, 'log_extract_SCN10_DIPIBG10.txt'), 'w', encoding='utf-8') as f:
        f.write(f'Erro em extract_SCN10_DIPIBG10.py em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}\n')
        f.write(error)
    print('Erro ao extrair o SCN10_DIPIBG10. Verifique o log em "Doc/Municipios/log_extract_SCN10_DIPIBG10.txt".')