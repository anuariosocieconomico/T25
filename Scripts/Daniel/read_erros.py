import json

with open(r'C:\Users\Daniel\Documents\GitHub\T25\Doc\relatorios_de_erros\script--t17.2.txt', encoding='utf-8') as f:
    erros = json.load(f)

    for k,v in erros.items():
        print(k,v)