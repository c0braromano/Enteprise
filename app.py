# -*- coding: utf-8 -*-
"""
Created on Sun Apr  17 15:47:37 2022

@author: romano
"""

import pandas as pd

from functions.helper import extract_files, delete_not_necessary
from functions.helper import get_data, sort_index, oracle_fiap
from functions.helper import transform_plantas

from datetime import datetime

inicio = datetime.now()

extract_files('data/Dados_Sensores.zip')
delete_not_necessary('data')
data = get_data('data/extraction/Dados_Sensores')
#sort_index(data)
instancia_fiap = oracle_fiap('rm92629', '050396')
info_orcl = instancia_fiap.get_data()
df_plantas = transform_plantas(data['Plantas'])
info_orcl['T_PRODUCAO'] = df_plantas
a = instancia_fiap.insert_data(info_orcl)



print(datetime.now() - inicio)
