# -*- coding: utf-8 -*-
"""
Created on Sun Apr  17 15:43:20 2022

@author: romano
"""

import zipfile
import os
import pandas as pd
import cx_Oracle


def extract_files(path):
    path_list = path.split('/')
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(f"{path_list[0]}/extraction")

def delete_not_necessary(path):
    for file in os.listdir(path):
        try:
            os.remove(f'{path}/extraction/{file}')
        except PermissionError:
            print(file)
        except FileNotFoundError:
            print(file)


def get_data(path):
    dict_dataframes = {}
    #iterar primeira lista de pastas (hora, paradas e plantas)
    for directory in os.listdir(path):
        dict_dataframes[directory] = pd.DataFrame()
        #pular diretórios que não forem o de plantas
        if directory != 'Plantas':
            continue
        #iterar arquivos excel
        for excel_file in os.listdir(f'{path}/{directory}'):
            excel = pd.ExcelFile(f'{path}/{directory}/{excel_file}')
            df = pd.read_excel(f'{path}/{directory}/{excel_file}')
            df.set_index('Date', inplace=True, drop=True)
            #iterar tabelas do excel
            for sheet in excel.sheet_names[1:]:
                df_sheet = pd.read_excel(f'{path}/{directory}/{excel_file}', sheet_name=sheet)
                df_sheet.set_index('Date', inplace=True, drop=True)
                print(excel_file)
                try:
                    df = pd.concat([df, df_sheet], axis=1)
                except:
                    df = df.join(df_sheet, how='outer')

            dict_dataframes[directory] = pd.concat([dict_dataframes[directory], df])

    return dict_dataframes

def sort_index(dict_frames):
    for key, dataframe in dict_frames.items():
        dataframe.sort_index(inplace=True)


class oracle_fiap:
    
    def __init__(self, user, password):
        self.dsn = cx_Oracle.makedsn("oracle.fiap.com.br", 1521, service_name="orcl.fiap.com.br")
        
        self.con= cx_Oracle.connect(
                user=user,
                password=password,
                dsn=self.dsn
                )
        
        self.cur = self.con.cursor()
        
    def get_data(self):
        
        self.cur.execute("select * from pf0110.v_dados_cli_maq_jkcontrol")
        
        
        ### pesquisar comando para extrair nome das colunas direto do oracle
        columns =  [
         'CD_PROPRIETARIO', 'NM_PROPRIETARIO', 'DT_NASCIMENTO', 'NM_SEXO_BIOLOGICO',
         'CD_EMPRESA', 'NM_RAZAO_SOCIAL', 'NR_CNPJ', 'NM_FANTASIA', 'SG_ESTADO', 'NM_ESTADO',
         'NM_CIDADE', 'NM_BAIRRO', 'CD_MAQUINA', 'NM_MAQUINA',
         'DS_MAQUINA', 'NM_FABRICANTE', 
         'NM_PAIS_ORIGEM', 'NR_SERIE_MAQUINA', 'NR_ANO_FABRICACAO', 
         'DS_VOLTAGEM']
        
        df = pd.DataFrame(self.cur.fetchall(), columns=columns)
        
        maquina = df.loc[:, 'CD_MAQUINA':]
        maquina['T_EMPRESA_CD_EMPRESA'] = df['CD_EMPRESA'].values
        
        
        data = {
            'T_PROPRIETARIO' : df.loc[:, :'NM_SEXO_BIOLOGICO'],
            'T_EMPRESA' : df.loc[:, 'CD_EMPRESA':'NM_BAIRRO'],
            'T_MAQUINA' : maquina
            }
    
        return data
    
    def insert_data(self, data):
        
        def make_queries(columns):
            query = '('
            for column in columns:
                if column != columns[-1]:
                    query = f'{query}:{column}, '
                else:
                    query = f'{query}:{column}'
            
            query = f'{query})'
            
            return query
        
        for tabela, df in data.items():
            uniques = df.drop_duplicates()
            query = f'INSERT INTO {tabela} VALUES {make_queries(uniques.columns)}'
            exec_list = []
            for index, row in uniques.iterrows():
                query_content = []
                for element in row:
                    query_content.append(element)
                
                print(query_content)
                exec_list.append(query_content)
            
            print('agora##')
            try:
                self.cur.executemany(query, exec_list)
            except cx_Oracle.IntegrityError as e:
                error_obj, = e.args
                if error_obj.code == 1:
                    print(error_obj.message)
                    continue
                else:
                    raise(error_obj.message)

            self.con.commit()
            
        return None


def transform_plantas(plantas):
    new_df = pd.DataFrame(columns=['DATA', 'CD_MAQUINA'])
    for column in plantas.columns:
        notnull = plantas[column][plantas[column].notna()]
        new_df2 = pd.DataFrame({'DATA':notnull.index,
                                'CD_MAQUINA' : column})
        new_df = pd.concat([new_df, new_df2])
        
        new_df.sort_values(by='DATA', inplace=True)
        #new_df.reset_index(inplace=True)
        new_df['CD_MAQUINA'] = new_df['CD_MAQUINA'].apply(lambda x: x.lower())
    return new_df