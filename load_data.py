import os
import zipfile
import requests
import shutil
import pandas as pd

class load_data:

    def __init__(self, caminho_dados):

        self.chave_api = os.getenv("KEY_FINTZ")
        self.caminho_dados = caminho_dados

        self.headers = {'accept': 'application/json',
                        'X-API-Key': self.chave_api}
        
        os.chdir(self.caminho_dados)

    def get_cad_fi(self):

        self.url_base = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/'

        download = requests.get(self.url_base + 'cad_fi.csv')
        open('cad_fi.csv','wb').write(download.content)
        shutil.move('cad_fi.csv', './cad_fi/cad_fi.csv')

        cad_fi = pd.read_csv('./cad_fi/cad_fi.csv', encoding = 'Windows-1252', sep = ';', low_memory = False)
        cad_fi = cad_fi[(cad_fi['TP_FUNDO'] == 'FI') & (cad_fi['SIT'] == 'EM FUNCIONAMENTO NORMAL') & (cad_fi['CLASSE'] != 'FMP-FGTS')]
        cad_fi = cad_fi.replace(pd.NA, '--')

        cad_fi.to_parquet('cad_fi.parquet', index = False)


    def get_inf_diario_fi(self, initial_date = pd.Timestamp.today() - pd.DateOffset(days=2), final_date = pd.Timestamp.today()):

        url_base = 'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'    

        range_date = pd.date_range(initial_date, final_date)
        series_date = pd.Series(range(len(range_date)), index=range_date)
        resample_date = series_date.resample('ME').first()
        date_list = resample_date.index.strftime('%Y%m')

        for date in date_list:
            
            try:
                print(f'Baixando: inf_diario_fi_{date}.zip...')

                download = requests.get(url_base + f'inf_diario_fi_{date}.zip')
                open(f'inf_diario_fi_{date}.zip','wb').write(download.content)
                zip = zipfile.ZipFile(f'inf_diario_fi_{date}.zip')
                zip.extractall()
                    
                zip.close()
                os.remove(f'inf_diario_fi_{date}.zip')
                shutil.move(f'inf_diario_fi_{date}.csv', f'./inf_diario_fi/inf_diario_fi_{date}.csv')

                print(f'Download concluído: inf_diario_fi_{date}.csv')

            except: os.remove(f'inf_diario_fi_{date}.zip')

    def get_documentos(self, initial_date = pd.Timestamp.today() - pd.DateOffset(days=2), final_date = pd.Timestamp.today()):

        url_base = 'https://dados.cvm.gov.br/dataset/fi-doc-eventual'

        range_date = pd.date_range(initial_date, final_date)
        series_date = pd.Series(range(len(range_date)), index=range_date)
        resample_date = series_date.resample('YE').first()
        date_list = resample_date.index.strftime('%Y')

        for year in date_list:

            print(f'Baixando: eventual_fi_{year}.csv...')

            download = requests.get(url_base + f'eventual_fi_{year}.csv')
            open(f'eventual_fi_{year}.csv','wb').write(download.content)

            shutil.move(f'eventual_fi_{year}.csv', f'./eventual_fi/eventual_fi_{year}.csv')

            print(f'Download concluído: eventual_fi_{year}.csv...')

    def get_ibov(self):

        response = requests.get('https://api.fintz.com.br/indices/historico?indice=IBOV&dataInicio=1994-06-30',
                                headers=self.headers)
        
        df = pd.DataFrame(response.json())
        df = df.sort_values('data', ascending=True)
        df.columns = ['indice', 'data', 'fechamento']
        df = df.drop('indice', axis = 1)
        df.to_parquet('ibov.parquet', index = False)

        print('Download concluído: ibov.parquet')

    def get_cdi(self):

        response = requests.get('https://api.fintz.com.br/taxas/historico?codigo=12&dataInicio=1994-06-30&ordem=ASC',
                                headers=self.headers)
    
        cdi = pd.DataFrame(response.json())
        cdi = cdi.drop(["dataFim", 'nome'], axis = 1)
        cdi.columns = ['data', 'retorno']
        cdi['retorno'] = cdi['retorno']/100 
        cdi.to_parquet('cdi.parquet', index = False)

        print('Download concluído: cdi.parquet')

if __name__ == '__main__':

    client = load_data(caminho_dados= r'C:\Users\moura\dev\github\Fundos de investimento - Dash\src\data')
    client.get_cad_fi()
    client.get_inf_diario_fi()
    client.get_documentos()