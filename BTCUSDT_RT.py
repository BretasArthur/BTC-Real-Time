import sqlite3
import websocket
import json
from datetime import datetime

def consulta(banco_de_dados, texto):
    retorno = None
    with sqlite3.connect(banco_de_dados) as conn:
            cursor = conn.cursor()
            cursor.execute(texto)

            try:
                retorno = cursor.fetchall()
            except:
                retorno = 'Consulta_Sem_Retorno'

            try:
                assert retorno is not None
            except:
                raise Exception('Não foi possível obter retorno da consulta.')

            if (retorno != 'Consulta_Sem_Retorno'
               and 'select' in texto.lower()):
                return retorno
    try:
        assert retorno is not None
    except:
        raise Exception('Não foi possível obter retorno da consulta.')

def real_time_etl(moeda, banco_de_dados):
    """A moeda deve conter o símbolo padrão da Binance (ex: BTCUSDT)"""

    tabela = moeda
    def limpar_tabela():
        consulta(banco_de_dados, f"DROP TABLE IF EXISTS {tabela}")  
        consulta(
            banco_de_dados,
                f"""
                CREATE TABLE {tabela}(
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                time DATE NOT NULL,
                price REAL NOT NULL)
                """
        )
        
    def inserir_dados(message):
        dictm = json.loads(message)
        symbol = dictm['s']
        time = datetime.fromtimestamp((dictm['E']/1000)) #Considerando milisegundos
        price = dictm['p']
        ultimo = consulta(
            banco_de_dados, 
            f"SELECT time FROM {tabela} WHERE id = (SELECT MAX(id) FROM {tabela})"
        )
        if ultimo == [] or ultimo[0][0] != str(time): #Assegurar que irão casos repetidos
            consulta(
                banco_de_dados,
                f"""
                    INSERT INTO {tabela} (symbol, time, price) VALUES ('{symbol}', '{time}', {price})
                """)
            print(f"      |   {symbol}   |   {time}   |   {price}   |")
    
    def on_message(wsapp, message):
        inserir_dados(message=message)
    
    limpar_tabela()
    wsapp = websocket.WebSocketApp(f"wss://stream.binance.com:9443/ws/{moeda.lower()}@trade", on_message=on_message)
    wsapp.run_forever()

if __name__ == '__main__':
    real_time_etl(
        'BTCUSDT',
        banco_de_dados='moedas.db')
