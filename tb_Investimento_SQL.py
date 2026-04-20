import pyodbc as sql
import os
import pandas as pd
from gerar_conexao_sql import gerar_conexao

caminho_investimento = r"C:\Users\jacks\OneDrive\Documentos\Banco J.Morgan\investimentos.csv"
try:
    if os.path.exists(caminho_investimento):
        arquivo_investimentos = pd.read_csv(caminho_investimento)
        print('Dados Carregados com Sucesso...')
except Exception as e:
    print('Falha Ao Carregar Dados...', e)

if __name__ == "__main__":
    conexao = gerar_conexao()

    dados_investimentos = []

for linha in arquivo_investimentos.itertuples(index=False):
    dados_investimentos.append((
        linha.Nome_Produto,
        linha.Taxa_Rendimento,
        linha.Prazo_Minimo
        ))
    
def tabela_investimentos():
    cursor = conexao.cursor()

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Tb_Investimentos' )        
        CREATE TABLE Tb_Investimentos(
            ID_Investimentos int identity(1,1) primary key,
            Nome_Investimento varchar(100) not null,
            Taxa_Rendimento varchar(100) not null,
            Prazo_Minimo varchar(100) not null
        )
        ''')
    
    for investimento in dados_investimentos:
        cursor.execute('''
            INSERT INTO Tb_Investimentos (Nome_Investimento,Taxa_Rendimento,Prazo_Minimo) values(?,?,?)''',
            investimento)
        
    return dados_investimentos

if __name__ == '__main__':
    tabela_investimentos()
    conexao.commit()
    print(F'Tabela Criada e {len(dados_investimentos)} Dados Inseridos Com Sucesso...')