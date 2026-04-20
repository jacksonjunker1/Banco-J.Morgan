import pandas as pd
import pyodbc as sql
import os
from gerar_conexao_sql import gerar_conexao


if __name__ == "__main__":
    conexao = gerar_conexao()
    dados_Tipo_Transacao = [
        ('PIX Enviado', 'Transferência instantânea de saída'),
        ('PIX Recebido', 'Transferência instantânea de entrada'),
        ('Depósito em Dinheiro', 'Entrada via caixa eletrônico ou boca do caixa'),
        ('Pagamento ', 'Saída para quitação de títulos'),
        ('Saque ATM', 'Retirada de espécie em terminal automático'),
        ('Transferência TED', 'Transferência bancária tradicional'),
        ('Investimento ', 'Saída da conta corrente para aplicação financeira.')
        ]

def tabela_Tipo_Transacao():
    cursor = conexao.cursor()

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='Tb_Tipo_Transacao')
        CREATE TABLE Tb_Tipo_Transacao(
            ID_Tipo_Transacao int identity(1,1) primary key,
            Nome_Tipo char(20) not null,
            Descricao varchar(100) not null
        )
        ''')
    
    query = 'INSERT INTO Tb_Tipo_Transacao (Nome_Tipo,Descricao) values(?, ?)'
    cursor.executemany(query,dados_Tipo_Transacao)


if __name__ == "__main__":
    try:
        tabela_Tipo_Transacao()
        conexao.commit()
        print(f"Tabela Criada e {len(dados_Tipo_Transacao)} Dados Inseridos com Sucesso...")
    except Exception as e:
        conexao.rollback()
        print(f'Erro ao Cadastrar Dados {e} ')    
    finally:
        conexao.close()
        print('Conexao encerrada')


