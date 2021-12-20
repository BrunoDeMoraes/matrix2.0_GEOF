import csv
import mysql.connector
from mysql.connector import Error
from datetime import datetime

controle = csv.reader(
    open('C:/Users/14343258/Desktop/Projeto Matrix 2.0/base_2020.csv'),
    delimiter=';'
)
fornecedores = csv.reader(open(
    'C:/Users/14343258/Desktop/Projeto Matrix 2.0/fornecedores.csv'),
    delimiter=';'
)

produto = csv.reader(open(
    'C:/Users/14343258/Desktop/Projeto Matrix 2.0/produtos_escape2.csv'),
    delimiter=';'
)

def conexao(comando_sql):
    con = mysql.connector.connect(
        host='localhost', database='matrix', user='root',
        password='Mysqlhybris1#'
    )
    db_info = con.get_server_info()
    print(f'Conectado ao servidor Mysql versão {db_info}')
    cursor = con.cursor()
    if con.is_connected():
        cursor.execute(comando_sql)
        con.commit()
        cursor.close()
        con.close()
        print('Conexão encerrada.')

def extrai_dados_processo_cotacao(planilha):
    for linha in planilha:
        if (linha[0] == ""
        or linha[1] == ""
        or linha[2] == ""
        or linha[2] == 'Empenhado'
        or linha[2] == 'Recebimento'
        or linha[3] == ""
        or linha[5] == ""
        ):
            continue
        else:
            print(linha[2])
            data = datetime.strptime(linha[2], '%d/%m/%Y')
            entrada = f'{data.year}-{data.month}-{data.day}'
            print(entrada)
            con = mysql.connector.connect(
                host='localhost', database='matrix', user='root',
                password='Mysqlhybris1#'
            )
            db_info = con.get_server_info()
            print(f'Conectado ao servidor Mysql versão {db_info}')
            cursor = con.cursor()

            if con.is_connected():
                comando = f'''insert into processo_cotacao (
                processo_sei, data_de_entrada, projeto_basico, nucleo_origem, numero_cotacao, responsavel
                ) 
                values 
                ("{linha[3]}", "{entrada}", "{linha[0]}", "{linha[1]}", "{linha[5]}", "{linha[4]}");'''

                try:
                    cursor.execute(comando)
                    con.commit()
                    print('Conexão encerrada.')
                except Error as erro:
                    print(f'Erro localizado - {erro} - processo {linha[3]} já inserido.')
                finally:
                    if con.is_connected():
                        cursor.close()
                        con.close()
                        print('Conexão encerrada.')

def extrai_dados_fornecedor(planilha):
    for linha in planilha:
        if (
                linha[0] == ""
                or linha[7] == ""
                or linha[0] in (
                '*CANCELADO',
                '*OBSERVAÇÃO',
                '*SUSPENSO',
                '*URGENTE')
        ):
            continue
        else:
            comando = f'''insert into fornecedor 
            (cnpj_forncecedor, nome_fantasia, razao_social, codigo_ses)
             values 
             ("{linha[7]}", "{linha[13]}", "{linha[1]}", "{linha[12]}");'''
            conexao(comando)

def extrai_dados_produto(planilha):
    for linha in planilha:
        if (
                linha[0] == "Código"
                or linha[0] == ""
                or not linha[0].isnumeric()
        ):
            continue
        else:
            comando = f'''insert into produto 
            (codigo_produto, descricao, unidade)
             values 
             ('{linha[0]}', '{linha[1]}', '{linha[3]}');'''
            conexao(comando)

extrai_dados_processo_cotacao(controle)
#extrai_dados_fornecedor(fornecedores)
#extrai_dados_produto(produto)