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

nota_fiscal = csv.reader(open(
    'C:/Users/14343258/PycharmProjects/matrix_2.0/arquivos csv/nota.csv'),
    delimiter=';'
)

pagamento = csv.reader(open(
    'C:/Users/14343258/PycharmProjects/matrix_2.0/arquivos csv/pagamento.csv'),
    delimiter=';'
)

aquisicoes = csv.reader(open(
    'C:/Users/14343258/PycharmProjects/matrix_2.0/arquivos csv/aquisicoes2.csv'),
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

def converte_data(data_original):
    data = datetime.strptime(data_original, '%d/%m/%Y')
    saida = f'{data.year}-{data.month}-{data.day}'
    return saida

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

def extrai_dados_nota_fiscal(planilha):
    for linha in planilha:
        if (
                linha[20] == ""
                or linha[21] == ""
                or linha[20] == "Nº DANFE"
                or linha[21] == "Data DANFE"
        ):
            continue
        else:
            print(f'{linha[20]} - {linha[15]} - {linha[21]} - {linha[22]} - {linha[23]}')

            data_danfe = converte_data(linha[21])
            data_atesto = ""
            data_entrada = ""
            if linha[22] !="":
                data_atesto = converte_data(linha[22])
            if linha[23] != "":
                data_entrada = converte_data(linha[23])

            print(f'{linha[20]}, "{linha[15]}", {data_danfe}, {data_atesto}, {data_entrada}')

            con = mysql.connector.connect(
                host='localhost', database='matrix', user='root',
                password='Mysqlhybris1#'
            )
            db_info = con.get_server_info()
            print(f'Conectado ao servidor Mysql versão {db_info}')
            cursor = con.cursor()

            if data_atesto =="" and data_entrada == "":
                comando = f'''insert into nota_fiscal 
                (danfe, cnpj_forncecedor, data_da_danfe)
                values 
                ({linha[20]}, "{linha[15]}", "{data_danfe}");'''
            elif data_atesto == "":
                comando = f'''insert into nota_fiscal 
                (danfe, cnpj_forncecedor, data_da_danfe, data_recebimento_sei) 
                values 
                ({linha[20]}, "{linha[15]}", "{data_danfe}", "{data_entrada}");'''
            elif data_entrada == "":
                comando = f'''insert into nota_fiscal 
                (danfe, cnpj_forncecedor, data_da_danfe, data_do_atesto)
                values
                ({linha[20]}, "{linha[15]}", "{data_danfe}", "{data_atesto}");'''
            else:
                comando = f'''insert into nota_fiscal 
                (danfe, cnpj_forncecedor, data_da_danfe, data_do_atesto, data_recebimento_sei) 
                values 
                ({linha[20]}, "{linha[15]}", "{data_danfe}", "{data_atesto}", "{data_entrada}");'''


            if con.is_connected():
                try:
                    cursor.execute(comando)
                    con.commit()
                    print('Conexão encerrada.')
                except Error as erro:
                    print(f'Erro localizado - {erro}')
                finally:
                    if con.is_connected():
                        cursor.close()
                        con.close()
                        print('Conexão encerrada.')


def extrai_dados_de_pagamento(planilha):
    for linha in planilha:
        if (
                linha[20] == ""
                or linha[24] == ""
                or linha[25] == ""
                or linha[20] == "Nº DANFE"
                or linha[26] == "A/A"
        ):
            continue
        else:
            print(f'{linha[24]} - {linha[25]} - {linha[20]} - {linha[26]}')

            data_ted = converte_data(linha[25])
            print(f'{linha[24]}, {data_ted}, {linha[20]}, {linha[26]}')

            con = mysql.connector.connect(
                host='localhost', database='matrix', user='root',
                password='Mysqlhybris1#'
            )
            db_info = con.get_server_info()
            print(f'Conectado ao servidor Mysql versão {db_info}')
            cursor = con.cursor()

            comando = f'''insert into pagamento 
            (numero_ted, data_de_pagamento_ted, danfe, codigo_bimestre) 
            values
            ({linha[24]}, "{data_ted}", {linha[20]}, {linha[26]});'''

            if con.is_connected():
                try:
                    cursor.execute(comando)
                    con.commit()
                    print('Conexão encerrada.')
                except Error as erro:
                    print(f'Erro localizado - {erro}')
                finally:
                    if con.is_connected():
                        cursor.close()
                        con.close()
                        print('Conexão encerrada.')

def extrai_dados_aquisicao(planilha):
    for linha in planilha:
        if (
                linha[3] == ""
                or linha[7] == ""
                or linha[13] == ""
                or linha[20] == ""
        ):
            continue
        else:
            print(f'{linha[3]} - {linha[7]} - {linha[13]} - {linha[17]} - {linha[16]} - {linha[19]} - {linha[15]} - {linha[20]}')

            data_fechamento = converte_data(linha[19])
            preco = str(linha[17]).replace(',', '.')

            print(f'{linha[3]}, {linha[7]}, {linha[13]}, {preco}, {linha[16]}, "{data_fechamento}", {linha[15]}, {linha[20]}')

            con = mysql.connector.connect(
                host='localhost', database='matrix', user='root',
                password='Mysqlhybris1#'
            )
            db_info = con.get_server_info()
            print(f'Conectado ao servidor Mysql versão {db_info}')
            cursor = con.cursor()

            comando = f'''insert into aquisicao
            (processo_sei, codigo_produto, quantidade_solicitada, preco_unitario, quantidade_adquirida, data_de_fechamento, cnpj_forncecedor, danfe)
            values
            ("{linha[3]}", {linha[7]}, {linha[13]}, "{float(preco)}", {linha[16]}, "{data_fechamento}", "{linha[15]}", {linha[20]});'''

            if con.is_connected():
                try:
                    cursor.execute(comando)
                    con.commit()
                    print('Conexão encerrada.')
                except Error as erro:
                    print(f'Erro localizado - {erro}')
                finally:
                    if con.is_connected():
                        cursor.close()
                        con.close()
                        print('Conexão encerrada.')


#extrai_dados_processo_cotacao(controle)
#extrai_dados_fornecedor(fornecedores)
#extrai_dados_produto(produto)
#extrai_dados_nota_fiscal(nota_fiscal)
#extrai_dados_de_pagamento(pagamento)
extrai_dados_aquisicao(aquisicoes)