import csv
from pprint import pprint
import pandas as pd
import re
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from alchemy_structures.connector import MysqlSandBoxConnector
from sqlalchemy.orm import sessionmaker, scoped_session
import unidecode
import datetime
import shutil
import os
import json


class VendorsIngestion:
    __tables = None
    __files = None

    def __init__(self, database: str = 'business_inteligence'):
        """
        Classe responsavel por realizar o upload das bases baixadas da Sistematizze
        :param database: database a ser acessada
        :type database: str
        """
        mysql_user = 'forge'
        mysql_password = 'tEDI2JItzUdhUc6dW2GI'
        mysql_host = '195.35.17.83'
        port = 3306
        connection_string = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{port}/{database}'
        engine = create_engine(
            connection_string,
            echo=False,
            pool_pre_ping=True,
            pool_recycle=180000,  # adjust to be slightly lower than MySQL wait_timeout
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            connect_args={
                "connect_timeout": 10,
                "read_timeout": 30000,  # increase if your queries often exceed 5 minutes
                "write_timeout": 30000,
                "charset": "utf8mb4",
                # Uncomment to stream huge result sets from server:
                # "cursorclass": pymysql.cursors.SSCursor,
            },
        )
        session = scoped_session(sessionmaker(bind=engine, autoflush=False, expire_on_commit=False))
        self.cursor = session()
        self.abs_path = os.path.dirname(os.path.realpath(__file__))
        self.dir_path = self.abs_path + '\\bases\\'
        self.inserted_path = self.dir_path + '\\inserted\\'
        self.timeDate = str(datetime.datetime.now()).split('.')[0]
        self.database = database
        self.init_trash = None
        self.end_thrash = None
        self.file_path = None
        self.key_value = None
        self.ex_code_dict = {'intermodal': 'IMS', 'hospitalar': 'HPR'}
        self.errors = {}

    @staticmethod
    def get_tables(cursor, schema=None):
        print(cursor)
        if not schema:
            tables_statement = cursor.execute(text(
                """SHOW tables"""
            ))
        else:
            tables_statement = cursor.execute(text(
                f"""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                    """
            ))
        tables = [t[0] for t in tables_statement]
        return tables

    @property
    def files(self) -> list[str]:
        """
        Metodo lista os arquivos presentes no diretorio de bases
        :return: lista de arquivos
        :rtype: list[str]
        """
        if self.__files is None:
            files = []
            for root, dirs, files in os.walk(self.dir_path):
                files.extend(files)
                break
            self.__files = files
        return self.__files

    @staticmethod
    def clean_header(raw_header: list[str]) -> tuple[list[str], list]:
        """
        Metodo responsavel por normalizar o header dos arquivos a serem inseridos
        :param raw_header: header do arquivo
        :type raw_header: list[str]
        :return: tupla com o header normalizado e o indices dos elementos do header original
        :rtype: tuple[list[str], list]
        """
        from nltk import word_tokenize
        indexes = []
        new_header = []
        stopwords = ['você', 'e',  'a', 'o', 'i', 'u', 'seu', 'um(a)', 'seu(a)', 'no', 'de', 'em', 'que', 'se' 'da',
                     'do', 'desses', 'dos', 'ou', 'na', 'seu', 'para', 'da', 'gostaria', 'responder', 'somente', 'se',
                     'marcou', 'já', 'por', 'sobre', '?', 'fórum', 'favor', 'especificar', 'indique', 'todos',
                     'aplicam', 'durante', 'os', 'poderiam', 'tecnologia', 'dessas', 'horarios',
                     'gratuito', 'nós', 'desejamos', 'entrar', 'com', 'relação', 'incluindo', 'cuidadosamente',
                     'possa', 'ingredientes', 'estar', 'tem', 'nas', 'selecionae', 'são', 'abaixo', '<', '>', 'br',
                     'as', 'dentre', 'estou', 'ciente', 'seguir', 'protocolos', 'meu', 'indicar',
                     'atua', 'ao', 'responder', 'esse', 'campo', 'podemos', 'tornar', 'experiên', 'suas',
                     'serão', 'acordo', 'nossa', 'seus', 'marque', 'opções', 'aplicaveis',
                     'pertencer', 'perfil', 'newsletters', 'quinzenais', 'revista', 'tissue', 'world', 'receber',
                     'das', 'edicoes', 'digitais', 'híbridos', 'como', '*', 'fi', 'south', 'america', 'para',
                     'indiquem', 'em', 'seguintes', 'está', 'planejamendo', 'principal', 'responsável', 'sbf',
                     'finalizar', 'nos', 'pesquisa', 'uma', 'até', 'linhas', 'eventualmente', 'selecione',
                     'comunicacao', 'comunicação', 'esta', 'opção', 'opcao', 'essas',
                     'desmarque', 'caixa', 'deseja', 'comunicacoes', 'comunicações', 'propriedade', 'rural',
                     'atualizacoes', 'ainda', 'mais', 'personalizada', 'respondendo', 'pergunta', 'participacao',
                     'con', 'los', 'y', 'mantenme', 'ultimos', 'me', 'gustaria', 'recibir', 'mi', 'servicios',
                     'relevantes', 'please', 'up-to-date', 'with', 'the', 'would', 'to', "'", 'relevant', 'from',
                     's', 'about', 'and', '2024', 'entiendo', 'puede', 'la', 'en', 'del', 'utilizada', 'seguridad',
                     'informacion', 'el', 'puede', 'utilizar', 'agregada', 'analizar', 'mejorar', 'associado', 'las',
                     'opciones', 'puedo', 'hacer', 'relacion', 'asociado', 'organizador', 'escaneo', 'consentimiento',
                     'experiencia', 'salida', 'visitante', 'ser', 'marketing', 'sua',
                     'pueda', 'mis', 'fuera', 'espacio', 'economico', 'al', 'europeo', 'proceso', 'tarjetas',
                     'pasar', 'administrar', 'understand', 'that', 'my', 'may', 'be', 'on', 'entry', 'exit',
                     'used', 'in', 'relation', 'event', 'safety', 'security', 'event', 'organiser', 'use', 'this',
                     'information', 'aggregate', 'form', 'analyse', 'improve', 'visitor', 'at', 'is', 'not', 'make',
                     'associated', 'consent', 'choices', 'agree', 'outside', 'of', 'european', 'economic', 'area',
                     'administering', 'process', 'when', 'exhibitors', 'passing', 'sponsors', 'quiero', 'oir',
                     'markets', 'ni', 'actualizaciones', 'carefully', 'want', 'if', 'wish', 'these',
                     'communications', 'get', 'selected', 'hear', 'or', 'receive', 'other',  'services',
                     'you', 'including', 'alimentacion', 'hospitalidad', 'personales', 'expositores',
                    'comunicacao', 'comunicação', 'esta', 'opção', 'opcao', 'essas', 'opcoes'
                     'desmarque', 'caixa','deseja', 'comunicacoes', 'comunicações', 'atualizacoes', 'ainda', 'mais',
                     'personalizada', 'respondendo', 'pergunta', 'participacao', 'dessa', 'sao', 'um', 'voce', 'todas']
        # entende, informações, usadas
        for h in raw_header:
            # print(h)
            h = str(h).split('<')[0]
            h = h.replace(' _ ', '_').replace('_', ' ').replace(
                '_', ' ').replace(' - ', ' ').replace('&', '_').replace(
                '_:', '').replace(':', '')
            h = h.replace(' _ ', '_')
            # print(h)
            raw_h = h
            if len(h) <= 1:
                continue
            try:
                # int(h[0])
                int(h)
                indexes.append(raw_header.index(h))
                # print(h)
                continue
            except ValueError:
                pass
            h = h.casefold()
            # print(h)
            if len(h) > 30:
                h = h.replace('por favor,', '').replace('"', '').replace('.', '')
                if 'aceito' in h and 'correio' in h:
                    new_header.append('optin_correio')
                    continue
                elif 'aceito' in h and 'e-mail' in h:
                    new_header.append('optin_email')
                    continue
                elif 'aceito' in h and 'sms' in h:
                    new_header.append('optin_sms')
                    continue
                elif 'aceito' in h and 'telefone' in h:
                    new_header.append('optin_telefone')
                    continue
                if len(h.split(' - ')) > 1:
                    try:
                        int(h.split(' - ')[0])
                        h = h.replace(' - ', '_')
                    except ValueError:
                        if len(h.split(' - ')[0]) > 5:
                            if 'l1' in h.split(' - ')[1] or 'l2' in h.split(' - ')[1]:
                                h = h.replace(' - ', ' ')
                            else:
                                h = h.split(' - ')[0]
                        else:
                            h = h.split(' - ')[1]
                if len(h.split(',')) > 1:
                    if h.split(',')[0].strip() != 'outro' and h.split(',')[0].strip() != 'sim':
                        h = h.split(',')[0]
                if len(h.split('(')) > 1:
                    if 'outro' in h.split('(')[1]:
                        h = h
                    elif h.split(',')[0].strip() != 'outro' and h.split(',')[0].strip() != 'sim':
                        h = h.split('(')[0]
                if len(h.split('https')) > 1:
                    h = h.split('https')[0]
                # print(h)
                ht = h.split()
                h = ' '.join([i.strip() for i in word_tokenize(h.casefold()) if i not in stopwords])
                # print(f'clean:{h}')
            h = unidecode.unidecode(str(h)).strip().casefold().replace(' - ', '_').replace(
                '?', '').replace(' ( ', '_').replace(' ) ', '').replace(' / ', '_').replace(' . ', '_').replace(
                ',', '').replace('.', '_').replace('	', '').replace('#', '').replace('*', "").replace(
                '/', '_').replace(' )', '').replace(')', '').replace('(', '').strip().replace(' ', '_').replace(
                '_|_fispal_sorvetes_|_fispal_cafe', '').replace(
                '_desmarque_nao_deseja_essas_atualizacoes', '').replace('|_fispal_sorvetes_|_fispal_cafe', '').replace(
                '_desmarque_esta_caixa_nao_deseja_essas_comunicacoes', '').replace('%', 'porc').replace(
                'ca"digo', 'codigo').replace('a++af', 'ca').replace('cartafo', 'cartao').replace(
                '_fispal_sorvetes_atualizacoes', '').replace('_fispal_sorvetes', '').replace(
                '_service_nao_atualizacoes', '').replace('_nao_atualizacoes', '').replace(
                '_south_america', '').replace('_de_selecao', '').replace('_o_seu', '').replace(
                'no_que_diz_respeito_a_cadeia_produtiva_do_',
                '').replace('caso_seja_um_embarcador_quais_sao_os_tipo_de_carga_que_voce_embarca', 'caso_embarcador_quais_cargas').replace(
                '_atualizacoes', '').replace('entrada_alimentacion_hospitalidad_latam_evento_', '')
            h = '_'.join([i.strip() for i in h.split('_') if i not in stopwords])
            if len(h) > 64:
                print(f'raw: {raw_h} -> {len(raw_h)}')
                print(f'final: {h} -> {len(h)}')
                exit()
            if ('20' in h and len(h) == 10) or ('20' in h and len(h) == 8):
                # print(h)
                # h = 'day_' + h.replace('-', '_').replace('2022', '').replace('2019', '')
                h = 'day_' + h.replace('-', '_').replace('/', '_')
            else:
                h = h.replace('-', '').replace('/', '')
            h = h.replace('__', '_')
            new_header.append(h)
        new_header.append('inserted_at')
        new_header.append('service_provider')
        # raw_header.append('inserted_at')
        # raw_header.append('service_provider')
        # print(new_header)
        # exit()
        return new_header, indexes

    @staticmethod
    def line_cleaner(matchobj: str) -> str:
        """
        Metodo responsavel por normalizar pontuacoes e acentos presentes nos valores
        :param matchobj: sentenca original
        :type matchobj: str
        :return: stenca normalizada
        :rtype: str
        """
        # print(matchobj.group(0))
        if matchobj.group(0) == '  ':
            return ' '
        elif matchobj.group(0) == ') | ':
            return ' | '
        elif matchobj.group(0) == ' ':
            return ' '
        elif matchobj.group(0) == ' , ':
            return ', '
        elif matchobj.group(0) == ')':
            return ' '
        elif matchobj.group(0) == ' | ':
            return ' | '
        elif matchobj.group(0) == ';':
            return ', '
        elif matchobj.group(0).strip() == '|':
            return '|'
        elif matchobj.group(0).strip() == ';':
            return ';'
        elif matchobj.group(0) == ' & ':
            return ' E '
        elif matchobj.group(0).strip() == '&':
            return '&'
        elif matchobj.group(0).strip() == '/':
            return '/'
        elif matchobj.group(0) == ', ':
            return ', '
        elif matchobj.group(0) == ' (':
            return ' '
        elif matchobj.group(0) == ',':
            return ','
        elif matchobj.group(0).strip() == '-':
            return '-'
        elif matchobj.group(0).strip() == '$':
            return '$'
        elif matchobj.group(0).strip() == '- $':
            return '-$'
        elif matchobj.group(0) == '.':
            return '.'
        else:
            return ''

    def xls_vendor(self, service_provider):
        try:
            body = open(self.file_path, 'r', encoding='utf-8').read()
        except UnicodeDecodeError:
            body = open(self.file_path, 'r', encoding='iso-8859-1').read()
        soup = BeautifulSoup(body, 'html.parser')
        table = soup.find('table')
        lines = table.find_all('tr')
        L = []
        for line in lines[1:]:
            l = [a.text for a in line.find_all('td')]
            # if len(l[-1]) <= 1:
            #     l.pop(-1)
            l.append(self.timeDate)
            l.append(service_provider)
            L.append(l)
        if len(L) <= 1:
            print('Empty file!')
            return None, None
        header = []
        raw_header = [l.text for l in lines[0].find_all('td')]
        # self.init_trash = raw_header.index(' OPTIN THIRD')
        # self.end_thrash = self.init_trash + 20
        # new_header = raw_header[:self.init_trash] + raw_header[self.end_thrash:]
        new_header, indexes = self.clean_header(raw_header)
        L.pop(0)
        new_rows = []
        for line in L:
            for i in reversed(indexes):
                line.pop(i)
            new_rows.append(line)
        return new_header, new_rows

    def csv_vendor(self, file, service_provider) -> tuple[None, None] | tuple[list[str], list]:
        """
        Metodo responsavel por converter e noramlizar os dados de um arquivo csv
        :param file: nome do arquivo
        :type file: str
        :param service_provider: nome do fornecedor
        :type service_provider: str
        :return: tupla contendo o header normalizado e as linhas do arquivo normalizado
        :rtype: tuple[None, None] | tuple[list[str], list]
        """
        def clean_csv(f) -> list[str]:
            """
            Metodo para noramlizar as linhas do arquivo
            :param f: linhas do csv
            :type f: list[str]
            :return: linhas normalizadas
            :rtype: list[str]
            """

            EMAIL_RE = re.compile(
                r"^(?=.{1,254}$)(?=.{1,64}@)"
                r"(?:[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+"
                r"(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*)@"
                r"(?:(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}"
                r"|\[(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}"
                r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\])$"
            )

            def is_email(s: str) -> bool:
                return EMAIL_RE.fullmatch(s) is not None

            rows = []
            for line in f:
                for i in reversed(indexes):
                    line.pop(i)
                while len(line) + 2 > len(new_header):
                    line.pop(-1)
                for l in range(len(line)):
                    if len(line[l]) == 0:
                        line[l] = 'NULL'
                    try:
                        int(str(line[l][:2]))
                        nb = True
                    except ValueError:
                        nb = False
                    if nb and '/' in str(line[l]).replace('-', '/') and len(line[l].split()[0]) == 10:
                        pass
                    elif len(line[l]) == 18 and '/' in line[l] and '-' in line[l] and '.' in line[l]:
                        pass
                    elif len(line[l]) == 14 and '-' in line[l] and '.' in line[l]:
                        pass
                    elif len(line[l]) == 17 and '/' in line[l] and ':' in line[l]:
                        pass
                    elif len(line[l]) == 20 and '/' in line[l] and ':' in line[l]:
                        pass
                    elif len(line[l]) == 8 and ':' in line[l]:
                        pass
                    elif is_email(line[l].casefold().strip()):
                        line[l] = line[l].replace('#:~:text', '').upper()
                    else:
                        line[l] = line[l].replace('ÃƒÂ§', 'Ç').replace('ÃƒÂ£', 'Ã').replace('ÃƒÂ´', 'Ô').replace(
                            'ÃƒÂ³', 'Ó').replace('ÃƒÂ¡', 'Á').replace('ÃƒÂª', 'Ê').replace('ÃƒÂµ', 'Õ').replace(
                            'Ã³', 'Ó').replace('Ãxad', 'Í').replace('Ã£', 'Ã').replace("Â§", 'Ç').strip()
                        line[l] = re.sub(r'\W+', self.line_cleaner, unidecode.unidecode(line[l])).strip()
                if service_provider != 'reference':
                    line.append(self.timeDate)
                    line.append(service_provider)
                rows.append(line)
            return rows
        try:
            with open(file, encoding='utf-8', mode='r') as fp:
                f = csv.reader(fp, delimiter='|')
                header = next(f)
                print(header)
                if service_provider == 'dw':
                    new_header = [h.replace('\ufeff', '').strip() for h in header]
                elif service_provider == 'pipedrive':
                    new_header = [h.replace('\ufeff', '').replace(' - ', '_').replace(
                        'ç', 'c').replace('ã', 'a').strip().casefold() for h in header]
                else:
                    new_header, indexes = self.clean_header(header)
                if service_provider == 'reference':
                    new_header.remove('inserted_at')
                    new_header.remove('service_provider')
                print(new_header)
                if service_provider == 'pipedrive':
                    rows = []
                    for line in f:
                        line.append(self.timeDate)
                        line.append(service_provider)
                        rows.append(line)
                elif service_provider == 'dw':
                    rows = []
                    for line in f:
                        # print(line)
                        line[5] = datetime.datetime.strptime(line[5], '%d/%m/%Y').strftime("%Y-%m-%d")
                        line[6] = datetime.datetime.strptime(line[6], '%d/%m/%Y').strftime("%Y-%m-%d")
                        rows.append(line)
                else:
                    rows = clean_csv(f)
                fp.close()
        except UnicodeDecodeError:
            with open(file, encoding='iso-8859-1', mode='r') as fp:
                f = csv.reader(fp, delimiter='|')
                header = next(f)
                print(header)
                new_header, indexes = self.clean_header(header)
                print(new_header)
                rows = clean_csv(f)
                fp.close()
        except StopIteration:
            return None, None
        except FileNotFoundError:
            print('No more files to add.')
            return None, None
        if not rows:
            return None, None
        return new_header, rows

    def truncate_table(self, table_name: str) -> None:
        """
        Metodo responsavel por deletar os dados de uma tabela
        :param table_name: nome da tabela
        :type table_name: str
        """
        create_statement = f"""
                            TRUNCATE TABLE {table_name}
                            """
        print(create_statement)
        self.cursor.execute(text(create_statement))
        self.cursor.commit()

    def create_table(self, table_name: str, header: list[str]) -> None:
        """
        Metodo para criar uma tabela dado um nome e um header
        :param table_name: nome da tabelas
        :type table_name: str
        :param header: header da tabela
        :type header: list[str]
        """
        columns = ''
        for h in header:
            if h == 'inserted_at':
                columns += f"{h} timestamp NOT NULL, \n"
            else:
                columns += f"{h} TEXT, \n"
        cols = columns[:-3]
        create_statement = f"""
                            CREATE TABLE {table_name} (
                                    ingest_id SERIAL PRIMARY KEY,
                                    {cols}
                            )
                            """
        print(create_statement)
        self.cursor.execute(text(create_statement))
        self.cursor.commit()

    def backup_ingest_table(self, table_name: str, backup_table: str, header: list[str]) -> None:
        """
        Metodo responsavel por inserir dados em tabelas de backup
        :param table_name: nome da tabela
        :type table_name: str
        :param backup_table: nome da tabela de backup
        :type backup_table: str
        :param header: header da tabela
        :type header: list[str]
        """
        select_statement = f"""
        SELECT * FROM {table_name}
        """
        bkp_data = self.cursor.execute(text(select_statement)).all()
        D = []
        bkp_data = [x[1:] for x in bkp_data]
        for d in bkp_data:
            bkp_r = str(tuple([str(x) for x in d])).replace("'None'", "NULL").replace("''", 'NULL').replace("'NULL'", "NULL")
            D.append(bkp_r)
        if not D:
            return
        # exit()
        self.insert_table(backup_table, header, D, backup_data=True)

    @staticmethod
    def chunk_list(lst: list, n: int = 10000) -> list[list]:
        """
        Metodo que particiona uma lista em uma lista de listas
        :param lst: lista a ser particionada
        :type lst: list
        :param n: tamanho da particao
        :type n: int
        :return: uma lista de listas com o tamanho n
        :rtype: list[list]
        """
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def insert_table(self, table_name: str, header: list[str], rows: list[str], backup_data: bool = False) -> None:
        """
        Metodo responsavel realizar o upload dos dados no banco
        :param table_name: nome da tabela
        :type table_name: str
        :param header: header da tabela
        :type header: list[str]
        :param rows: lista de linhas a subir
        :type rows: list[str]
        :param backup_data: flag que determina ser ira realizar o backup dos dados
        :type backup_data: bool
        """
        self.errors[table_name] = []
        insert_state = f"""
                        INSERT INTO {table_name}({', '.join(header)}) VALUES 
                        """
        state_lines = []
        if backup_data:
            print(len(rows))
            if len(rows) > 10000:
                row_c = self.chunk_list(rows, 1000)
                for row in row_c:
                    insert_statement = insert_state + ', '.join(row) + ';'
                    self.cursor.execute(text(insert_statement))
                self.cursor.commit()
            else:
                insert_statement = insert_state + ', '.join(rows) + ';'
                self.cursor.execute(text(insert_statement))
                self.cursor.commit()
            return
        # exit()
        for line in rows:
            if self.database == 'bi_cidtech_api':
                l = []
                for e in line:
                    if '{' not in str(e):
                        e = str(e)
                        if e == 'nan':
                            e = 'NULL'
                        l.append(e)
                    else:
                        if len(e) > 0:
                            # print(e)
                            e = str(e).replace("'", '"').replace('None', '""')
                            try:
                                e = json.loads(e)
                                e = json.dumps(e)
                            except json.decoder.JSONDecodeError:
                                e = json.dumps(e)
                        else:
                            e = str(e)
                        l.append(e)
                tup_vars = tuple(l)
                tup_vars = str(tup_vars).replace("'None'", "NULL").replace("''", 'NULL')
            else:
                tup_vars = str(tuple([element.replace(
                    '.:', '').replace('//:', '://').replace(': ', '').replace(' 🇧🇷', '').replace(
                    "#", "").replace('[', '').replace("]", '').replace(' 🔨', '').replace(
                    '°:', '').replace(' 🇵🇹', '').replace(r'\x01', '').replace(r'\xa0', '').replace(
                    '💡', '').strip() for element in line])).replace("'NULL'", "NULL").replace("''", 'NULL')

            state_lines.append(tup_vars)
            if len(state_lines) > 10000:
                insert_statement = insert_state + ', '.join(state_lines) + ';'
                self.cursor.execute(text(insert_statement))
                self.cursor.commit()
                state_lines = []
        lines_to_add = len(state_lines)
        if lines_to_add > 0:
            insert_statement = insert_state + ', '.join(state_lines) + ';'
            self.cursor.execute(text(insert_statement))
            self.cursor.commit()
        else:
            print('Nothing to Commit')

    def check_unique(self, table_name, header, rows):
        try:
            self.key_value = header.index('codigodebarras')
            key_term = 'codigodebarras'
        except ValueError:
            try:
                self.key_value = header.index('id')
                key_term = 'id'
            except ValueError:
                try:
                    self.key_value = header.index('barcode')
                    key_term = 'barcode'
                except ValueError:
                    self.key_value = header.index('nr')
                    key_term = 'nr'

        return None
        key_values = tuple([l[self.key_value] for l in rows])
        check_state = f"""
        SELECT {key_term}
        FROM {table_name}
        WHERE {key_term} in {key_values}
        """
        # line_input = {}
        # new_line = [l[:self.init_trash] + l[self.end_thrash:] for l in rows]
        # tup_vars = str(tuple([f"%({h})s" for h in header]))
        # insert_state += tup_vars
        query = self.cursor.execute(text(check_state))
        all_query = query.all()
        print(f'query: {all_query}')
        if all_query:
            old_data = set([str(d[0]) for d in all_query])
            return old_data
        else:
            return None

    def adjust_table(self, file: str):
        """
        Metodo ajusta a ultima coluna da tabela de faturamentobi e salva o arquivo
        :param file: nome do arquivo
        :type file: str
        """
        import pandas as pd
        df = pd.read_csv(self.dir_path + file, encoding='utf-8', sep='|')
        d_col = list(df.columns)
        if 'TAXA INTERMEDIAÇÃO' in d_col:
            df['TAXA INTERMEDIAÇÃO'] = 0
        elif 'TAXA INTERMEDIAÃ‡ÃƒO' in d_col:
            df['TAXA INTERMEDIAÃ‡ÃƒO'] = 0
        # df['TAXA DE PARCELAMENTO'] = 0
        df.to_csv(self.dir_path + file, encoding='utf-8', sep='|', index=False)

    def insert_via_pandas(self, file, table_name):
        file_path = self.dir_path + file
        df = pd.read_csv(file_path, encoding='utf-8', sep='|')
        df['Deal - Edição'] = df['Deal - Edição'].astype(int)
        database = 'business_inteligence'
        mysql_user = 'forge'
        mysql_password = 'tEDI2JItzUdhUc6dW2GI'
        mysql_host = '195.35.17.83'
        connection_string = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{database}'
        bi_engine = create_engine(connection_string, echo=True)
        df.to_sql(table_name, con=bi_engine, schema='bi_aux', if_exists='replace')


    def maestro_adjust_table(self, file: str, edition, exhibition_code):
        import pandas as pd
        import numpy as np

        df = pd.read_excel(self.dir_path + file)
        print(df.columns)
        df['edition'] = edition
        df['exhibition_code'] = exhibition_code
        df = df.rename(columns={
            'ID': 'vendor_ingest_id',
            'NOSSO NÚMERO/PEDIDO': 'order_id',
            'Data de venda do ingresso (obrigatório)': 'order_date',
            'Classe (obrigatório)': 'classe',
            'PRODUTO': 'product',
            'SUBPRODUTO': 'subproduct',
            'REFERENTE': 'referente',
            'TIPO DE FATURAMENTO': 'tipo_faturamento',
            'TIPO DE COMPRA': 'tipo_compra',
            'NOTA FISCAL': 'nota_fiscal',
            'CPF': 'cpf',
            'NOME': 'nome',
            'SOBRE NOME': 'sobrenome',
            'CNPJ': 'cnpj',
            'RAZÃO SOCIAL': 'razao_social',
            'ENDEREÇO': 'endereco',
            'NÚMERO': 'numero',
            'COMPLEMENTO': 'complemento',
            'BAIRRO': 'bairro',
            'CEP': 'cep',
            'ESTADO': 'estado',
            'CIDADE': 'cidade',
            'EMAIL': 'email',
            'FORMA PAGAMENTO': 'forma_pagamento',
            'ID DE PAGAMENTO': 'id_pagamento',
            'ID DE TRANSAÇÃO': 'id_transacao',
            'SIMPLES NACIONAL': 'simples_nacional',
            'ORGÃO PÚBLICO': 'orgao_publico',
            'DATA PAGAMENTO': 'data_pagamento',
            'VALOR BRUTO': 'valor_bruto',
            'IR 1,5%': 'ir',
            'PIS 0,65%': 'pis',
            'COFINS 3%': 'cofins',
            'CSLL 1%': 'csll',
            'ISS 2%': 'iss',
            'VALOR PEDIDO INICIAL (UPGRADE)': 'valor_pedido_inicial',
            'Valor do desconto': 'valor_desconto',
            'VALOR LIQUIDO': 'valor_liquido',
            'Tipo de Desconto': 'tipo_desconto',
            'AUTORIZAÇÃO': 'autorizacao',
            'últmos 4 dígitos': 'quatro_digitos',
            'Tipo de venda': 'tipo_venda',
            'Autorização venda física?': 'venda_fisica',
            'STATUS PAGAMENTO': 'status_pagamento',
            'PARCELAS': 'parcelas',
            'COD. AUTORIZAÇÃO': 'cod_autorizacao',
            'BANDEIRA': 'bandeira',
            'OBSERVAÇÃO': 'observacao',
            'NOME COMPLETO FINANCEIRO CONTAS PAGAR': 'nome_completo_financeiro',
            'TELEFONE FINANCEIRO CONTAS PAGAR': 'telefone_financeiro',
            'EMAIL FINANCEIRO CONTAS PAGAR': 'email_financeiro',
            'Código alfanumérico do ingresso (obrigatório)': 'codigo_beneficiario'
        })

        columns = ['vendor_ingest_id', 'order_id', 'exhibition_code', 'edition', 'order_date', 'classe', 'product', 'subproduct', 'referente', 'tipo_faturamento', 'tipo_compra', 'nota_fiscal', 'cpf', 'nome', 'sobrenome', 'cnpj', 'razao_social', 'endereco', 'numero', 'complemento', 'bairro', 'cep', 'estado', 'cidade', 'email', 'forma_pagamento', 'id_pagamento', 'id_transacao', 'simples_nacional', 'orgao_publico', 'data_pagamento', 'valor_bruto', 'ir', 'pis', 'cofins', 'csll', 'iss', 'valor_pedido_inicial', 'valor_desconto', 'valor_liquido', 'tipo_desconto', 'autorizacao', 'quatro_digitos', 'tipo_venda', 'venda_fisica', 'status_pagamento', 'parcelas', 'cod_autorizacao', 'bandeira', 'observacao', 'nome_completo_financeiro', 'telefone_financeiro', 'email_financeiro', 'codigo_beneficiario']
        number_cols = ['valor_bruto', 'ir', 'pis', 'cofins', 'csll', 'iss', 'valor_pedido_inicial', 'valor_desconto', 'valor_liquido', ]
        df = df[columns]
        df['product'] = df['product'].str.replace('$', '')
        for col in columns:
            df[col] = df[col].replace('-', np.nan)
        df['codigo_beneficiario'] = df['codigo_beneficiario'].astype('Int64')
        for nc in number_cols:
            df[nc] = df[nc].astype(float)
        df.to_csv(self.dir_path + file, encoding='utf-8', index=False, sep='|')


    def send_vendors_to_raw(self) -> None:
        """
        Metodo principal responsavel por inserir os dados dos arquivos presentes no
        diretorio no banco de dados com os devidos tratamentos e transformacoes
        e registra o log em arquivo externo
        :return: arquivo com log de erros e sucessos por tabela
        :rtype: dict
        """
        trunk = 0
        for file in list(set(self.files)):
            raw_table_name = file.split('.')[0]
            service_provider = raw_table_name.split('_')[0]
            table_name = '_'.join(raw_table_name.split('_')[1:-1]).replace('-', '_')
            self.file_path = self.dir_path + file
            if 'pipedrive' in table_name:
                print(table_name)
                self.con = MysqlSandBoxConnector(database='bi_aux')
                self.cursor = self.con
                table_name = table_name.casefold()
                self.insert_via_pandas(file, table_name)
                continue
            if service_provider == 'cidtech':
                print(f'Service Provider: {service_provider}')
                self.con = MysqlSandBoxConnector(database='bi_cidtech_admin')
                self.cursor = self.con.session
                if table_name in self.get_tables(self.cursor):
                    create = False
                else:
                    create = True
                print(table_name, create)
                header, rows = self.csv_vendor(self.dir_path + file, service_provider)
                if not header:
                    os.remove(self.file_path)
                    continue
                if create:
                    self.create_table(table_name, header)
                elif not create and trunk != table_name:
                    self.truncate_table(table_name)
                    trunk = table_name
                # self.check_unique(table_name, header, rows)
                self.insert_table(table_name, header, rows)
                new_path = self.dir_path + '\\inserted\\' + file
                shutil.move(self.file_path, new_path)
            elif service_provider == 'sistematizze':
                print(f'Service Provider: {service_provider}')
                self.con = MysqlSandBoxConnector(database='bi_sistematizze')
                self.cursor = self.con.session
                print(table_name)
                if 'faturamentobi' in table_name:
                    self.adjust_table(file)
                backup_table_name = f'bkp_{table_name}'
                print(table_name)
                if table_name in self.get_tables(self.cursor):
                    create = False
                else:
                    create = True
                header, rows = self.csv_vendor(self.dir_path + file, service_provider)
                if not header and not rows:
                    os.remove(self.file_path)
                    continue
                if not header:
                    os.remove(self.file_path)
                    continue
                if create:
                    self.create_table(table_name, header)
                elif not create and trunk != table_name:
                    self.truncate_table(table_name)
                    trunk = table_name
                self.insert_table(table_name, header, rows)
                new_path = self.dir_path + '\\inserted\\' + file
                shutil.move(self.file_path, new_path)
            elif service_provider == 'maestro':
                print(f'Service Provider: {service_provider}')
                self.con = MysqlSandBoxConnector(database='bi_maestro', echo=True)
                self.cursor = self.con.session
                print(table_name)
                if table_name in self.get_tables(self.cursor):
                    create = False
                else:
                    create = True
                edition = table_name.split('_')[0]
                event_name = '_'.join(table_name.split('_')[1:-1])
                self.maestro_adjust_table(file, edition, self.ex_code_dict[event_name])
                print(table_name, create)
                header, rows = self.csv_vendor(self.dir_path + file, service_provider)
                if not header and not rows:
                    os.remove(self.file_path)
                    raise Exception("Error on uploading: File with empty rows and header")
                if not header:
                    raise Exception("Error on uploading: File with empty header")
                if create:
                    print(f'Creating table: {table_name}')
                    self.create_table(table_name, header)
                elif not create and trunk != table_name:
                    print(f'Truncating table: {table_name}')
                    self.truncate_table(table_name)
                    trunk = table_name
                print(f'Uploading to table: {table_name}')
                self.insert_table(table_name, header, rows)
                new_path = self.dir_path + '\\inserted\\' + file
                shutil.move(self.file_path, new_path)
            elif service_provider == 'techhouse' or service_provider == 'analitico':
                print(f'Service Provider: {service_provider}')
                self.con = MysqlSandBoxConnector(database='bi_techhouse')
                self.cursor = self.con.session
                # table_year = table_name.split('_')[-1]
                # table_name = '_'.join(table_name.split('_')[:-1])
                # if service_provider == 'analitico':
                #     table_name = f'{table_name}_analytic'
                #     service_provider = 'techhouse'
                # else:
                #     table_name = f'{table_name}_cred'
                # print(table_name, self.get_tables(self.cursor))
                # exit()
                if table_name in self.get_tables(self.cursor):
                    create = False
                else:
                    create = True
                print(table_name)
                print(f'create: {create}')
                header, rows = self.csv_vendor(self.dir_path + file, service_provider)
                if not header:
                    os.remove(self.file_path)
                    continue
                if create:
                    self.create_table(table_name, header)
                elif not create and trunk != table_name:
                    self.truncate_table(table_name)
                    trunk = table_name
                # self.check_unique(table_name, header, rows)
                self.insert_table(table_name, header, rows)
                new_path = self.dir_path + '\\inserted\\' + file
                shutil.move(self.file_path, new_path)
            elif ('leads' in file.casefold()) or service_provider == 'aux':
                print(f'Service Provider: Aux')
                self.con = MysqlSandBoxConnector(database='bi_aux')
                self.cursor = self.con.session
                table_name = table_name.casefold()
                if 'pipedrive' in table_name:
                    service_provider = 'pipedrive'
                header, rows = self.csv_vendor(self.dir_path + file, service_provider)
                if not header:
                    os.remove(self.file_path)
                    continue
                if table_name in self.get_tables(self.cursor):
                    create = False
                else:
                    create = True
                print(table_name)
                # print(header)
                # print(rows)
                # exit()
                if create:
                    self.create_table(table_name, header)
                elif not create and trunk != table_name:
                    self.truncate_table(table_name)
                    trunk = table_name
                # self.check_unique(table_name, header, rows)
                self.insert_table(table_name, header, rows)
                new_path = self.dir_path + '\\inserted\\' + file
                shutil.move(self.file_path, new_path)
            elif service_provider == 'dw':
                print(f'Service Provider: DW')
                self.con = MysqlSandBoxConnector(database='bi_dw')
                self.cursor = self.con.session
                table_name = table_name.casefold()
                header, rows = self.csv_vendor(self.dir_path + file, service_provider)
                if not header:
                    os.remove(self.file_path)
                    continue
                if table_name in self.get_tables(self.cursor):
                    create = False
                else:
                    create = True
                print(table_name)
                # print(header)
                # print(rows)
                # exit()
                if create:
                    self.create_table(table_name, header)
                elif not create and trunk != table_name:
                    self.truncate_table(table_name)
                    trunk = table_name
                # self.check_unique(table_name, header, rows)
                self.insert_table(table_name, header, rows)
                new_path = self.dir_path + '\\inserted\\' + file
                shutil.move(self.file_path, new_path)
            elif service_provider == 'map':
                print(f'Service Provider: Map')
                self.con = MysqlSandBoxConnector(database='bi_onsite_map')
                self.cursor = self.con.session
                table_name = table_name.casefold()
                header, rows = self.csv_vendor(self.dir_path + file, service_provider)
                if not header:
                    os.remove(self.file_path)
                    continue
                if table_name in self.get_tables(self.cursor):
                    create = False
                else:
                    create = True
                print(table_name)
                # print(header)
                # print(rows)
                # exit()
                if create:
                    self.create_table(table_name, header)
                elif not create and trunk != table_name:
                    self.truncate_table(table_name)
                    trunk = table_name
                # self.check_unique(table_name, header, rows)
                self.insert_table(table_name, header, rows)
                new_path = self.dir_path + '\\inserted\\' + file
                shutil.move(self.file_path, new_path)
            elif service_provider == 'reference':
                self.con = MysqlSandBoxConnector(database='bi_reference')
                self.cursor = self.con.session
                table_name = table_name.split('_')
                table_name = f'ref_{"_".join(table_name[0:-1])}'
                print(table_name)
                print(table_name)
                if table_name in self.get_tables(self.cursor):
                    create = False
                else:
                    create = True
                header, rows = self.csv_vendor(self.dir_path + file, service_provider)

                if not header and not rows:
                    os.remove(self.file_path)
                    continue
                if not header:
                    os.remove(self.file_path)
                    continue
                if create:
                    self.create_table(table_name, header)
                elif not create and trunk != table_name:
                    self.truncate_table(table_name)
                    trunk = table_name
                self.insert_table(table_name, header, rows)
                new_path = self.dir_path + '\\inserted\\' + file
                shutil.move(self.file_path, new_path)
        pprint(f'{len(self.errors)} tables returned some error on ingest -> {self.errors}')

    
if __name__ == '__main__':
    a = VendorsIngestion()
    a.send_vendors_to_raw()
