import pandas as pd
from df_creating import gs1_requester
from attributes_extractor import AtrrValueParesr
import sys
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# ОТКЛЮЧАЕТ ВОРНИНГИ НО ВОЗМОЖНО ЗАМЕДЛЯЕТ
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import argparse
from  argparse import RawDescriptionHelpFormatter
import operator
import textwrap
import textwrap as _textwrap
'''
class LineWrapRawTextHelpFormatter(argparse.RawDescriptionHelpFormatter):
    def _split_lines(self, text, width):
        text = self._whitespace_matcher.sub(' ', text).strip()
        return _textwrap.wrap(text, width)
'''

general_description = '''Утилита парсинга ГС1. Делает запрос по методу GetItemByGTIN.
        --------------------------------------------------------------------------
        Если в возвращаемом XML для value искомого атрибута существут ключ в Lst_AttrTOGS1Attr.valueMap, то в качестве значения возвращается соотвествующее данному ключу значение.
        Если для value искомого атрибута НЕ существут valueMap, то в качестве значензия возвращается значение параметра descr из полученного XML (TBD).
        Пример команды: python cli_app.py test_in.xlsx test_out.xlsx, где test_in.xlsx и test_out.xlsx обязательные позиционные аргументы, т.е. обязательные аргументы порядок задания которых является критичным.
        Сперва необходимо задать входной файл, затем выходной.
        Путь к файлам допустимо указывать как абсолютный (например D:\\test_out.xlsx) ,
    так и относительный. Например ..\\test_out.xlsx (для записи в директорию выше уровнем),
    или test\\test_out.xlsx (для записи в директорию ниже уровнем, или test_out.xlsx (для записи в ту же директорию, где лежит исполняемы скрипт'''




class cli_class:

    def __init__(self):

        parser = argparse.ArgumentParser(formatter_class=RawDescriptionHelpFormatter, description=general_description)
        subparsers = parser.add_subparsers(dest='subparser_name')
        subparser1 = subparsers.add_parser('file')

        subparser1.add_argument('-i', metavar="in_file", nargs='?',
                            type=argparse.FileType('r'),  # str , #argparse.FileType('r'),                     #encoding='Windows-1252'), # Windows-1252 encoded XLSX file, , encoding='latin-1'
                            # action="store",
                            default=sys.stdin,  # argparse.FileType('r'),
                            help='''Входной файл с двумя обязательными столбцами \"GTIN\" и \"GS1Attr\". Если файл лежит в той же директории, что и исполняемый файл, то можно указать только его название ''',
                            required=False)

        subparser1.add_argument('-o', metavar="out_file",
                            type=argparse.FileType('w'),
                            default=sys.stdout,
                            help='''Выходной файл. Путь допустимо указывать как абсолютный, так и относительный. Если файл лежит в той же директории, что и исполняемый файл, то можно указать только его название''')  # dest="output_file", default='out_file.xlsx',

        subparser1.add_argument("-p", "--print", help="print result in current window", action='store_true')  # ,,
        subparser1.add_argument("-novm", "--no_valueMap", help="отключает запрос в БД для получения valueMap", action='store_true', )
        subparser1.add_argument("-v", "--verbose", help="отображает процесспарсинга GS1", action='store_true')
        subparser1.add_argument("-e", "--eav", help="входной файл в EAV формате, т.е. содержит два столбца 'GTIN' и 'GS1Attr'", action='store_true')
        subparser1.add_argument("-c", "--chunk", help="размер чанка (порции выгрузки)", default=50, type=int)



        #subparser2 = subparsers.add_parser('clipboard')

        #subparser2.add_argument('-gtn', '--gtins', nargs='+', help='список gtin через пробел', required=False)
        #subparser2.add_argument('-att', '--attributes', nargs='+', help='список GS1Attr через пробел', required=False)


        args = parser.parse_args()


        self.input_file_full_path = args.i.name
        self.output_file_full_path = args.o.name
        self.printout_result = args.print
        self.verbose_result = args.verbose
        self.no_valueMap = args.no_valueMap
        self.eav = args.eav
        self.chunk = args.chunk
        #self.gtins = args.gtins
        #self.attributes = args.attributes

        #subparser2.set_defaults(func=cli_class.get_table_from_clipboard(self))

    @staticmethod
    def test_db_connection():
        #TODO добавить Tкy Except
        try:
            status, message = AtrrValueParesr.test_connection()
        except Exception as e:
            status = False
            message = str(e)
        return  status, message

    def check_output_file_extension(self, output_file_full_path):
        extension = str(output_file_full_path[(len(output_file_full_path) - 5):])
        if extension == '.xlsx':
            current_function_result = True
        else:
            current_function_result = False
        return current_function_result

    def check_input_file_format_eav(self, input_file_full_path):
        input_df = pd.read_excel(input_file_full_path)
        header_list = list(input_df.columns.values)
        print('header_list =',header_list)
        needed_columns = ['GTIN',	'GS1Attr']
        if header_list == needed_columns:
            current_function_result = True
        else:
            current_function_result = False
        return current_function_result

    def check_input_file_format_grid(self, input_file_full_path):
        input_df = pd.read_excel(input_file_full_path)
        try:
            full_gtin_list = input_df['GTIN'].values.tolist()
            column_list = input_df.columns.values.tolist()
            attr_list = list(column_list)
            attr_list.remove('GTIN')

            if len(attr_list)>0 and 'GTIN' in column_list and len(full_gtin_list) > 0:
                current_function_result = True
            else:
                current_function_result = False
        except Exception as e:
            print('!!!!!!!!',e)
            current_function_result = False
        return current_function_result

    def check_chunk(self, chunk):
        if int(chunk)>50:
            current_function_result = False
        else:
            current_function_result = True
        return current_function_result


    @staticmethod
    def preliminary_single_check(current_check_result, negative_output_message, previous_check_passed = True, previous_output_message ='', skip_this_check=False):
        positive_output_message = 'Все предварительные проверки пройдены. Начался парсинг GS1 \n'
        if skip_this_check == True and previous_check_passed==False:
            go_to_next_check = False
            output_message = previous_output_message
        elif skip_this_check == True and previous_check_passed==True:
            go_to_next_check = True
            output_message = positive_output_message
        elif previous_check_passed == True and current_check_result == True:
            go_to_next_check = True
            output_message = positive_output_message
        elif previous_check_passed == False:
            go_to_next_check = False
            output_message  = previous_output_message
        elif current_check_result == False:
            go_to_next_check = False
            output_message = negative_output_message
        return go_to_next_check, output_message

    def preliminary_check_set(self):
         # проерка соединения
        is_connected_to_database, connection_message = cli_class.test_db_connection()
        go_to_next_check, output_message = cli_class.preliminary_single_check(skip_this_check=self.no_valueMap,
                                                                              current_check_result=is_connected_to_database,
                                                                              negative_output_message=' - ' + connection_message)

        # проверка формата для EAV
        input_eav_file_check = cli_class.check_input_file_format_eav(self, self.input_file_full_path)
        go_to_next_check, output_message = cli_class.preliminary_single_check(skip_this_check=operator.not_(self.eav), # если формат входного файла EAV - то НЕ пропускаем проверку
                                                                              previous_check_passed=go_to_next_check,
                                                                              current_check_result=input_eav_file_check,
                                                                              previous_output_message=output_message,
                                                                              negative_output_message=' - входной файл должен содержать два столбца с названиями: GTIN, GS1Attr')

        # проверка формата для GRID
        input_grid_file_check = cli_class.check_input_file_format_grid(self, self.input_file_full_path)
        go_to_next_check, output_message = cli_class.preliminary_single_check(skip_this_check=self.eav,# если формат входного файла EAV -  проверку ПРОПУСКАЕМ
                                                                              previous_check_passed=go_to_next_check,
                                                                              current_check_result=input_grid_file_check,
                                                                              previous_output_message=output_message,
                                                                              negative_output_message=' - столбцы входного файла должны содержать столбец \'GTIN\' и как минимум еще один столбец с произвольным значением GS1AttrId')

        # проверка что chunk <= 50
        chunk_less_or_equal_50 = cli_class.check_chunk(self, self.chunk)
        go_to_next_check, output_message = cli_class.preliminary_single_check(previous_check_passed=go_to_next_check,
                                                                               current_check_result=chunk_less_or_equal_50,
                                                                               previous_output_message=output_message,
                                                                               negative_output_message='значение chunk не должно превышать 50')

        # проверка расширения  выходного файла ЭТА ПРОВЕРКА  ДОЛЖНА БЫТЬ ПОСЛЕДНЕЙ. ЕЕ НЕЛЬЗЯ СКИПАТЬ!
        out_file_extension_check = cli_class.check_output_file_extension(self, self.output_file_full_path)
        go_to_next_check, output_message = cli_class.preliminary_single_check(previous_check_passed=go_to_next_check,
                                                                               current_check_result=out_file_extension_check,
                                                                               previous_output_message=output_message,
                                                                               negative_output_message=' - формат выходного файла должен быть .xlsx')

        # TODO здесь продолжить проверки

        # здесь закончились провеерки
        #########################################################################################

        all_checks_passed = go_to_next_check
        return all_checks_passed, output_message

    def eav_file_generator(self): # in_file, out_file
        print('\n' + '-' * 20)



        all_checks_passed, output_message = cli_class.preliminary_check_set(self)
        if all_checks_passed:
            print(output_message)

            input_df = pd.read_excel(self.input_file_full_path, dtype={'GTIN': object})
            gs1_request = gs1_requester(source_df=input_df, get_valueMap=operator.not_(self.no_valueMap), verbose_result = self.verbose_result)

            if self.eav:
                output_df = gs1_request.batch_requester_eav_mode(chunk=self.chunk)

            else:
                output_df = gs1_request.batch_requester(chunk=self.chunk)

            output_df.to_excel(self.output_file_full_path, sheet_name='sheet_1', index=False)

            if self.printout_result:
                print('\nтекущий результат:\n{}'.format(output_df.to_string(index=False)))

            print('\nФайл {} записан'.format(self.output_file_full_path))

        else:
            print('В процессе предварительных проверок обнаружены ошибки:\n')
            print(output_message)





if __name__ == "__main__":
    inp = 'data/test_eav_long.xlsx'
    outp = 'data/result.xlsx'
    args = argparse.Namespace(input_file_full_path=inp, output_file_full_path =outp, printout_result = True,verbose_result = True, no_valueMap = False, eav = True, chunk=1)





