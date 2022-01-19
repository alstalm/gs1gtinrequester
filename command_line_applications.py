import pandas as pd
from df_creating import one_by_one_requester
from attributes_extractor import AtrrValueParesr

import sys
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# ОТКЛЮЧАЕТ ВОРНИНГИ НО ВОЗМОЖНО ЗАМЕДЛЯЕТ
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import argparse
from  argparse import RawDescriptionHelpFormatter
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

        parser.add_argument('-i', metavar="in_file", nargs='?',
                            type=argparse.FileType('r'),  # str , #argparse.FileType('r'),                     #encoding='Windows-1252'), # Windows-1252 encoded XLSX file, , encoding='latin-1'
                            # action="store",
                            default=sys.stdin,  # argparse.FileType('r'),
                            help='''Входной файл с двумя обязательными столбцами \"GTIN\" и \"GS1Attr\". Если файл лежит в той же директории, что и исполняемый файл, то можно указать только его название ''')

        parser.add_argument('-o', metavar="out_file",
                            type=argparse.FileType('w'), default=sys.stdout,
                            help='''Выходной файл. Путь допустимо указывать как абсолютный, так и относительный. Если файл лежит в той же директории, что и исполняемый файл, то можно указать только его название''')  # dest="output_file", default='out_file.xlsx',

        parser.add_argument("-p", "--print", help="print result in current window", action='store_true')  # ,,
        parser.add_argument("-v", "--verbose", help="print details in current window", action='store_true')
        # group = parser.add_mutually_exclusive_group()
        # group.add_mutually_exclusive_group("-c", "--clipboard", type=str, help="input from clipboard" )
        # group.add_mutually_exclusive_group("-f", "--file", type=str, help="input from file" )
        args = parser.parse_args()

        self.input_file_full_path = args.i.name
        self.output_file_full_path = args.o.name
        self.printout_result = args.print
        self.verbose_result = args.verbose

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

    def check_input_file_columns(self, input_file_full_path):
        input_df = pd.read_excel(input_file_full_path)
        header_list = list(input_df.columns.values)
        needed_columns = ['GTIN',	'GS1Attr']
        if header_list == needed_columns:
            current_function_result = True
        else:
            current_function_result = False
        return current_function_result

    @staticmethod
    def preliminary_check(previous_check_passed, current_check_result, previous_output_message, negative_output_message):
        output_message = '' # далее это значение перезапишется
        default_positive_message = 'Все предварительные проверки пройдены. \nНаинчается парсинг GS1 \n'
        go_to_next_check = False
        positive_output_message = default_positive_message  #TODO  подумать как убрать из статического метода в глобальную переменную
        if previous_check_passed == True and current_check_result == True:
            go_to_next_check = True
            output_message = positive_output_message
        elif previous_check_passed == False:
            go_to_next_check = False
            output_message  = previous_output_message
        elif current_check_result == False:
            go_to_next_check = False
            output_message = negative_output_message
        return go_to_next_check, output_message


    def eav_file_generator(self): # in_file, out_file
        print('\n' + '-' * 20)
#########################################################################################
        # Выполним проверки

        # проерка соединения
        successfully_connection_to_database, connection_message = cli_class.test_db_connection()
        go_to_next_check, output_message = cli_class.preliminary_check(previous_check_passed = True,
                                                                       current_check_result = successfully_connection_to_database,
                                                                       previous_output_message = '',
                                                                       negative_output_message = ' - ' + connection_message)

        # проверка расширения  выходного файла
        out_file_extension_check = cli_class.check_output_file_extension(self, self.output_file_full_path)
        go_to_next_check, output_message = cli_class.preliminary_check(previous_check_passed = go_to_next_check,
                                                                       current_check_result = out_file_extension_check,
                                                                       previous_output_message = output_message,
                                                                       negative_output_message = ' - формат выходного файла должен быть .xlsx')

        # проверка формата для EAV
        input_file_column_check = cli_class.check_input_file_columns(self, self.input_file_full_path)
        go_to_next_check, output_message = cli_class.preliminary_check(previous_check_passed = go_to_next_check,
                                                                       current_check_result = input_file_column_check,
                                                                       previous_output_message=output_message,
                                                                       negative_output_message = ' - входной файл должен содержать два столбца с названиями: GTIN, GS1Attr')



    #TODO здесь продолжить проверки

            # здесь закончились провеерки
#########################################################################################

        all_checks_passed = go_to_next_check

        if all_checks_passed:
            print(output_message)

            input_df = pd.read_excel(self.input_file_full_path, dtype={'GTIN': object})
            input_df = input_df.loc[:, ['GTIN', 'GS1Attr']]

            output_df = one_by_one_requester(source_df=input_df)
            output_df.to_excel(self.output_file_full_path, sheet_name='sheet_1', index=False)

            if self.printout_result:
                print('\nФайл записан. \n\nтекущий результат:\n{}'.format(output_df.to_string(index=False)))
            else:
                pass # print('\nfile has successfully written')

            if self.verbose_result:

                print('\nhello from verbose inside eav_file_generator')
            else:
                pass #

            print('\nФайл записан')

        else:
            print('В процессе предварительных проверок обнаружены ошибки:\n')
            print(output_message)


if __name__ == "__main__":
    inp = 'data/test_in_file.xlsx'
    outp = 'data/parsed.xlsx2'
    args = argparse.Namespace(input_file_full_path=inp, output_file_full_path =outp)
    cli_class.eav_file_generator(args)








