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


general_description = '''Утилита парсинга ГС1. Делает запрос по методу GetItemByGTIN.
        --------------------------------------------------------------------------
        Если в возвращаемом XML для value искомого атрибута существут ключ в Lst_AttrTOGS1Attr.valueMap, то в качестве значения возвращается соотвествующее данному ключу значение.
        Если для value искомого атрибута НЕ существут valueMap, то в качестве значензия возвращается значение параметра descr из полученного XML (TBD).
        Пример команды: python cli_app.py test_in.xlsx test_out.xlsx, где test_in.xlsx и test_out.xlsx обязательные позиционные аргументы, т.е. обязательные аргументы порядок задания которых является критичным.
        Сперва необходимо задать входной файл, затем выходной.
        Путь к файлам допустимо указывать как абсолютный (например D:\\test_out.xlsx) ,
    так и относительный. Например ..\\test_out.xlsx (для записи в директорию выше уровнем),
    или test\\test_out.xlsx (для записи в директорию ниже уровнем, или test_out.xlsx (для записи в ту же директорию, где лежит исполняемы скрипт'''



def test_db_connection():
    # TODO добавить Tкy Except
    try:
        status, message = AtrrValueParesr.test_connection()
    except Exception as e:
        status = False
        message = str(e)
    return status, message


def check_output_file_extension(output_file_full_path):
    extension = str(output_file_full_path[(len(output_file_full_path) - 5):])
    if extension == '.xlsx':
        current_function_result = True
    else:
        current_function_result = False
    return current_function_result


def check_input_file_format_eav(input_file_full_path):
    input_df = pd.read_excel(input_file_full_path)
    header_list = list(input_df.columns.values)
    needed_columns = ['GTIN', 'GS1Attr']
    if header_list == needed_columns:
        current_function_result = True
    else:
        current_function_result = False
    return current_function_result


def check_input_file_format_grid(input_file_full_path):
    input_df = pd.read_excel(input_file_full_path)
    try:
        full_gtin_list = input_df['GTIN'].values.tolist()
        column_list = input_df.columns.values.tolist()
        attr_list = list(column_list)
        attr_list.remove('GTIN')

        if len(attr_list) > 0 and 'GTIN' in column_list and len(full_gtin_list) > 0:
            current_function_result = True
        else:
            current_function_result = False
    except Exception as e:
        print('!!!!!!!!', e)
        current_function_result = False
    return current_function_result


def check_chunk(chunk):
    if int(chunk) > 50:
        current_function_result = False
    else:
        current_function_result = True
    return current_function_result


def preliminary_single_check(current_check_result, negative_output_message, previous_check_passed=True, previous_output_message='', skip_this_check=False):
    positive_output_message = 'Все предварительные проверки пройдены. Начался парсинг GS1 \n'
    if skip_this_check == True and previous_check_passed == False:
        go_to_next_check = False
        output_message = previous_output_message
    elif skip_this_check == True and previous_check_passed == True:
        go_to_next_check = True
        output_message = positive_output_message
    elif previous_check_passed == True and current_check_result == True:
        go_to_next_check = True
        output_message = positive_output_message
    elif previous_check_passed == False:
        go_to_next_check = False
        output_message = previous_output_message
    elif current_check_result == False:
        go_to_next_check = False
        output_message = negative_output_message
    return go_to_next_check, output_message


def preliminary_check_set(args):

    input_file_full_path = args.i.name
    output_file_full_path = args.o.name
    eav = args.eav
    no_valueMap = args.no_valueMap
    chunk = args.chunk

    # проерка соединения
    is_connected_to_database, connection_message = test_db_connection()
    go_to_next_check, output_message = preliminary_single_check(skip_this_check=no_valueMap,
                                                                          current_check_result=is_connected_to_database,
                                                                          negative_output_message=' - ' + connection_message)

    # проверка формата для EAV
    input_eav_file_check = check_input_file_format_eav(input_file_full_path)
    go_to_next_check, output_message = preliminary_single_check(skip_this_check=operator.not_(eav),  # если формат входного файла EAV - то НЕ пропускаем проверку
                                                                          previous_check_passed=go_to_next_check,
                                                                          current_check_result=input_eav_file_check,
                                                                          previous_output_message=output_message,
                                                                          negative_output_message=' - входной файл должен содержать два столбца с названиями: GTIN, GS1Attr')

    # проверка формата для GRID
    input_grid_file_check = check_input_file_format_grid(input_file_full_path)
    go_to_next_check, output_message = preliminary_single_check(skip_this_check=eav,  # если формат входного файла EAV -  проверку ПРОПУСКАЕМ
                                                                          previous_check_passed=go_to_next_check,
                                                                          current_check_result=input_grid_file_check,
                                                                          previous_output_message=output_message,
                                                                          negative_output_message=' - столбцы входного файла должны содержать столбец \'GTIN\' и как минимум еще один столбец с произвольным значением GS1AttrId')

    # проверка что chunk <= 50
    chunk_less_or_equal_50 = check_chunk(chunk)
    go_to_next_check, output_message = preliminary_single_check(previous_check_passed=go_to_next_check,
                                                                          current_check_result=chunk_less_or_equal_50,
                                                                          previous_output_message=output_message,
                                                                          negative_output_message='значение chunk не должно превышать 50')

    # проверка расширения  выходного файла ЭТА ПРОВЕРКА  ДОЛЖНА БЫТЬ ПОСЛЕДНЕЙ. ЕЕ НЕЛЬЗЯ СКИПАТЬ!
    out_file_extension_check = check_output_file_extension(output_file_full_path)
    go_to_next_check, output_message = preliminary_single_check(previous_check_passed=go_to_next_check,
                                                                          current_check_result=out_file_extension_check,
                                                                          previous_output_message=output_message,
                                                                          negative_output_message=' - формат выходного файла должен быть .xlsx')

    # TODO здесь продолжить проверки

    # здесь закончились провеерки
    #########################################################################################

    all_checks_passed = go_to_next_check
    return all_checks_passed, output_message


def get_table_from_clipboard(args):
    gtins = args.gtins
    attributes = args.attributes
    no_valueMap = args.no_valueMap
    verbose = args.verbose
    chunk = args.chunk

    all_checks_passed, output_message = preliminary_check_set(args)  ###################################################################################################################
    print('', )
    if all_checks_passed:
        print(output_message)

        input_df = pd.DataFrame(columns=attributes)
        input_df['GTIN']=gtins
        gs1_request = gs1_requester(source_df=input_df, get_valueMap=operator.not_(no_valueMap), verbose_result=verbose)
        output_df = gs1_request.batch_requester(chunk=chunk)
        print('\nтекущий результат:\n{}'.format(output_df.to_string(index=False)))

    else:
        print('В процессе предварительных проверок обнаружены ошибки:\n')
        print(output_message)


def get_table_from_file(args): # in_file, out_file
    all_checks_passed, output_message = preliminary_check_set(args)  ###################################################################################################################
    print('', )
    if all_checks_passed:
        print(output_message)
        print('\n' + '-' * 20)
        input_file_full_path = args.i.name
        output_file_full_path = args.o.name
        eav = args.eav
        no_valueMap = args.no_valueMap
        verbose_result = args.verbose
        chunk = args.chunk
        printout_result = args.print

        input_df = pd.read_excel(input_file_full_path, dtype={'GTIN': object})
        gs1_request = gs1_requester(source_df=input_df, get_valueMap=operator.not_(no_valueMap), verbose_result=verbose_result)

        if eav:
            output_df = gs1_request.batch_requester_eav_mode(chunk=chunk)
        else:
            output_df = gs1_request.batch_requester(chunk=chunk)
        output_df.to_excel(output_file_full_path, sheet_name='sheet_1', index=False)

        if printout_result:
            print('\nтекущий результат:\n{}'.format(output_df.to_string(index=False)))

        print('\nФайл {} записан'.format(output_file_full_path))

    else:
        print('В процессе предварительных проверок обнаружены ошибки:\n')
        print(output_message)


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=RawDescriptionHelpFormatter, description=general_description)
    subparsers = parser.add_subparsers(dest='subparser_name')

    ############################################################################
    subparser_from_file = subparsers.add_parser('file')

    subparser_from_file.add_argument('-i', metavar="in_file", nargs='?',
                        type=argparse.FileType('r'),  # str , #argparse.FileType('r'),                     #encoding='Windows-1252'), # Windows-1252 encoded XLSX file, , encoding='latin-1'
                        # action="store",
                        default=sys.stdin,  # argparse.FileType('r'),
                        help='''Входной файл с двумя обязательными столбцами \"GTIN\" и \"GS1Attr\". Если файл лежит в той же директории, что и исполняемый файл, то можно указать только его название ''',
                        required=False)
    subparser_from_file.add_argument('-o', metavar="out_file",
                        type=argparse.FileType('w'),
                        default=sys.stdout,
                        help='''Выходной файл. Путь допустимо указывать как абсолютный, так и относительный. Если файл лежит в той же директории, что и исполняемый файл, то можно указать только его название''')  # dest="output_file", default='out_file.xlsx',
    subparser_from_file.add_argument("-p", "--print", help="print result in current window", action='store_true')  # ,,
    subparser_from_file.add_argument("-novm", "--no_valueMap", help="отключает запрос в БД для получения valueMap", action='store_true', )
    subparser_from_file.add_argument("-v", "--verbose", help="отображает процесспарсинга GS1", action='store_true')
    subparser_from_file.add_argument("-e", "--eav", help="входной файл в EAV формате, т.е. содержит два столбца 'GTIN' и 'GS1Attr'", action='store_true')
    subparser_from_file.add_argument("-c", "--chunk", help="размер чанка (порции выгрузки)", default=50, type=int)
    subparser_from_file.set_defaults(func=get_table_from_file)

    #############################################################################
    subparser_from_clipboard = subparsers.add_parser('clipboard')

    subparser_from_clipboard.add_argument('-gtn', '--gtins', nargs='+', help='список gtin через пробел', type=int, required=False)
    subparser_from_clipboard.add_argument('-att', '--attributes', nargs='+', help='список GS1Attr через пробел', required=False)

    subparser_from_clipboard.add_argument("-novm", "--no_valueMap", help="отключает запрос в БД для получения valueMap", action='store_true', )
    subparser_from_clipboard.add_argument("-v", "--verbose", help="отображает процесспарсинга GS1", action='store_true', required=False)
    subparser_from_clipboard.add_argument("-c", "--chunk", help="размер чанка (порции выгрузки)", default=50, type=int)

    subparser_from_clipboard.set_defaults(func=get_table_from_clipboard)

    return parser.parse_args()

def main():
    args = parse_args()
    args.func(args)

if __name__ == "__main__":
    main()

'''
if __name__ == "__main__":
    x = cli_class()
    x.eav_file_generator()
'''
