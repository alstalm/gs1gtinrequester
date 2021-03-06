import pandas as pd
from df_creating import gs1_requester
from attributes_extractor import AtrrValueParesr
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# ОТКЛЮЧАЕТ ВОРНИНГИ НО ВОЗМОЖНО ЗАМЕДЛЯЕТ
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import argparse
from  argparse import RawDescriptionHelpFormatter
import operator


general_description = '''
Утилита парсинга GS1 Rus по методу GetItemByGTIN. 
--------------------------------------------------------------------------
 Перед запуском приложения в файле params.yaml необходимо задать параметры подключения к GS1 Rus и СУБД Нац.Каталога .
 В случае невозможности получения доступа к СУБД Национального Каталога, необходимо при запуске приложения указать параметр -n или --no_valueMap, который отключает запрос valueMap из БД НК.
 Если  параметр -n / --no_valueMap не передан при запуске, то, в случае наличия доступа к сУБД и существования для данного атрибута в таблице Lst_AttrToGS1Attr в столбце valueMap пары ключ-значение, \nв качестве значения возвращается соответствующее данному ключу значение.
 Если для value искомого атрибута не существует значения в valueMap, то в качестве значения возвращается значение параметра value из полученного XML.
Утилита не требует инсталляции и запуск осуществляется из из командной строки. При запуске утилиты необходимо указать точку получения входных данных: clipboard или file. 
Пример команды: .\gs1_gtin_parser.exe  clipboard -g 4660167673552 -a WEB_90001807.
С параметрами запуска приложения можно ознакомиться указав параметр -h. Пример команды  \gs1_gtin_parser.exe -h
В случае запуска приложения в режиме file к исходному и выходному файлам допустимо указывать как абсолютный путь (например D:\\test_out.xlsx) ,
так и относительный. Например ..\\test_out.xlsx (для записи в директорию выше уровнем). '''



def test_db_connection():
    # TODO добавить Tкy Except
    try:
        status, message = AtrrValueParesr.test_connection()
    except Exception as e:
        status = False
        message = str(e)
    return status, message


def check_output_file_extension(output_file_full_path):
    if output_file_full_path !=None:
        extension = str(output_file_full_path[(len(output_file_full_path) - 5):])
        if extension == '.xlsx':
            current_function_result = True
        else:
            current_function_result = False
    else:
        current_function_result = True

    return current_function_result


def check_input_file_format_eav(input_file_full_path):
    if input_file_full_path != None: # кейс, если в Numspase нет входного файла, а проверку провести надо
        input_df = pd.read_excel(input_file_full_path)
        header_list = list(input_df.columns.values)
        needed_columns = ['GTIN', 'GS1Attr']
        if header_list == needed_columns:
            current_function_result = True
        else:
            current_function_result = False

    else:
        current_function_result = True # считаем, что проверка пройдена. т.к. флаг скипнуть проверку отработает в пайплайне

    return current_function_result


def check_input_file_format_grid(input_file_full_path):
    if input_file_full_path != None:
        try:
            input_df = pd.read_excel(input_file_full_path)
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
    else:
        current_function_result = True # считаем, что проверка пройдена. т.к. флаг скипнуть проверку отработает в пайплайне
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

    try: # если вызван режим file, nо путь пропишется корректно
        input_file_full_path = args.in_file.name
        output_file_full_path = args.out_file.name
        eav = args.eav
    except: # если вызван режим clipboard, то в namespace данные переменные будут отсутсововать, поотому задаим для них None и в функциях проверки обработаем этот кейс
        input_file_full_path = None
        output_file_full_path = None
        eav = None


    no_valueMap = args.no_valueMap
    chunk = args.chunk

    # проерка соединения
    is_connected_to_database, connection_message = test_db_connection()
    go_to_next_check, output_message = preliminary_single_check(skip_this_check=no_valueMap,
                                                                          current_check_result=is_connected_to_database,
                                                                          negative_output_message=' - ' + connection_message)

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! - or input_from_clipboard
    # проверка формата для EAV
    input_eav_file_check = check_input_file_format_eav(input_file_full_path)
    go_to_next_check, output_message = preliminary_single_check(skip_this_check=(operator.not_(eav) or 'gtins' in dir()),  # если формат входного файла EAV - то НЕ пропускаем проверку
                                                                          previous_check_passed=go_to_next_check,
                                                                          current_check_result=input_eav_file_check,
                                                                          previous_output_message=output_message,
                                                                          negative_output_message=' - входной файл должен содержать два столбца с названиями: GTIN, GS1Attr')

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! - or input_from_clipboard
    # проверка формата для GRID
    input_grid_file_check = check_input_file_format_grid(input_file_full_path)
    go_to_next_check, output_message = preliminary_single_check(skip_this_check=(eav or 'gtins' in dir()),  # если формат входного файла EAV -  проверку ПРОПУСКАЕМ
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

    # проверка расширения выходного файла.  ЭТА ПРОВЕРКА  ДОЛЖНА БЫТЬ ПОСЛЕДНЕЙ. ЕЕ НЕЛЬЗЯ СКИПАТЬ!
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
        input_file_full_path = args.in_file.name
        output_file_full_path = args.out_file.name
        eav = args.eav
        no_valueMap = args.no_valueMap
        verbose_result = args.verbose
        chunk = args.chunk
        printout_result = args.print

        input_df = pd.read_excel(input_file_full_path, dtype={'GTIN': object})
        print('cli218: no_valueMap =', no_valueMap)
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

    subparser_from_file.add_argument("in_file", # metavar="in_file", nargs='?', # '-i',
                        type=argparse.FileType('r'),  # str , #argparse.FileType('r'),                     #encoding='Windows-1252'), # Windows-1252 encoded XLSX file, , encoding='latin-1'
                        #default=sys.stdin,  # argparse.FileType('r'),
                        help='''Входной файл. Путь допустимо указывать как абсолютный, так и относительный. Модель входных данных может быть представлена в двух видах: 
                        1. Grid (простая таблица с номерами gtin в столбец и запрашиваемыми параметрами представленными в названии столбцов. 
                        2. EAV (Entity Attribute Value) - исходные данные приведены в двух столбцах \"GTIN\" и \"GS1Attr\"'''
                        # ,required=False
                                     )
    subparser_from_file.add_argument("out_file", # metavar="out_file", '-o',
                        type=argparse.FileType('w'),
                        # default=sys.stdout,
                        help='''Выходной файл. Путь допустимо указывать как абсолютный, так и относительный. Если файл лежит в той же директории, что и исполняемый файл, то можно указать только его название. Разрешение фалйа должно быть .xlsx''')  # dest="output_file", default='out_file.xlsx',
    subparser_from_file.add_argument("-p", "--print", help="Вывести результат на экран", action='store_true')  # ,,
    subparser_from_file.add_argument("-n", "--no_valueMap", help="Не запрашивать маппинг (valueMap) значения атрибута из БД.", action='store_true', )
    subparser_from_file.add_argument("-v", "--verbose", help="Отображать текущий процесс парсинга GS1 RUS", action='store_true')
    subparser_from_file.add_argument("-e", "--eav", help="Входной файл в EAV формате, т.е. содержит два столбца 'GTIN' и 'GS1Attr'", action='store_true')
    subparser_from_file.add_argument("-c", "--chunk", help="Размер чанка (порции выгрузки). Не должен превышать 50", default=50,  type=int) # без ACTION добавляет строчными буквами
    subparser_from_file.set_defaults(func=get_table_from_file)
    #print('cli260: ',args.func)
    #############################################################################
    subparser_from_clipboard = subparsers.add_parser('clipboard')

    subparser_from_clipboard.add_argument('-g', '--gtins', nargs='+', help='Список gtin. Указываются через пробел', type=int, required=True)
    subparser_from_clipboard.add_argument('-a', '--attributes', nargs='+', help='Список GS1Attr. Указываются через пробел', required=True)

    subparser_from_clipboard.add_argument("-n", "--no_valueMap", help="Не запрашивать маппинг (valueMap) значения атрибута из БД.", action='store_true', )
    subparser_from_clipboard.add_argument("-v", "--verbose", help="Отображать текущий процесс парсинга GS1 RUS", action='store_true', required=False)
    subparser_from_clipboard.add_argument("-c", "--chunk", help="Размер чанка (порции выгрузки). Не должен превышать 50", default=50, type=int)

    subparser_from_clipboard.set_defaults(func=get_table_from_clipboard)

    return parser.parse_args()

def main():
    args = parse_args()
    args.func(args)

# ЭТОТ ФРАГМЕНТ ДЛЯ ОТЛАДКИ get_table_from_clipboard
if __name__ == "__main__":
    gtins = ['4660167673552']
    attributes = ['WEB_90001807']
    subparser_name = 'clipboard'
    args = argparse.Namespace(gtins=gtins, attributes =attributes, printout_result = True, verbose = True, no_valueMap = False,   chunk=50) #func=func,
    get_table_from_clipboard(args)