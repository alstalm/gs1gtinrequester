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




def get_table_from_clipboard(args):
    gtins = args.gtins
    attributes = args.attributes

    print('gtins = ->{}<- and type = {}'.format(gtins, type(gtins)))
    print('attributes = ->{}<- and type = {}'.format(attributes, type(attributes)))

def eav_file_generator(args): # in_file, out_file
    print('\n' + '-' * 20)

    #print('parsing has started \n')

    input_df = pd.read_excel(self.input_file_full_path, dtype={'GTIN': object})
    input_df = input_df.loc[:, ['GTIN', 'GS1Attr']]

    output_df = one_by_one_requester(source_df=input_df)
    output_df.to_excel(self.output_file_full_path, sheet_name='sheet_1', index=False)

    if self.printout_result:
        print('\nfile has successfully written. \n\ncurrent result:\n{}'.format(output_df.to_string(index=False)))
    else:
        pass # print('\nfile has successfully written')

    if self.verbose_result:

        print('\nhello from verbose inside eav_file_generator')
    else:
        pass #

    print('\nfile has successfully written')


def parse_args():


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



    subparser2 = subparsers.add_parser('clipboard')

    subparser2.add_argument('-gtn', '--gtins', nargs='+', help='список gtin через пробел', required=False)
    subparser2.add_argument('-att', '--attributes', nargs='+', help='список GS1Attr через пробел', required=False)

    '''
    input_file_full_path = args.i.name
    output_file_full_path = args.o.name
    printout_result = args.print
    verbose_result = args.verbose
    no_valueMap = args.no_valueMap
    eav = args.eav
    chunk = args.chunk
    gtins = args.gtins
    attributes = args.attributes
    '''
    subparser2.set_defaults(func=get_table_from_clipboard)

    return parser.parse_args()

def main():
    args = parse_args()
    args.func(args)








if __name__ == "__main__":
    inp = 'data/test_eav_long.xlsx'
    outp = 'data/result.xlsx'
    args = argparse.Namespace(input_file_full_path=inp, output_file_full_path =outp, printout_result = True,verbose_result = True, no_valueMap = False, eav = True, chunk=1)





