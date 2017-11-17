'''
SYNBIOCHEM-DB (c) University of Manchester 2017

SYNBIOCHEM-DB is licensed under the MIT License.

To view a copy of this license, visit <http://opensource.org/licenses/MIT/>.

@author:  neilswainston
'''
import csv
import os
import sys

import xlrd


def convert(xl_filename):
    '''Convert Excel file.'''
    dir_name, _ = os.path.splitext(xl_filename)

    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    workbook = xlrd.open_workbook(xl_filename)

    for sheet in workbook.sheets():
        csv_filename = os.path.join(dir_name, sheet.name + '.csv')

        with open(csv_filename, 'wb') as csv_file:
            writer = csv.writer(csv_file)

            for rownum in xrange(sheet.nrows):
                writer.writerow(sheet.row_values(rownum))

            csv_file.close()

    return dir_name


def main(args):
    '''main method.'''
    print convert(args[0])


if __name__ == '__main__':
    main(sys.argv[1:])
