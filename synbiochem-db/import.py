'''
Created on 15 Nov 2017

@author: neilswainston
'''
import sys

from gg_utils.neo4j import utils
import pandas as pd


def import_dts(filename):
    '''Import a data tracking sheet.'''

    values = {}
    with open(filename, 'rU') as fle:
        in_options = False

        for line in fle:
            tokens = [val for val in line.strip().split(',') if val]

            if len(tokens) > 1:
                if 'Experimentalists List' in tokens:
                    in_options = True

                if in_options:
                    continue
                else:
                    values[tokens[0]] = tokens[1]

    df = pd.DataFrame(values, index=[0])
    df.to_csv('raw.csv', index=False)
    return df


def get_neo4j_csvs(df):
    '''Get Neo4j csvs.'''
    exp_df = pd.DataFrame(df['Experiment Name/Revision'].values,
                          columns=['exp_id:ID'])
    exp_df[':LABEL'] = 'Experiment'

    person_df = pd.DataFrame(df['Experimentalist'].values,
                             columns=['name:ID'])
    person_df[':LABEL'] = 'Person'

    plate_df = pd.DataFrame(df[['Analysis Plate Dilution Factor',
                                'Date Created (yymmdd)',
                                'Induction time',
                                'Lab Archives URL',
                                'Number of Well Plates',
                                'Plate ID (yymmdd-inst-exp)',
                                'Technology Type (e.g. GCMS)',
                                'Temperature']].values,
                            columns=['dilution_factor',
                                     'date_created',
                                     'induction_time',
                                     'lab_archives_url',
                                     'num_well_plates',
                                     'plate_id:ID',
                                     'instrument',
                                     'temperature'])
    plate_df[':LABEL'] = 'Plate'

    return [exp_df, person_df, plate_df], []


def main(args):
    '''main method.'''
    df = import_dts(args[0])
    node_dfs, rel_dfs = get_neo4j_csvs(df)
    node_files = []
    rels_files = []

    for idx, node_df in enumerate(node_dfs):
        filename = 'node' + str(idx) + '.csv'
        node_df.to_csv(filename, index=False)
        node_files.append(filename)

    utils.create_db(args[1], node_files, rels_files)


if __name__ == '__main__':
    main(sys.argv[1:])
