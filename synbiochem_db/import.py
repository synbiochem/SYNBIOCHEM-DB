'''
SYNBIOCHEM-DB (c) University of Manchester 2017

SYNBIOCHEM-DB is licensed under the MIT License.

To view a copy of this license, visit <http://opensource.org/licenses/MIT/>.

@author:  neilswainston
'''
# pylint: disable=invalid-name
import os
import shutil
import sys

from gg_utils.neo4j import utils
import pandas as pd
from synbiochem_db import xl_converter


def import_sts(xl_filename, neo4j_root):
    '''Import a sample tracking sheet.'''
    # Parse metadata:
    dir_name = xl_converter.convert(xl_filename)

    metadata_df = import_metadata(os.path.join(dir_name, 'Metadata.csv'))
    node_dfs, rels_dfs, plate_id = parse_metadata(metadata_df)

    # Parse strain:
    st_node_dfs, st_rels_dfs = \
        parse_strain(import_plate(os.path.join(dir_name, 'Strain.csv')),
                     plate_id)
    node_dfs.extend(st_node_dfs)
    rels_dfs.extend(st_rels_dfs)

    # Parse media:
    md_node_dfs, md_rels_dfs = parse_plate(import_plate(
        os.path.join(dir_name, 'Media.csv')), 'Media')
    node_dfs.extend(md_node_dfs)
    rels_dfs.extend(md_rels_dfs)

    # Parse treatment:
    trt_node_dfs, trt_rels_dfs = \
        parse_plate(import_plate(os.path.join(dir_name, 'Treatment.csv')),
                    'Treatment')
    node_dfs.extend(trt_node_dfs)
    rels_dfs.extend(trt_rels_dfs)

    # Parse ODS:
    trt_node_dfs, trt_rels_dfs = \
        parse_ods([import_plate(os.path.join(dir_name, 'OD induction.csv')),
                   import_plate(os.path.join(dir_name, 'OD harvest.csv'))])

    node_dfs.extend(trt_node_dfs)
    rels_dfs.extend(trt_rels_dfs)

    # Convert DataFrames to csv:
    node_files = _get_filenames(node_dfs, 'node')
    rels_files = _get_filenames(rels_dfs, 'rels')

    # Populate database:
    utils.create_db(neo4j_root, node_files, rels_files)

    # Clean-up:
    _clean_up(node_files, rels_files, dir_name)


def import_metadata(filename):
    '''Import metadata.'''
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
    return df


def parse_metadata(df):
    '''Parse metadata.'''
    rels = []

    # Get Experiment:
    exp_df = pd.DataFrame(df['Experiment Name/Revision'].values,
                          columns=['exp_id:ID'])
    exp_df[':LABEL'] = 'Experiment'

    # Get Person:
    person_df = pd.DataFrame(df['Experimentalist'].values,
                             columns=['name:ID'])
    person_df[':LABEL'] = 'Person'

    # Get Plate:
    plate_df = pd.DataFrame(df[['Analysis Plate Dilution Factor',
                                'Date Created (yymmdd)',
                                'Induction time',
                                'Lab Archives URL',
                                'Plate ID (yymmdd-inst-exp)',
                                'Technology Type (e.g. GCMS)',
                                'Temperature']].values,
                            columns=['dilution_factor',
                                     'date_created',
                                     'induction_time',
                                     'lab_archives_url',
                                     'plate_id:ID',
                                     'instrument',
                                     'temperature:float'])
    plate_df[':LABEL'] = 'Plate'

    # Get Chemical:
    chem_df = pd.DataFrame(df.filter(regex='Target').transpose())
    chem_df.columns = ['chebi_id:ID']
    chem_df[':LABEL'] = 'Chemical'

    # Get Relationships:
    rels.append(df[['Plate ID (yymmdd-inst-exp)',
                    'Experiment Name/Revision']].values[0].tolist() +
                ['PART_OF'])

    rels.append(df[['Plate ID (yymmdd-inst-exp)',
                    'Experimentalist']].values[0].tolist() +
                ['CREATED_BY'])

    for chebi_id in chem_df['chebi_id:ID']:
        rels.append(df[['Plate ID (yymmdd-inst-exp)']].values[0].tolist() +
                    [chebi_id, 'HAS_TARGET'])

    rels_df = pd.DataFrame.from_records(rels, columns=[':START_ID', ':END_ID',
                                                       ':TYPE'])

    plate_id = df[['Plate ID (yymmdd-inst-exp)']].values[0][0]
    return [exp_df, person_df, plate_df, chem_df], [rels_df], plate_id


def import_plate(filename):
    '''Import plates.'''
    df = pd.read_csv(filename, usecols=range(13))[:8]
    df.set_index(df.columns[0], inplace=True)
    return df


def parse_strain(df, plate_id):
    '''Parse strain.'''
    plate_df = _get_plate_df(df, 'Replicate')

    replicate_rels_df = pd.DataFrame(
        plate_df['loc_well:ID'], columns=['loc_well:ID'])
    replicate_rels_df.columns = [':END_ID']
    replicate_rels_df[':START_ID'] = plate_id
    replicate_rels_df[':TYPE'] = 'CONTAINS'

    sample_df = pd.DataFrame(columns=['host', 'plasmid', 'replicate'])
    sample_df[sample_df.columns] = \
        plate_df['id'].str.split('_', expand=True)
    sample_df['loc_well:ID'] = plate_df['loc_well:ID']
    sample_df.drop('replicate', axis=1, inplace=True)

    host_df = pd.DataFrame(sample_df['host'], columns=['host'])
    host_df.columns = ['host_id:ID']
    host_df.drop_duplicates(inplace=True)
    host_df[':LABEL'] = 'Host'

    plasmid_df = pd.DataFrame(sample_df['plasmid'], columns=['plasmid'])
    plasmid_df.columns = ['plasmid_id:ID']
    plasmid_df.drop_duplicates(inplace=True)
    plasmid_df[':LABEL'] = 'Plasmid'

    host_rels_df = pd.DataFrame(sample_df[['host', 'loc_well:ID']],
                                columns=['host', 'loc_well:ID'])
    host_rels_df.columns = [':START_ID', ':END_ID']
    host_rels_df[':TYPE'] = 'FOUND_IN'

    plasmid_rels_df = pd.DataFrame(sample_df[['plasmid', 'loc_well:ID']],
                                   columns=['plasmid', 'loc_well:ID'])
    plasmid_rels_df.columns = [':START_ID', ':END_ID']
    plasmid_rels_df[':TYPE'] = 'FOUND_IN'

    return [plate_df, host_df, plasmid_df], \
        [replicate_rels_df, host_rels_df, plasmid_rels_df]


def parse_plate(df, label):
    '''Parse plate data.'''
    plate_df = _get_plate_df(df, label)

    plate_vals_df = pd.DataFrame(plate_df['id'])
    plate_vals_df.columns = ['id:ID']
    plate_vals_df.drop_duplicates(inplace=True)
    plate_vals_df[':LABEL'] = label

    plate_vals_rels_df = pd.DataFrame(plate_df[['id', 'loc_well:ID']],
                                      columns=['id', 'loc_well:ID'])
    plate_vals_rels_df.columns = [':START_ID', ':END_ID']
    plate_vals_rels_df[':TYPE'] = 'FOUND_IN'

    return [plate_vals_df], [plate_vals_rels_df]


def parse_ods(dfs, timepoints=None):
    '''Parse OD data.'''
    if not timepoints:
        timepoints = ['induction', 'harvest']

    data_dfs = [_get_plate_df(df, 'OD', 'value:float') for df in dfs]
    data_rels_dfs = []

    for data_df, timepoint in zip(data_dfs, timepoints):
        data_df['id:ID'] = data_df['loc_well:ID'] + '_OD_' + timepoint
        data_df['timepoint'] = timepoint
        data_df['unit'] = 'unitless'

        data_rels_df = pd.DataFrame(data_df[['id:ID', 'loc_well:ID']],
                                    columns=['id:ID', 'loc_well:ID'])
        data_rels_df.columns = [':END_ID', ':START_ID']
        data_rels_df[':TYPE'] = 'HAS_DATA'
        data_rels_dfs.append(data_rels_df)

        columns = ['loc_row', 'loc_col', 'loc_well:ID']
        data_df.drop(columns, axis=1, inplace=True)

    return data_dfs, data_rels_dfs


def _get_filenames(dfs, prefix):
    '''Get filenames from DataFrames.'''
    filenames = []

    for idx, node_df in enumerate(dfs):
        filename = prefix + str(idx) + '.csv'
        node_df.to_csv(filename, index=False)
        filenames.append(filename)

    return filenames


def _get_plate_df(df, label, val_col='id'):
    '''Get plate DataFrame.'''
    samples = [zip(df.index.values,
                   [col for _ in range(len(df.index))],
                   df[col].values)
               for col in df.columns]

    plate_df = pd.DataFrame([pos for col in samples for pos in col],
                            columns=['loc_row', 'loc_col', val_col])

    plate_df.dropna(how='any', inplace=True)
    plate_df[':LABEL'] = label
    plate_df['loc_well:ID'] = \
        plate_df['loc_row'] + plate_df['loc_col']

    return plate_df


def _clean_up(node_files, rels_files, dir_name):
    '''Clean-up tempfiles.'''
    for fle in node_files + rels_files:
        os.remove(fle)

    shutil.rmtree(dir_name)


def main(args):
    '''main method.'''
    import_sts(*args)


if __name__ == '__main__':
    main(sys.argv[1:])
