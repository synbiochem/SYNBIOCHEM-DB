'''
SYNBIOCHEM-DB (c) University of Manchester 2018

SYNBIOCHEM-DB is licensed under the MIT License.

To view a copy of this license, visit <http://opensource.org/licenses/MIT/>.

@author:  neilswainston
'''
import sys

from neo4j.v1 import GraphDatabase


class Writer(object):
    '''Class to write to SYNBIOCHEM-DB.'''

    def __init__(self, uri, user, password):
        self.__driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        '''Close.'''
        self.__driver.close()

    def write(self, obj):
        '''Write designs.'''
        with self.__driver.session() as session:
            return session.write_transaction(_write, obj)


def _write(trx, obj, idx=-1, rel=None, parent=None):
    '''Write an object recursively.'''
    label = obj.pop('label')

    props = '{' + \
        ','.join([key + ':\'' + str(value) + '\''
                  for key, value in obj.iteritems()
                  if not isinstance(value, list)]) + \
        '}'

    node_identity = label + ' ' + props

    trx.run('MERGE (n:' + node_identity + ') RETURN n')

    if parent:
        trx.run(
            'MATCH (par: ' + parent + '), (chl: ' + node_identity + ') '
            'WITH par, chl '
            'MERGE (par)-[rel:' + rel + ' {index: $idx}]->(chl) '
            ' RETURN par, rel, chl', idx=idx)

    for key, value in obj.iteritems():
        if isinstance(value, list):
            for val_idx, val in enumerate(value):
                _write(trx, val, val_idx, key, node_identity)


def main(args):
    '''main method.'''
    wrt = Writer(*args)

    project = {'id': 'Project 1',
               'name': 'Project 1',
               'label': 'Project',
               'contains': [
                   {'id': 'Design 1',
                    'label': 'Design',
                    'contains': [
                        {
                            'id': 'Plasmid 1',
                            'label': 'Plasmid',
                            'built_from': [
                                {
                                    'id': 'Part 1',
                                    'label': 'Part'
                                },
                                {
                                    'id': 'Part 2',
                                    'label': 'Part'
                                }
                            ]
                        },
                        {
                            'id': 'Plasmid 2',
                            'label': 'Plasmid',
                            'built_from': [
                                {
                                    'id': 'Part 3',
                                    'label': 'Part'
                                },
                                {
                                    'id': 'Part 4',
                                    'label': 'Part'
                                }
                            ]
                        }
                    ]
                    }
               ]
               }

    wrt.write(project)

    plate = {'id': 'Plate 1',
             'name': 'Plate 1',
             'label': 'Plate',
             'contains': [
                 {
                     'row': 'A',
                     'col': 1,
                     'label': 'Well',
                     'holds': [
                              {
                                  'id': 'Plasmid 8',
                                  'label': 'Plasmid',
                                  'contains': [
                                      {
                                          'id': 'Part 3',
                                          'label': 'Part'
                                      }
                                  ]
                              }
                     ]
                 },
             ]
             }

    wrt.write(plate)


if __name__ == '__main__':
    main(sys.argv[1:])
