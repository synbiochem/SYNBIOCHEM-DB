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

    def write_project(self, name):
        '''Write project.'''
        with self.__driver.session() as session:
            return session.write_transaction(_write_project, name)

    def write_designs(self, designs):
        '''Write designs.'''
        with self.__driver.session() as session:
            return session.write_transaction(_write_designs, designs)


def _write_project(trx, name):
    return trx.run('MERGE (n:Project {name: $name}) '
                   'RETURN n', name=name)


def _write_designs(trx, designs):
    for design in designs['designs']:
        _write_design(trx, design, designs['project'])


def _write_design(trx, design, proj_name):
    trx.run('MERGE (d:Design {id: $des_id})<-[:CONTAINS]-(p:Project '
            '{name: $proj_name}) '
            'RETURN d, p', des_id=design['id'], proj_name=proj_name)

    for plasmid in design['plasmids']:
        _write_plasmid(trx, plasmid, design['id'])


def _write_plasmid(trx, plasmid, des_id):
    trx.run('MERGE (p:Plasmid {id: $plas_id})<-[:CONTAINS]-(d:Design '
            '{id: $des_id}) '
            'RETURN p, d', plas_id=plasmid['id'], des_id=des_id)

    for idx, part in enumerate(plasmid['parts']):
        _write_part(trx, part, idx, plasmid['id'])


def _write_part(trx, part, idx, plas_id):
    trx.run('MERGE (pt:Part {id: $part_id})<-[:CONTAINS '
            '{index: $idx}]-(pl:Plasmid '
            '{id: $plas_id}) '
            'RETURN pt, pl', part_id=part['id'], plas_id=plas_id, idx=idx)


def main(args):
    '''main method.'''
    wrt = Writer(*args)
    wrt.write_project('Alkaloids')

    designs = {'project': 'Alkaloids',
               'designs': [
                   {'id': 'Design 1',
                    'plasmids': [
                        {
                            'id': 'Plasmid 1',
                            'parts': [
                                {
                                    'id': 'Part 1'
                                },
                                {
                                    'id': 'Part 2'
                                }
                            ]
                        },
                        {
                            'id': 'Plasmid 2',
                            'parts': [
                                {
                                    'id': 'Part 3'
                                },
                                {
                                    'id': 'Part 4'
                                }
                            ]
                        }
                    ]
                    }
               ]
               }

    wrt.write_designs(designs)


if __name__ == '__main__':
    main(sys.argv[1:])
