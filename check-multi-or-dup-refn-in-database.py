#!/usr/bin/python3

'''
Read a RootsMagic database file (v7,v9) and report any person which has more
than one REFN fact or has a REFN value which is a duplicate of someone else.

Optionally a different event item could be selected.

Please run first on a test database or a copy of the true database.

This code is released under the MIT License: https://opensource.org/licenses/MIT
Copyright (c) 2023 John A. Andrea

Code is provided AS IS.
No support, discussion, maintenance, etc. is included or implied.
'''

import sys
import os
import sqlite3
import argparse


def get_version():
    return '1.2'


def get_program_options():
    results = dict()

    # defaults
    results['item'] = 'refn'
    results['version'] = False
    results['infile'] = None

    item_types = ['refn','exid','ssn','afn']

    arg_help = 'Report multiple or duplicate individual REFN values.'
    parser = argparse.ArgumentParser( description=arg_help )

    parser.add_argument('infile', type=argparse.FileType('r') )

    arg_help = 'Identifier fact to check. Options: ', item_types, ' Default:' + results['item']
    parser.add_argument( '--item', default=results['item'], type=str, help=arg_help )

    arg_help = 'Show the event values. Default: not selected'
    parser.add_argument( '--verbose', action='store_true', help=arg_help  )

    arg_help = 'Show version then exit.'
    parser.add_argument( '--version', action='version', version=get_version() )

    args = parser.parse_args()

    results['verbose'] = args.verbose
    results['infile'] = args.infile.name

    item = args.item
    if item.lower() in item_types:
       results['item'] = item

    return results


def from_name_table( db_file ):
    data = dict()

    try:
      conn = sqlite3.connect( db_file )
      cur = conn.cursor()

      sql = 'select OwnerId, Surname, Given, BirthYear, DeathYear'
      sql += ' from NameTable'
      sql += ' where NameType = 0 and IsPrimary > 0'

      cur.execute( sql )
      for row in cur:
          data[row[0]] = { 'surname':row[1], 'given':row[2], 'birth':row[3], 'death':row[4] }

      cur.close()

    except Exception as e:
      print( 'DBError:', e, str(e), file=sys.stderr )
    finally:
      if conn:
         conn.close()

    return data


def show_facts( db_file, fact, verbose, names ):
    has_trouble = False
    people = dict()
    values = dict()

    prefix = ''
    if verbose:
       prefix = 'Warning: '

    def get_name():
        name = str(p_id)
        if p_id in names:
           name = names[p_id]['surname'] + ', ' + names[p_id]['given']
           dates = str(names[p_id]['birth']) + '-' + str(names[p_id]['death'])
           if dates != '-':
              name += ' (' + dates + ')'
        return name

    try:
      conn = sqlite3.connect( db_file )
      cur = conn.cursor()

      sql = "select ownerid,details from eventtable where ownertype=0"
      sql += " and eventtype=(select FactTypeID from FactTypeTable where GedcomTag='" + fact +"')"

      cur.execute( sql )
      for row in cur:
          p_id = row[0]
          value = row[1]

          name = get_name()

          if verbose:
             print( name, '=>', value )

          if p_id in people:
             has_trouble = True
             people[p_id] += ',' + str(value)
             print( prefix + name, 'has multiple', fact, people[p_id] )
          else:
             people[p_id] = str(value)

          if value in values:
             has_trouble = True
             print( prefix + name, 'has duplicate', fact, value, 'with (at least)', values[value] )

          values[value] = name

      cur.close()

    except Exception as e:
      print( 'DBError:', e, str(e), file=sys.stderr )
    finally:
      if conn:
         conn.close()

    return has_trouble


options = get_program_options()

exit_code = 0

db_file = options['infile']
if db_file.lower().endswith( '.rmgc' ) or db_file.lower().endswith( '.rmtree' ):
   if os.path.isfile( db_file ):

      names = from_name_table( db_file )

      trouble = show_facts( db_file, options['item'].upper(), options['verbose'], names )

      if trouble:
         exit_code = 1

   else:
      print( 'File not found:', db_file, file=sys.stderr )
      exit_code = 1

else:
   print( 'Given file does not match RM name types:', db_file, file=sys.stderr )
   exit_code = 1

sys.exit( exit_code )
