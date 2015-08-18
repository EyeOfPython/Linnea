'''
Created on 13.08.2015

@author: Tobias Ruck
'''
from __future__ import print_function

from linnea_parser import SQLCompiler
from datetime import datetime
from string import Template


table_name = 'hplDNSReplies'

identifier_map = { 
    't0':       "(TIMESTAMP '2015-08-03 00:00:00')",
    'domain':   'request',
    'client':   'dst',
    'timestamp':'timestamp'
}

function_map = { 
    'match':    ['(REGEXP_INSTR(',0,',',1,')>0)'],
    'count':    ['REGEXP_COUNT(',0,',',1,')']
}

with_group_by = True

timestamp_format = '%Y-%m-%d %H:%M:%S'

def compile_source(src, timestamp, with_group_by):
    t_to_str   = timestamp.strftime(timestamp_format)
    imap = dict(identifier_map)
    imap['t0'] = "(TIMESTAMP '%s')" % t_to_str
    
    compiler = SQLCompiler(table_name, imap, function_map, with_group_by)
    
    return compiler.compileSQL(src)

def main(filename, timestamp=None, with_group_by=with_group_by, execute=False):
    source = open(filename).read()
    
    if not timestamp:
        timestamp = datetime.now()
    else:
        timestamp = datetime.strptime(timestamp, timestamp_format)
    
    sql_query = compile_source(source, timestamp, int(with_group_by))
    
    if not int(execute):
        print(sql_query)
    else:
        import pytoml
        import pyodbc
        config = pytoml.loads(open('config.toml').read())
        
        odbc_connection_template = Template(config['odbc']['connect_template'])
        odbc_connection_string = odbc_connection_template.substitute(**config['odbc'])
        connection = pyodbc.connect(odbc_connection_string)
        cur = connection.cursor()
        cur.execute(sql_query)
        
        print('-'*79)
        for row in cur:
            print(*row, sep='\t| ')

if __name__ == '__main__':
    import sys
    main(*sys.argv[1:])