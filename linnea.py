'''
Created on 13.08.2015

@author: Tobias Ruck
'''
from __future__ import print_function

from linnea_parser import SQLCompiler
from datetime import datetime
from string import Template
import os.path
import time


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
timestamp_file_format = '%Y-%m-%d-%H-%M-%S'

def compile_source(src, timestamp, with_group_by):
    t_to_str   = timestamp.strftime(timestamp_format)
    imap = dict(identifier_map)
    imap['t0'] = "(TIMESTAMP '%s')" % t_to_str
    
    compiler = SQLCompiler(table_name, imap, function_map, with_group_by)
    
    return compiler.compileSQL(src)

def main(filename, timestamp=None, with_group_by=with_group_by, execute=False):
    if filename == 'batch':
        batch_execute('examples', True)
        return 
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

def batch_execute(directory, with_group_by=with_group_by):
    import pytoml
    import pyodbc
    
    config = pytoml.loads(open('config.toml').read())
    
    files = config['batch']['dgas']
    days = config['batch']['days']
    hours = config['batch']['hours']
            
    class FileAndStdout():
        def __init__(self, filename):
            self.f = open(filename, 'w')
            self.stdout = sys.stdout
            
        def write(self, s):
            self.stdout.write(s)
            self.f.write(s)
            
        def flush(self):
            self.stdout.flush()
            self.f.flush()
            
    sys.stdout = FileAndStdout('preformance.txt')
            
    from numpy import std, max, min, mean, array
    
    save_results = True
    
    odbc_connection_template = Template(config['odbc']['connect_template'])
    odbc_connection_string = odbc_connection_template.substitute(**config['odbc'])
    connection = pyodbc.connect(odbc_connection_string)
    
    cur = connection.cursor()
    total_results = {}
    total_exec_times = []
    
    for f in files:
        dga_name = f.title()
        print('-'*79)
        filepath = '%s/%s.linn' % (directory, f)
        print('Running', dga_name, '(file', filepath, ')')
        src = open(filepath).read()
        exec_times = []
        for day in days:
            print('Running for the', day)
            if save_results:
                result_file = open('results/%s-%s.txt' % (dga_name, day), 'w')
            result_set = set()
            for hour in hours:
                t = datetime.strptime("%s %s" % (day, hour), timestamp_format)
                sql_query = compile_source(src, t, with_group_by)
                
                cur = connection.cursor()
                t0 = time.time()
                cur.execute(sql_query)
                dt = (time.time() - t0)
                print('%.2fs' % dt, end=' ')
                
                if save_results:
                    print('--------- At %s ---------' % hour, file=result_file)
                    for row in cur:
                        results = list(row)
                        print(*results, sep='\t| ', file=result_file)
                        result_set.add(results[0])
            
                exec_times.append(dt)
                total_exec_times.append(dt)
            if save_results:
                print('*'*40)
                print('-------- Aggregated: n = %d --------' % len(result_set), file=result_file)
                print('\n'.join(sorted(result_set)), file=result_file)
                result_file.close()
            
            for r in result_set:
                total_results.setdefault(r, []).append(dga_name)
            print()
        print('***********')
        print('RESULTS FOR', dga_name, 'EXECUTION TIME')
        numarray = array(exec_times)
        print('Max:\t', max(numarray))
        print('Min:\t', min(numarray))
        print('Mean:\t', mean(numarray))
        print('Std deriv:\t', std(numarray))
        
    print('***********')
    print('TOTAL EXECUTION TIME')
    numarray = array(total_exec_times)
    print('Max:\t', max(numarray))
    print('Min:\t', min(numarray))
    print('Mean:\t', mean(numarray))
    print('Std deriv:\t', std(numarray))
        
    print('All results:', len(total_results))
    print( '\n'.join( '%s: %s' % (ip, ', '.join(dgas)) for ip, dgas in sorted(total_results.items())) )

if __name__ == '__main__':
    import sys
    main(*sys.argv[1:])