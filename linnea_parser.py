'''
Created on 13.08.2015

@author: Tobias Ruck
'''

from __future__ import print_function

from pyparsing import Word, Regex, alphas, alphanums, Forward,\
    delimitedList, Optional, sglQuotedString, Literal, oneOf,\
    operatorPrecedence, opAssoc, Group, OneOrMore, infixNotation, ParserElement
from datetime import timedelta
ParserElement.enablePackrat()
import sys


class ParseContext(object):


    def __init__(self, lookup_table, function_table):
        self.layers = []
        self.current_layer = None
        self.current_sublayer = None
        self.current_sublayer_idx = 0
        self.current_mode = 'where'
        self.current_items = None
        self.lookup_table = lookup_table
        self.function_table = function_table
        self.define_table = {}
        
        self.mode_stack = []
        
        self.debug = False
        
        self.gen_idx = 0
        
        self.property_list = []
        
        self.used_columns = set()
    
    def emit(self, e):
        self.debug and print('emit', e)
        self.current_items[-1].append(e)
        
    def define(self, identifier, replacement):
        self.debug and print('define', identifier, replacement)
        self.define_table[identifier.id] = replacement
        
    def undefine(self, identifier):
        self.debug and print('undefine', identifier)
        del self.define_table[identifier.id]
        
    def lookup(self, identifier):
        self.debug and print('lookup', identifier)
        result = self.define_table.get(identifier.id, None)
        if result is None:
            if identifier.id == 'nxdomain':
                return "(cat='NXDOMAIN')"
            col = self.lookup_table.get(identifier.id, identifier.id)
            if identifier.id == 't0':
                return col
            self.used_columns.add(col)
            return col
        return result
        
    def get_function_template(self, identifier):
        return self.function_table[identifier.id]
        
    def down(self):
        self.debug and print('down')
        self.current_sublayer_idx += 1
        if len(self.current_layer) <= self.current_sublayer_idx:
            self.current_layer.append( {'select':[],'where':[]} )
        self.current_sublayer = self.current_layer[self.current_sublayer_idx]
        self.current_items = self.current_sublayer[self.current_mode]
        
    def up(self):
        self.debug and print('up')
        self.current_sublayer_idx -= 1
        if self.current_sublayer_idx < 0:
            raise ValueError()
        self.current_sublayer = self.current_layer[self.current_sublayer_idx]
        self.current_items = self.current_sublayer[self.current_mode]
        
    def generate_name(self, unique_properties=None):
        self.debug and print('generate name', unique_properties)
        recycle = False
        idx = self.gen_idx
        self.gen_idx += 1
        """if unique_properties is not None and False:
            try:
                idx = self.property_list.index(unique_properties)
            except:
                idx = len(self.property_list)
                self.property_list.append(unique_properties)
            else:
                recycle = True
        else:
            idx = len(self.property_list)
            self.property_list.append(None)"""
            
        return 'number_%d' % (idx), recycle
        
    def push_mode(self, mode):
        self.debug and print('push mode', mode)
        self.mode_stack.append(self.current_mode)
        self.current_mode = 'select'
        self.current_items = self.current_sublayer[self.current_mode]
        #if not self.current_items:
        #    self.current_items.append([])
        
    def pop_mode(self):
        self.debug and print('pop mode')
        self.current_mode = self.mode_stack.pop()
        self.current_items = self.current_sublayer[self.current_mode]
        #if not self.current_items:
        #    self.current_items.append([])
        
    def new_selected(self):
        self.debug and print('new selected')
        self.current_items.append([])
        
    def new_predicate(self):
        self.debug and print('new predicate')
        self.current_items.append([])
        
    def new_layer(self):
        self.debug and print('new layer')
        self.layers.append([{'select':[],'where':[]}])
        self.current_layer = self.layers[-1]
        self.current_sublayer_idx = 0
        self.current_sublayer = self.current_layer[self.current_sublayer_idx]
        self.current_items = self.current_sublayer[self.current_mode]
        self.property_list = []
        
class BuilderSQL():
    
    def __init__(self, layers, columns, table_name='hplDNSReplies', basis_columns=dict(domain='request',client='dst',timestamp='timestamp')):
        '''
        - layers: The sql hierarchy
        - columns: The columns used in predicates
        - sql_params: timeslot properties
        '''
        self.layers = layers
        self.columns = columns
        self.current_layer_depth = 0
        
        self.additional_rows = self.columns - set(basis_columns.values())
        self.additional_rows_str = ', '.join(sorted(self.additional_rows))
        if self.additional_rows_str:
            self.additional_rows_str = ', ' + self.additional_rows_str
            
        self.table_name = table_name
        self.basis_columns = basis_columns
        
    def build_sql(self, with_group_by=False, sql_params={'timeInterval':'<timeInterval>','hoursFrameBack':'<hoursFrameBack>','minutesFrameBack':'<minutesFrameBack>','hoursFrameForward':'<hoursFrameForward>','minutesFrameForward':'<minutesFrameForward>'}):
        sql = self.build_root_layer(self.layers[0])
        for layer in self.layers[1:]:
            sql = self.build_layer(layer, sql)
            
        if with_group_by:
            sql = ['SELECT {client}, COUNT({client}) AS freq'.format(**self.basis_columns), 'FROM (', sql, ') layer_group', 'GROUP BY {client}'.format(**self.basis_columns)]
            
        def join_recursive(sql, depth):
            return '\n'.join( '    '*depth + l if not isinstance(l, list) else join_recursive(l, depth + 1) for l in sql )
        
        sql_str = join_recursive(sql, 0)
        
        for p, r in sql_params.items():
            sql_str = sql_str.replace('<%s>' % p, r)
        
        return sql_str
    
    def build_root_layer(self, layer):
        if len(layer) > 1:
            raise ValueError('Lowest layer cannot contain any count for performance reasons.')
        root_template = [
        'SELECT {domain}, {client}{0}, MAX({timestamp}) AS {timestamp}'.format(self.additional_rows_str, **self.basis_columns),
        'FROM {0}'.format(self.table_name) ,
        #'    WHERE <timeStampBuilderForFinegrainTimeInterval>',
        'WHERE ',
        '@predicates',
        'GROUP BY dst, request%s' % self.additional_rows_str ]
        sublayer = layer[0]
        sql = []
        if sublayer['where']:
            predicates = [ '    ' + ''.join(sublayer['where'][0] ) ] + [ '    AND ' + ''.join(items) for items in sublayer['where'][1:] ]
        else:
            predicates = [ 'TRUE' ]
        for l in root_template:
            if l == '@predicates':
                sql.extend( predicates )
            else:
                sql.append(l)
        return sql
    
    def build_layer(self, layer, sql):
        for sublayer in reversed(layer):
            sql = self.build_sublayer(sublayer, sql)
        return sql
        
    def build_sublayer(self, sublayer, sql):
        select = ['SELECT {domain}, {client}{0}, {timestamp}'.format(self.additional_rows_str, **self.basis_columns)]
        for items in sublayer['select']:
            select[-1] += ','
            select.append( '    ' + ''.join(items) )
        
        predicates = []
        for items in sublayer['where']:
            if predicates:
                predicates.append('    AND ' + ''.join(items))
            else:
                predicates.append(''.join(items))
        
        if predicates:
            where = ['WHERE ' + predicates[0]] + predicates[1:]
        else:
            where = []
        sql = select + ['FROM ('] + [sql] + [') layer_%d' % self.current_layer_depth] + where
        
        self.current_layer_depth += 1
        
        return sql
        
class SQLCompiler():
    class Element():
        pass
    class DomainLevel(Element):
        def __init__(self, toks):
            self.level = int(toks[0][1:])
        def __repr__(self):
            return '<d:%s>' % self.level
        def visit(self, ctx):
            if self.level not in range(0,10):
                raise ValueError('domain level not in range 0-9')
            ctx.used_columns.add('d%s' % self.level)
            ctx.emit('d%d' % self.level)
            
    class DomainLevelLength(Element):
        def __init__(self, toks):
            self.level = int(toks[0][1:])
        def __repr__(self):
            return '<l:%s>' % self.level
        def visit(self, ctx):
            if self.level not in range(0,10):
                raise ValueError('domain level not in range 0-9')
            ctx.used_columns.add('d%s' % self.level)
            ctx.emit('LENGTH(d%d)' % self.level)
            
    class Identifier(Element):
        def __init__(self, toks):
            self.id = toks[0]
        def __repr__(self):
            return '<id:%s>' % self.id
        def visit(self, ctx):
            ctx.emit(ctx.lookup(self))
        
    class Integer(Element):
        def __init__(self, toks):
            self.value = int(toks[0])
        def __repr__(self):
            return '<i:%s>' % self.value
        def visit(self, ctx):
            ctx.emit(repr(self.value))
    
    class Float(Element):
        def __init__(self, toks):
            self.value = float(toks[0])
        def __repr__(self):
            return '<f:%s>' % self.value
        def visit(self, ctx):
            ctx.emit(repr(self.value))
    
    class String(Element):
        def __init__(self, s, l, toks):
            self.value = toks[0]
        def __repr__(self):
            return '<s:%r>' % self.value
        def visit(self, ctx):
            ctx.emit(self.value)
        
    class Boolean(Element):
        def __init__(self, toks):
            self.value = toks[0]
        def visit(self, ctx):
            ctx.emit(self.value)
            
    class Interval(Element):
        def __init__(self, toks):
            if len(toks) == 2:
                t = dict(h=0,m=0)
                t[toks[1]] = toks[0].value
            elif len(toks) == 4:
                t = dict(hours=toks[0].value,
                         minutes=toks[2].value)
                
            t = timedelta(hours=t['h'], minutes=t['m'])
            self.value = dict(h=int(t.total_seconds())//3600, m=(int(t.total_seconds())//60)%60)
        def __repr__(self):
            return '%s' % self.value
        def visit(self, ctx):
            ctx.emit("INTERVAL '{h} hour {m} minute'".format(**self.value))
    
    class FunctionCall(Element):
        def __init__(self, toks):
            self.func_name = toks[0]
            self.params = list(toks[1])
        def __repr__(self):
            return '%s(%s)' % (self.func_name.id, self.params)
        def visit(self, ctx):
            template = ctx.get_function_template(self.func_name)
            for t in template:
                if isinstance(t, int):
                    self.params[t].visit(ctx)
                else:
                    ctx.emit(t)
    
    class NumRange(Element):
        def __init__(self, toks):
            self.range_from = toks[0]
            self.range_to = toks[-1]
        def __repr__(self):
            return '<r:%s,%s>' % (self.range_from,self.range_to)
        def visit(self, ctx):
            pass # Only for 'in' statements
        def __iter__(self):
            return iter(range(self.range_from.value, self.range_to.value+1))
        def __len__(self):
            return self.range_to.value - self.range_from.value + 1
    
    class StringList(Element):
        def __init__(self, toks):
            self.items = list(item.value for item in toks)
        def __repr__(self):
            return '<sl:%s>' % (self.items,)
        def visit(self, ctx):
            pass # Only for 'in' statements
        def __iter__(self):
            return iter(self.items)
        def __len__(self):
            return len(self.items)
    
    class NumberList(Element):
        def __init__(self, toks):
            self.items = list(item.value for item in toks)
        def __repr__(self):
            return '<nl:%s>' % (self.items,)
        def visit(self, ctx):
            pass # Only for 'in' statements
        def __iter__(self):
            return iter(self.items)
        def __len__(self):
            return len(self.items)
        
    class InExpr(Element):
        def __init__(self, toks):
            self.left = toks[0]
            self.right = toks[1]
        def __repr__(self):
            return '%s in %s' % (self.left, self.right)
        def visit(self, ctx):
            ctx.emit('(')
            self.left.visit(ctx)
            ctx.emit(' IN (')
            for i,item in enumerate(self.right):
                ctx.emit(str(item))
                if i != len(self.right) - 1:
                    ctx.emit(',')
            ctx.emit('))')
        
    class CountExpr(Element):
        def __init__(self, toks):
            self.group = toks[0]
            self.pred = toks[-1]
            self.time_interval = None
            if len(toks) == 3:
                self.time_interval = toks[1] 
                
        def __repr__(self):
            return '#%s|%s#' % (self.group, self.pred)
        def visit(self, ctx):
            def recursiveDictify(o):
                if isinstance(o, (list, tuple)):
                    return [ recursiveDictify(e) for e in o ]
                elif isinstance(o, dict):
                    return { k: recursiveDictify(e) for k,e in o.items() }
                elif isinstance(o, SQLCompiler.Element):
                    return recursiveDictify(o.__dict__)
                else:
                    return o
            counter, recycle = ctx.generate_name(('count', recursiveDictify(list(self.group)), recursiveDictify(self.pred), recursiveDictify(self.time_interval)))
            ctx.emit(counter)
            if not recycle:
                ctx.down()
                ctx.push_mode('select')
                ctx.new_selected()
                ctx.emit('COUNT(')
                self.pred.visit(ctx)
                ctx.emit(' OR NULL) OVER(PARTITION BY ')
                for i,g in enumerate(self.group):
                    g.visit(ctx)
                    if i != len(self.group) - 1:
                        ctx.emit(',')
                t = self.time_interval
                if t:
                    ctx.emit(' ORDER BY timestamp RANGE BETWEEN ')
                    self.time_interval.visit(ctx)
                    ctx.emit(' PRECEDING AND ')
                    self.time_interval.visit(ctx)
                    ctx.emit(' FOLLOWING')
                ctx.emit(') AS ')
                ctx.emit(counter)
                ctx.up()
                ctx.pop_mode()
        
    class ForExpr(Element):
        def __init__(self, toks):
            self.iteratee = toks[0]
            self.iterator = toks[1]
            self.expr = toks[2]
        def __repr__(self):
            return '|%s in %s: %s|' % (self.iteratee, self.iterator, self.expr)
        def visit(self, ctx):
            ctx.emit('(')
            for i,item in enumerate(self.iterator):
                ctx.define(self.iteratee, str(item))
                ctx.emit('(CASE WHEN (')
                self.expr.visit(ctx)
                ctx.emit(') THEN 1 ELSE 0 END)')
                if i != len(self.iterator) - 1:
                    ctx.emit('+')
            ctx.undefine(self.iteratee)
            ctx.emit(')') # forfor
        
    class BinaryOp(Element):
        def __init__(self, toks):
            self.op = toks[0][1]
            self.left = toks[0][0]
            self.right = toks[0][2]
        def __repr__(self):
            return '%s %s %s' % (self.left, self.op, self.right)
        def visit(self, ctx):
            self.left.visit(ctx)
            ctx.emit(' '+self.op.upper()+' ')
            self.right.visit(ctx)
        
    class UnaryOp(Element):
        def __init__(self, toks):
            self.op = toks[0][0]
            self.right = toks[0][1]
        def __repr__(self):
            return '%s %s' % (self.op, self.right)
        def visit(self, ctx):
            ctx.emit(self.op + ' ')
            self.right.visit(ctx)
        
    class PredicateSet(Element):
        def __init__(self, toks):
            self.preds = toks
        def __repr__(self):
            return '{%s}' % (self.preds,)
        def visit(self, ctx):
            for pred in self.preds:
                ctx.new_predicate()
                pred.visit(ctx)
        
    class PredicateList(Element):
        def __init__(self, toks):
            self.preds = toks
        def __repr__(self):
            return '%s' % self.preds
        def visit(self, ctx):
            for pred in self.preds:
                ctx.new_layer()
                pred.visit(ctx)
        
    expression = Forward()
    
    domain_level = Regex('d[0-9]+')
    domain_level.setParseAction(DomainLevel)
    
    domain_level_length = Regex('l[0-9]+')
    domain_level_length.setParseAction(DomainLevelLength)
    
    identifier = Word(alphas, alphanums)
    identifier.setParseAction(Identifier)
    
    num_integer = Regex(r'-?(0|[1-9][0-9]*)')
    num_integer.setParseAction(Integer)
    num_float = Regex(r'-?(0|[1-9][0-9]*)\.[0-9]*')
    num_float.setParseAction(Float)
    number = num_float | num_integer
    
    l = lambda s: Literal(s).suppress()
    
    string = sglQuotedString
    string.addParseAction(String)
    
    true_val = Literal('true')
    true_val.setParseAction(Boolean)
    false_val = Literal('false')
    false_val.setParseAction(Boolean)
    
    param_list = Group(delimitedList(expression))
    function_call = identifier + l('(') + Optional(param_list) + l(')')
    function_call.setParseAction(FunctionCall)
    
    num_range = number + ',' + '...' + ',' + number
    num_range.setParseAction(NumRange)
    string_list = string + OneOrMore(l(',') + string)
    string_list.setParseAction(StringList)
    number_list = number + OneOrMore(l(',') + number)
    number_list.setParseAction(NumberList)
    enumeration = num_range | string_list | number_list
    
    h = number + Literal('h')
    m = number + Literal('m')
    time_interval = ( (h+m) | h | m )
    time_interval.setParseAction(Interval)
    count_expr = l('[') + Group(delimitedList(identifier)) + Optional(l(':') + time_interval) + l('|') + expression + l(']')
    count_expr.setParseAction(CountExpr)
    for_expr = l('|') + identifier + l('in') + enumeration + l(':') + expression + l('|')
    for_expr.setParseAction(ForExpr)
    
    in_expr = (domain_level | domain_level_length | identifier) + l('in') + enumeration
    in_expr.setParseAction(InExpr)
    
    value = for_expr | function_call | count_expr | in_expr | domain_level | domain_level_length | true_val | false_val | identifier | time_interval | string | number
    
    signop = oneOf('-')
    multop = oneOf('* /')
    plusop = oneOf('+ -')
    relop = oneOf('= != > >= < <=')
    notop = oneOf('not')
    andop = oneOf('and')
    orop = oneOf('or')
    infixNotation
    expression <<= operatorPrecedence( value, [
            (signop, 1, opAssoc.RIGHT, UnaryOp),
            (multop, 2, opAssoc.LEFT, BinaryOp),
            (plusop, 2, opAssoc.LEFT, BinaryOp),
            (relop, 2, opAssoc.LEFT, BinaryOp),
            (notop, 1, opAssoc.RIGHT, UnaryOp),
            (andop, 2, opAssoc.LEFT, BinaryOp),
            (orop, 2, opAssoc.LEFT, BinaryOp)
        ] )
    
    predicate_set = l('{') + delimitedList(expression) + l('}')
    predicate_set.setParseAction(PredicateSet)
    
    predicate_list = delimitedList(predicate_set)
    predicate_list.setParseAction(PredicateList)
    
    parser = predicate_list
    
    def __init__(self, table_name, identifier_map, function_map, with_group_by=False):
        self.table_name = table_name
        self.identifier_map = identifier_map
        self.function_map = function_map
        self.with_group_by = with_group_by
    
    def compileSQL(self, s):
        ctx = ParseContext(self.identifier_map, self.function_map)
        
        parsed = self.parser.parseString(s, parseAll=True)
        parsed[0].visit(ctx)
        return BuilderSQL(ctx.layers, ctx.used_columns, self.table_name, self.identifier_map).build_sql(with_group_by=self.with_group_by)
        
    @classmethod
    def test(cls):
        tests = ('d0', 'l0', 'domain', "'com'", "match(domain, '^[a-z]{4,12}\\.com$')", "100.10", "ex in 1,2,3",
                 "10+11", "-id", "10-10", "10*10", "9/11", "2.2",
                 "10+f(10*1)", "[dst,d1|l1 = 5]", "|suf in 'com','biz','net': suf|", "|i in 5,...,12: i|")
        for t in tests and []:
            print('-'*80)
            print('looking at', t)
            try:
                parsed = cls.expression.parseString(t, parseAll=True)
            except:
                print('got only', cls.expression.parseString(t), file=sys.stderr)
            else:
                ctx = ParseContext()
                parsed[0].visit(ctx)
                
        
        dgas = [
            ('Bedep', 
r"""
{
    match(domain, '^[a-z]{11,16}\.com$'),
    timestamp >= t0 - 2h,
    timestamp <= t0
},
{
    [dst:1h|match(d1,'[0-9]')] / [dst:1h|true] >= 0.2,
    [dst:60m|true] >= 18
}
"""),
            ('ConfickerAB',
r"""
{match(domain, '^[a-z]{5,12}\.(biz|com|info|net|org)$')},
{
    [dst|true] >= 25,
    |i in 5,...,12: [dst|l1=i]>=1| >= 5,
    |suffix in 'com','biz','info','net','org': [dst|d0=suffix]>=1| >= 4,
    [dst|l1=5 and d0 in 'com','info','net','org'] >= 1,
    [dst|l1=12 and d0 in 'com','info','net','org'] = 0
}
"""),
            ('Elephant',
r"""
{match(domain, '^[a-f0-9]{8}\.(com|info|net)$')},
{
    [dst| |suffix in 'com','info','net': [dst,d1|d0=suffix]>=1| >= 2 ] >= 16
}
""")
        ]
        
        identifier_map = { 
            't0':       "(TIMESTAMP '2015-08-03 00:00:00')",
            'domain':   'request',
            'client':   'dst'
        }
        
        function_map = { 
            'match':    ['(REGEXP_INSTR(',0,',',1,')>0)'],
            'count':    ['REGEXP_COUNT(',0,',',1,')']
        }
        
        for dga_name, s in dgas:
            print('-'*80)
            print('looking at', dga_name)
            print('with:')
            print(s)
            print('--')
            try:
                parsed = cls.parser.parseString(s, parseAll=True)
            except:
                print('got only', cls.parser.parseString(s), file=sys.stderr)
                raise
            else:
                print(parsed)
                ctx = ParseContext(identifier_map, function_map)
                parsed[0].visit(ctx)
                print( BuilderSQL(ctx.layers, ctx.used_columns).build_sql(with_group_by=True) )
    
if __name__ == '__main__':
    SQLCompiler.test()
    