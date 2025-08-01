grammar TblCalcField;

calc_field : expr EOF;

expr
    : date_literal
    | LEFT_PAREN expr RIGHT_PAREN
    | expr CARET expr
    | expr AIRTHEMATIC_OPERATION expr
    | expr LOGICAL_OPERATION expr
    | NOT expr
    | expr (AND|OR|IN) expr
    | case_expr
    | if_expr
    | iif_expr
    | function_expr
    | lod_expr
    | field_expr
    | boolean_literal
    | numeric_literal
    | string_literal
    ;

case_expr
    : CASE case_term when_expr+ ELSE expr END
    ;

case_term
    : expr
    ;

when_expr
    : WHEN expr THEN expr
    ;

if_expr
    : IF expr THEN expr elseif_expr* else_expr? END
    ;

elseif_expr
    : ELSEIF expr THEN expr
    ;

else_expr
    : ELSE expr
    ;
        
iif_expr
    : IIF LEFT_PAREN expr COMMA expr COMMA expr (COMMA expr)? RIGHT_PAREN
    ;

function_expr
    : function_name LEFT_PAREN (expr (COMMA expr)*)? RIGHT_PAREN
    ;

function_name
    : 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'COUNT' | 'ISDATE' | 'ZN' | 'IFNULL' | 'DATE' | 'REPLACE'
    | 'SPLIT' | 'PERCENTILE' | 'LOOKUP' | 'DIV' | 'DATEADD' | 'DATEDIFF' | 'DATENAME' | 'TODAY'
    | 'ABS' | 'ACOS' | 'ASIN' | 'ATAN' | 'ATAN2' | 'CEILING' | 'COS' | 'DIV' | 'EXP' | 'FLOOR' | 'LN'
    | 'MAX' | 'POWER' | 'ROUND' | 'SIGN' | 'SIN' | 'SQRT' | 'SQUARE' | 'TAN' | 'LEFT' | 'LEN'
    | 'MIN' | 'DATE' | 'IFNULL' | 'COUNTD' | 'MEDIAN' | 'STDEV' | 'VAR' | 'RANK'
    | 'RUNNING_AVG' | 'RUNNING_COUNT' | 'RUNNING_MAX' | 'RUNNING_MIN' | 'RUNNING_SUM' | 'ISNULL' 
    | 'LOG' | 'DAY' | 'MONTH' | 'NOW' | 'TODAY' | 'YEAR' | 'DATE' | 'MID' | 'COUNTD'
    ; //  Add more function names

lod_expr
    : LEFT_BRACKET lod_type=(FIXED | INCLUDE | EXCLUDE)? (lod_dim (COMMA lod_dim)*)? COLON? lod_aggr RIGHT_BRACKET
    ;

lod_dim
    : expr
    ;

lod_aggr
    : expr
    ;

field_expr
    : LEFT_SQ_BRACKET lod_expr RIGHT_SQ_BRACKET
    | field_literal
    ;

field_literal
    : FIELD
    ;

boolean_literal
    : 'TRUE' | 'FALSE'
    ;

numeric_literal
    : NUMBER
    ;

string_literal
    : STRING
    ;

NUMBER
    : DIGITS
    | '.' DIGITS
    | DIGITS '.' DIGITS
    ;

STRING
    : SINGLE_QUOTE .*? SINGLE_QUOTE
    | DOUBLE_QUOTE .*? DOUBLE_QUOTE
    ;

date_literal
    : TBL_DATE
    ;

COMMENT: '/*' .*? '*/' -> skip;

LINE_COMMENT: '//' .*? ('\n' | EOF)  -> skip;

LEFT_PAREN: '(';
RIGHT_PAREN: ')';
LEFT_BRACKET: '{';
RIGHT_BRACKET: '}';
LEFT_SQ_BRACKET: '[';
RIGHT_SQ_BRACKET: ']';        
CARET: '^';
COMMA: ',';
COLON: ':';
fragment SINGLE_QUOTE: '\'';
fragment DOUBLE_QUOTE: '"';
LOGICAL_OPERATION
    : '=='
    | '='
    | '>'
    | '<'
    | '>='
    | '<='
    | '!='
    | '<>'
    ;
AIRTHEMATIC_OPERATION
    : '+'
    | '-'
    | '/'
    | '*'
    | '%'
    ;
NOT: [nN][oO][tT];
AND: 'AND';
OR: 'OR';
IN: 'IN';
IF: 'IF';
THEN: 'THEN';
ELSE: 'ELSE';
ELSEIF: 'ELSEIF';
CASE: 'CASE';
WHEN: 'WHEN';
END: 'END';
IIF: 'IIF';
FIXED: 'FIXED';
INCLUDE: 'INCLUDE';
EXCLUDE: 'EXCLUDE';
DIGITS: [0-9]+;


// FIELD: '['.*?']';
// To disambiguate lod_expr inside a field, forcing first char after '['
// to NOT BE '{'.
// TODO(PankajK) Maybe a better way to handle this using rule precedence
FIELD: '[' ~'{'.*?']';

TBL_DATE: '#'.*?'#';

WHITESPACE: [ \r\n\t]+ -> skip;
