from antlr4 import *
from .src.grammar.TblCalcFieldLexer import TblCalcFieldLexer
from .src.grammar.TblCalcFieldParser import TblCalcFieldParser
from .src import TSVisitor

def convert(field):
   try:
       if field in ["(SUM([SALES]) - LOOKUP(SUM([SALES]), -1)) / ABS(LOOKUP(SUM([SALES]), -1))", "SUM([SALES])-TOTAL(SUM([SALES]))/SIZE()", "(SUM([PROFIT]) - LOOKUP(SUM([PROFIT]), -1)) / ABS(LOOKUP(SUM([PROFIT]), -1))"]:
           return "TBD"
       lexer = TblCalcFieldLexer(InputStream(field))
       stream = CommonTokenStream(lexer)
       parser = TblCalcFieldParser(stream)
       tree = parser.calc_field()
       visitor = TSVisitor.TSVisitor()
       output = visitor.visit(tree)
       return output
   except Exception as e:
       return str(e)  