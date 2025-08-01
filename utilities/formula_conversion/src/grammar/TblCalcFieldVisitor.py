# Generated from TblCalcField.g4 by ANTLR 4.13.1
from antlr4 import *
if "." in __name__:
    from .TblCalcFieldParser import TblCalcFieldParser
else:
    from TblCalcFieldParser import TblCalcFieldParser

# This class defines a complete generic visitor for a parse tree produced by TblCalcFieldParser.

class TblCalcFieldVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by TblCalcFieldParser#calc_field.
    def visitCalc_field(self, ctx:TblCalcFieldParser.Calc_fieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#expr.
    def visitExpr(self, ctx:TblCalcFieldParser.ExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#case_expr.
    def visitCase_expr(self, ctx:TblCalcFieldParser.Case_exprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#case_term.
    def visitCase_term(self, ctx:TblCalcFieldParser.Case_termContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#when_expr.
    def visitWhen_expr(self, ctx:TblCalcFieldParser.When_exprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#if_expr.
    def visitIf_expr(self, ctx:TblCalcFieldParser.If_exprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#elseif_expr.
    def visitElseif_expr(self, ctx:TblCalcFieldParser.Elseif_exprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#else_expr.
    def visitElse_expr(self, ctx:TblCalcFieldParser.Else_exprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#iif_expr.
    def visitIif_expr(self, ctx:TblCalcFieldParser.Iif_exprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#function_expr.
    def visitFunction_expr(self, ctx:TblCalcFieldParser.Function_exprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#function_name.
    def visitFunction_name(self, ctx:TblCalcFieldParser.Function_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#lod_expr.
    def visitLod_expr(self, ctx:TblCalcFieldParser.Lod_exprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#lod_dim.
    def visitLod_dim(self, ctx:TblCalcFieldParser.Lod_dimContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#lod_aggr.
    def visitLod_aggr(self, ctx:TblCalcFieldParser.Lod_aggrContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#field_expr.
    def visitField_expr(self, ctx:TblCalcFieldParser.Field_exprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#field_literal.
    def visitField_literal(self, ctx:TblCalcFieldParser.Field_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#boolean_literal.
    def visitBoolean_literal(self, ctx:TblCalcFieldParser.Boolean_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#numeric_literal.
    def visitNumeric_literal(self, ctx:TblCalcFieldParser.Numeric_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#string_literal.
    def visitString_literal(self, ctx:TblCalcFieldParser.String_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TblCalcFieldParser#date_literal.
    def visitDate_literal(self, ctx:TblCalcFieldParser.Date_literalContext):
        return self.visitChildren(ctx)



del TblCalcFieldParser