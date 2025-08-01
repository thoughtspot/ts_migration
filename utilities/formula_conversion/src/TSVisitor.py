from antlr4 import *
from .grammar.TblCalcFieldParser import TblCalcFieldParser
from .grammar.TblCalcFieldVisitor import TblCalcFieldVisitor

import inspect

def log(msg: str):
  return
  callerframerecord = inspect.stack()[1]    # 0 represents this line
                                            # 1 represents line at caller
  frame = callerframerecord[0]
  info = inspect.getframeinfo(frame)
  print(f"{info.filename}:{info.lineno}: {msg}")

formula_mapping = {
    "CEILING": "ceil",
    "DIV": "safe_divide",
    "LOG": "log10",
    "POWER": "pow",
    "SQUARE": "sq",
    "LEN": "strlen",
    "MID": "substr",
    "QUARTER": "quarter_number",
    "WEEK" : "week_number_of_year",
    "STDEV": "stddev",
    "RUNNING_AVG": "cumulative_average",
    "AVG" : "average",
    "RUNNING_SUM": "cumulative_sum",

    
    
}
class TSVisitor(TblCalcFieldVisitor):

    def __init__(self):
        super(TSVisitor, self).__init__()

    # Visit a parse tree produced by TblCalcFieldParser#calc_field.
    def visitCalc_field(self, ctx:TblCalcFieldParser.Calc_fieldContext):
        res = None
        if ctx.expr():
            res = self.visit(ctx.expr())
        else:
            res = "TBD"
        log(res)
        return res
    

    # Visit a parse tree produced by TblCalcFieldParser#expr.
    def visitExpr(self, ctx:TblCalcFieldParser.ExprContext):
        res = None
        if (ctx.field_expr()):
            res = self.visit(ctx.field_expr())
        elif (ctx.lod_expr()):
            res = self.visit(ctx.lod_expr())
        elif (ctx.function_expr()):
            res = self.visit(ctx.function_expr())
        elif (ctx.string_literal()):
            res = ctx.string_literal().getText()
        elif (ctx.numeric_literal()):
            res = ctx.numeric_literal().getText()
        elif (ctx.case_expr()):
            res = self.visit(ctx.case_expr())
        elif (ctx.if_expr()):
            res = self.visit(ctx.if_expr())
        elif (ctx.LEFT_PAREN()):
            res = "(" + self.visit(ctx.expr(0)) + ")"
        elif (ctx.LOGICAL_OPERATION()):
            res = self.visit(ctx.expr(0)) + " " + ctx.LOGICAL_OPERATION().getText() + " " + self.visit(ctx.expr(1))
        elif (ctx.AIRTHEMATIC_OPERATION()):
           res = self.visit(ctx.expr(0)) + " "+ ctx.AIRTHEMATIC_OPERATION().getText() + " "+ self.visit(ctx.expr(1))
        else:
            res = "TBD" 
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#case_expr.
    def visitCase_expr(self, ctx:TblCalcFieldParser.Case_exprContext):
        res = ""
        self.case_term_str = self.visit(ctx.case_term())
        for (i, when_expr) in enumerate(ctx.when_expr()):
            if i > 0:
                res += " else "
            res += self.visit(when_expr)
        res += " else " + self.visit(ctx.expr())
        log(res)
        return res


   # Visit a parse tree produced by TblCalcFieldParser#when_expr.
    def visitWhen_expr(self, ctx:TblCalcFieldParser.When_exprContext):
        res = "TBD"
        res = "if ( " + self.case_term_str + " = " + self.visit(ctx.expr(0)) + " ) then " + self.visit(ctx.expr(1))
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#if_expr.
    def visitIf_expr(self, ctx:TblCalcFieldParser.If_exprContext):
        # TODO(Pankaj): Handle elsif and else cases
        # This is INCOMPLETE.
        res = "if (" + self.visit(ctx.expr(0)) + ") then " + self.visit(ctx.expr(1)) + self.visit(ctx.else_expr())
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#elseif_expr.
    def visitElseif_expr(self, ctx:TblCalcFieldParser.Elseif_exprContext):
        res = "TBD"
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#else_expr.
    def visitElse_expr(self, ctx:TblCalcFieldParser.Else_exprContext):
        res = " else " + self.visit(ctx.expr())
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#iif_expr.
    def visitIif_expr(self, ctx:TblCalcFieldParser.Iif_exprContext):
        res = "TBD"
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#function_expr.
    def visitFunction_expr(self, ctx:TblCalcFieldParser.Function_exprContext):
        fn_name = ctx.function_name().getText()
        if fn_name in formula_mapping.keys():
            fn_name = formula_mapping[fn_name]
        elif fn_name == "DATENAME":
            return self.handleDateNameFn(ctx)
        elif fn_name == "DATEADD":
            return self.handleDateAddFn(ctx)
        elif fn_name == "DATEDIFF":
            return self.handleDateDiffFn(ctx)
        elif fn_name == "COUNTD":
            fn_name = "unique count"
        elif fn_name in ["ABS", "SUM" ,"ACOS", "ASIN", "ATAN", "ATAN2", "COS", "EXP", "FLOOR" , "LN", "MAX", "ROUND", "SIGN", "SIN", "SQRT", "TAN", "LEFT", "MIN", "IFNULL", "ISNULL", "COUNT", "MEDIAN" , "DAY", "MONTH", "NOW", "TODAY", "YEAR", "DATE"]:
            fn_name = fn_name.lower()  
        res = fn_name + "("
        for (i, expr) in enumerate(ctx.expr()):
            if i > 0:
                res += ", "
            res += self.visit(expr)
        res += ")" 
        log(res)
        return res

    # Visit a parse tree produced by TblCalcFieldParser#function_name.
    def visitFunction_name(self, ctx:TblCalcFieldParser.Function_nameContext):
        res = ctx.getText()
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#lod_expr.
    def visitLod_expr(self, ctx:TblCalcFieldParser.Lod_exprContext):
        # res = "{"
        # if ctx.lod_type:
        #     res = ctx.lod_type.text + " "
        # for (i, dim) in enumerate(ctx.lod_dim()):
        #     if i > 0:
        #         res += ", "
        #     res += self.visit(dim) 
        # res += ":"
        # res += self.visit(ctx.lod_aggr())
        # res += "}"

        # Handle special case where lod_type is not present
        # For example: {MIN([Order Date])} is equiv to {FIXED : MIN([Order Date])}
        if ctx.lod_type is None:
            res = "group_aggregate(" + self.visit(ctx.lod_aggr()) + ", {}, {})"
        # handle cases where lod_type is present
        else:
            lod_type = ctx.lod_type.text
            match lod_type:
                case "FIXED":
                    visited = False
                    res = "group_aggregate(" + self.visit(ctx.lod_aggr()) + ", "
                    for (i, dim) in enumerate(ctx.lod_dim()):
                        visited = True
                        print(i)
                        if i > 0:
                            res += " + "
                        res += '{' + self.visit(dim) + '}'
                    if visited == False:
                        res += "{}"
                    res += ", {})"
                case "INCLUDE":
                    res = "group_aggregate(" + self.visit(ctx.lod_aggr()) + ", query_groups() + " 
                    for (i, dim) in enumerate(ctx.lod_dim()):
                        if i > 0:
                            res += " + "
                        res += '{' + self.visit(dim) + '}'
                    res += "}, query_filters())"
                case "EXCLUDE":
                    res = "group_aggregate(" + self.visit(ctx.lod_aggr()) + ", query_groups() - " 
                    for (i, dim) in enumerate(ctx.lod_dim()):
                        if i > 0:
                            res += " - "
                        res += '{' + self.visit(dim) + '}'
                    res += ", query_filters()))"
                case _:
                    log("Unknown lod_type: " + lod_type)
        log(res)
        return res

  # Visit a parse tree produced by TblCalcFieldParser#lod_dim.
    def visitLod_dim(self, ctx:TblCalcFieldParser.Lod_dimContext):
        res = self.visit(ctx.expr())
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#lod_aggr.
    def visitLod_aggr(self, ctx:TblCalcFieldParser.Lod_aggrContext):
        res = self.visit(ctx.expr())
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#field_expr.
    def visitField_expr(self, ctx:TblCalcFieldParser.Field_exprContext):
        res = None
        if (ctx.lod_expr()):
            lod_res = self.visit(ctx.lod_expr())
            res = str(lod_res)
        else:
            res = self.visit(ctx.field_literal())
        log(str(res))
        return res


    # Visit a parse tree produced by TblCalcFieldParser#field_literal.
    def visitField_literal(self, ctx:TblCalcFieldParser.Field_literalContext):
        res = ctx.getText()
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#boolean_literal.
    def visitBoolean_literal(self, ctx:TblCalcFieldParser.Boolean_literalContext):
        res = ctx.getText()
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#numeric_literal.
    def visitNumeric_literal(self, ctx:TblCalcFieldParser.Numeric_literalContext):
        res = ctx.getText()
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#string_literal.
    def visitString_literal(self, ctx:TblCalcFieldParser.String_literalContext):
        res = ctx.getText()
        log(res)
        return res


    # Visit a parse tree produced by TblCalcFieldParser#date_literal.
    def visitDate_literal(self, ctx:TblCalcFieldParser.Date_literalContext):
        res = ctx.getText()
        log(res)
        return res

    def handleDateAddFn(self, ctx:TblCalcFieldParser.Function_exprContext):
        res = "TBD"
        log(ctx.expr(0).getText())
        match ctx.expr(0).getText():
            case "\"day\"":
                res = "add_days(" + self.visit(ctx.expr(2)) + ", " + self.visit(ctx.expr(1)) + ")"
            case "\"month\"":
                res = "add_months(" + self.visit(ctx.expr(2)) + ", " + self.visit(ctx.expr(1)) + ")"
            case "\"year\"":
                res = "add_years(" + self.visit(ctx.expr(2)) + ", " + self.visit(ctx.expr(1)) + ")"
            case "\"hour\"":
                res = "add_hours(" + self.visit(ctx.expr(2)) + ", " + self.visit(ctx.expr(1)) + ")"
            case "\"minute\"":
                res = "add_minutes(" + self.visit(ctx.expr(2)) + ", " + self.visit(ctx.expr(1)) + ")"
            case "\"second\"":  
                res = "add_seconds(" + self.visit(ctx.expr(2)) + ", " + self.visit(ctx.expr(1)) + ")"
            case "\"week\"":
                res = "add_weeks(" + self.visit(ctx.expr(2)) + ", " + self.visit(ctx.expr(1)) + ")"
            case _:
                log("Unknown date part: " + ctx.expr(0).getText())
        log(res)
        return res
    
    def handleDateDiffFn(self, ctx:TblCalcFieldParser.Function_exprContext):
        res = "TBD"
        match ctx.expr(0).getText():
            case "\"day\"":
                res = "diff_days(" + self.visit(ctx.expr(1)) + ", " + self.visit(ctx.expr(2)) + ")"
            case "\"month\"":
                res = "diff_months(" + self.visit(ctx.expr(1)) + ", " + self.visit(ctx.expr(2)) + ")"
            case "\"year\"":
                res = "diff_years(" + self.visit(ctx.expr(1)) + ", " + self.visit(ctx.expr(2)) + ")"
            case "\"hour\"":
                res = "diff_hours(" + self.visit(ctx.expr(1)) + ", " + self.visit(ctx.expr(2)) + ")"
            case "\"minute\"":
                res = "diff_minutes(" + self.visit(ctx.expr(1)) + ", " + self.visit(ctx.expr(2)) + ")"
            case "\"second\"":  
                res = "diff_seconds(" + self.visit(ctx.expr(1)) + ", " + self.visit(ctx.expr(2)) + ")"
            case "\"week\"":
                res = "diff_weeks(" + self.visit(ctx.expr(1)) + ", " + self.visit(ctx.expr(2)) + ")"
            case "\"quarter\"":
                res = "diff_quarters(" + self.visit(ctx.expr(1)) + ", " + self.visit(ctx.expr(2)) + ")"
            case "\"second\"":
                res = "diff_time(" + self.visit(ctx.expr(1)) + ", " + self.visit(ctx.expr(2)) + ")"
            case _:
                log("Unknown date part: " + ctx.expr(0).getText())
        log(res)
        return res
    
    def handleDateNameFn(self, ctx:TblCalcFieldParser.Function_exprContext):
        res = "TBD"
        match ctx.expr(0).getText():
            case "\"date_part\"":
                res = "days(" + self.visit(ctx.expr(1)) + ", " + self.visit(ctx.expr(2)) + ")"
            case "\"dayofyear\"":
                res = "day_number_of_year(" + self.visit(ctx.expr(1)) + ")"
            case "\"year\"":
                res = "year(" + self.visit(ctx.expr(1)) + ")"
            case "\"hour\"":
                res = "hour_of_day(" + self.visit(ctx.expr(1)) + ")"
            case "\"month\"":
                res = "month(" + self.visit(ctx.expr(1)) + ")"
            case "\"quarter\"":
                res = "quarter_number(" + self.visit(ctx.expr(1)) + ")"
            case "\"weekday\"":
                res = "day_of_week(" + self.visit(ctx.expr(1)) + ")"
            case "\"year\"":
                res = "year(" + self.visit(ctx.expr(1)) + ")"
            case _:
                log("Unknown date part: " + ctx.expr(0).getText())
        log(res)
        return res

