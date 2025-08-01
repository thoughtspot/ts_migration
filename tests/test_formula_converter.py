import unittest
import sys, os
from utilities.formula_conversion import formula

def test_convert():
    assert formula.convert("""
    /* Use short form code of [REGION]
     * This will make more room in the chart area
     */
    CASE [REGION]
        WHEN 'East' THEN 'E'
        WHEN 'West' THEN 'W'
        WHEN 'South' THEN 'S'
        WHEN 'North' THEN 'N'
        WHEN 'Central' THEN 'C'
        ELSE 'Unknown Region'
    END
    """) == "(calc_field (expr (case_expr CASE (case_term (expr (field_expr (field_literal [REGION])))) (when_expr WHEN (expr (string_literal 'East')) THEN (expr (string_literal 'E'))) (when_expr WHEN (expr (string_literal 'West')) THEN (expr (string_literal 'W'))) (when_expr WHEN (expr (string_literal 'South')) THEN (expr (string_literal 'S'))) (when_expr WHEN (expr (string_literal 'North')) THEN (expr (string_literal 'N'))) (when_expr WHEN (expr (string_literal 'Central')) THEN (expr (string_literal 'C'))) ELSE (expr (string_literal 'Unknown Region')) END)) <EOF>) if ( [REGION] = 'East' ) then 'E' else if ( [REGION] = 'West' ) then 'W' else if ( [REGION] = 'South' ) then 'S' else if ( [REGION] = 'North' ) then 'N' else if ( [REGION] = 'Central' ) then 'C' else 'Unknown Region'"