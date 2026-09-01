"""datapipe.dsl: jq-like expression language parser and compiler.

Phase 2 exports:

    from datapipe.dsl import parse, compile_expression
    from datapipe.dsl import Expression, Invocation, Selector, Span
    from datapipe.dsl.errors import ExpressionSyntaxError, ToolResolutionError
    from datapipe.dsl.selector import CompiledSelector
    from datapipe.dsl.compiler import CompiledExpression, ToolInvocation

Phase S1 additions:

    from datapipe.dsl import parse_program, compile_program
    from datapipe.dsl import Program, Statement
    from datapipe.dsl.compiler import CompiledProgram

Phase S2 additions:

    from datapipe.dsl import BareToolCall
    from datapipe.dsl.compiler import CompiledBareCall, CompiledStatement

Phase S3 additions:

    from datapipe.dsl import Assignment, AssignmentRHS
    from datapipe.dsl.compiler import CompiledAssignment

Phase S4 additions:

    from datapipe.dsl import FieldSet, MoveInto
    from datapipe.dsl.compiler import CompiledFieldSet, CompiledMoveInto

"""

from datapipe.dsl.errors import (
    ExpressionSyntaxError,
    SelectorResolutionError,
    Span,
    ToolConfigurationError,
    ToolResolutionError,
)
from datapipe.dsl.ast import (
    Argument,
    Assignment,
    AssignmentRHS,
    BareToolCall,
    Each,
    Expression,
    Field,
    FieldSet,
    Index,
    Invocation,
    Literal,
    MoveInto,
    Program,
    QualifiedName,
    QuotedKey,
    Selector,
    Statement,
)
from datapipe.dsl.parser import parse, parse_program
from datapipe.dsl.selector import CompiledSelector, Reference
from datapipe.dsl.compiler import (
    CompiledAssignment,
    CompiledBareCall,
    CompiledExpression,
    CompiledFieldSet,
    CompiledMoveInto,
    CompiledProgram,
    CompiledStatement,
    ToolInvocation,
    compile_expression,
    compile_program,
)

__all__ = [
    # Errors
    "ExpressionSyntaxError",
    "SelectorResolutionError",
    "Span",
    "ToolConfigurationError",
    "ToolResolutionError",
    # AST nodes
    "Argument",
    "Assignment",
    "AssignmentRHS",
    "BareToolCall",
    "Each",
    "Expression",
    "Field",
    "FieldSet",
    "Index",
    "Invocation",
    "Literal",
    "MoveInto",
    "Program",
    "QualifiedName",
    "QuotedKey",
    "Selector",
    "Statement",
    # Parser
    "parse",
    "parse_program",
    # Selector runtime
    "CompiledSelector",
    "Reference",
    # Compiler
    "CompiledAssignment",
    "CompiledBareCall",
    "CompiledExpression",
    "CompiledFieldSet",
    "CompiledMoveInto",
    "CompiledProgram",
    "CompiledStatement",
    "ToolInvocation",
    "compile_expression",
    "compile_program",
]
