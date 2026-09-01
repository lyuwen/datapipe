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
    Each,
    Expression,
    Field,
    Index,
    Invocation,
    Literal,
    Program,
    QualifiedName,
    QuotedKey,
    Selector,
    Statement,
)
from datapipe.dsl.parser import parse, parse_program
from datapipe.dsl.selector import CompiledSelector, Reference
from datapipe.dsl.compiler import (
    CompiledExpression,
    CompiledProgram,
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
    "Each",
    "Expression",
    "Field",
    "Index",
    "Invocation",
    "Literal",
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
    "CompiledExpression",
    "CompiledProgram",
    "ToolInvocation",
    "compile_expression",
    "compile_program",
]
