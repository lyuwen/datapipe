"""datapipe.dsl: jq-like expression language parser and compiler.

Phase 2 exports:

    from datapipe.dsl import parse, compile_expression
    from datapipe.dsl import Expression, Invocation, Selector, Span
    from datapipe.dsl.errors import ExpressionSyntaxError, ToolResolutionError
    from datapipe.dsl.selector import CompiledSelector
    from datapipe.dsl.compiler import CompiledExpression, ToolInvocation

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
    QualifiedName,
    QuotedKey,
    Selector,
)
from datapipe.dsl.parser import parse
from datapipe.dsl.selector import CompiledSelector, Reference
from datapipe.dsl.compiler import CompiledExpression, ToolInvocation, compile_expression

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
    "QualifiedName",
    "QuotedKey",
    "Selector",
    # Parser
    "parse",
    # Selector runtime
    "CompiledSelector",
    "Reference",
    # Compiler
    "CompiledExpression",
    "ToolInvocation",
    "compile_expression",
]
