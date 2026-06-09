package io.mycat.dialect.ast;

public class ExpressionNode {
    private final String expression;

    public ExpressionNode(String expression) {
        this.expression = expression;
    }

    public String getExpression() { return expression; }
}