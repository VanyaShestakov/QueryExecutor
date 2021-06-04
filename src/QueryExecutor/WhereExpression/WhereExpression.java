package QueryExecutor.WhereExpression;

public class WhereExpression {
    private final StringBuilder  expression;

    public WhereExpression() {
        expression = new StringBuilder();
    }

    public class Condition {
        private final StringBuilder expression;

        private Condition(StringBuilder expression) {
            this.expression = expression;
        }

        public Condition or(String condition) {
            expression.append(" OR ").append(condition);
            return this;
        }

        public Condition and(String condition) {
            expression.append(" AND ").append(condition);
            return this;
        }

        public Condition not(String condition) {
            expression.append(" NOT ").append(condition);
            return this;
        }

    }

    public Condition addCondition(String condition) {
        expression.append(condition);
        return new Condition(expression);
    }

    @Override
    public String toString () {
        return expression.toString();
    }

    public static void main(String[] args) {
        WhereExpression expression = new WhereExpression();
        expression.addCondition("aaa").and("ss");
        System.out.println(expression);
    }

}
