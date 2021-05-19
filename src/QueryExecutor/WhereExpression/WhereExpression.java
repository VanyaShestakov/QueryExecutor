package QueryExecutor.WhereExpression;

public class WhereExpression {
    private final StringBuilder  expression;

    public WhereExpression() {
        expression = new StringBuilder();
    }

    private static class Condition {
        private final StringBuilder expression;

        protected Condition(StringBuilder expression) {
            this.expression = expression;
        }

        public Condition or(String condition) {
            expression.append(" OR ").append(condition);
            return new Condition(expression);
        }

        public Condition and(String condition) {
            expression.append(" AND ").append(condition);
            return new Condition(expression);
        }

        public Condition not(String condition) {
            expression.append(" NOT ").append(condition);
            return new Condition(expression);
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
        expression.addCondition("a > b").and("c > a").or("c < b");
        System.out.println(expression);

    }
}
