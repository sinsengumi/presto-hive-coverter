package net.sinsengumi.presto_hive_coverter;

import static com.google.common.base.Preconditions.*;
import static com.google.common.collect.Iterables.*;
import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.Function;

import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class ExpressionConverter extends com.facebook.presto.sql.ExpressionFormatter.Formatter {

    private final HiveQueryConverter converter = new HiveQueryConverter();

    public ExpressionConverter(Optional<List<Expression>> parameters) {
        super(parameters);
    }

    @Override
    protected String visitFunctionCall(FunctionCall node, Void context) {
        StringBuilder builder = new StringBuilder();

        String arguments = joinExpressions(node.getArguments());
        if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
            arguments = "*";
        }
        if (node.isDistinct()) {
            arguments = "DISTINCT " + arguments;
        }

        if (converter.isConvertableFuncionCall(node.getName())) {
            builder.append(converter.convertFunction(node.getName(), node.getArguments()));
        } else {
            builder.append(formatQualifiedName(node.getName())).append('(').append(arguments);
        }

        if (node.getOrderBy().isPresent()) {
            builder.append(' ').append(formatOrderBy(node.getOrderBy().get(), Optional.empty()));
        }

        builder.append(')');

        if (node.getFilter().isPresent()) {
            builder.append(" FILTER ").append(visitFilter(node.getFilter().get(), context));
        }

        if (node.getWindow().isPresent()) {
            builder.append(" OVER ").append(visitWindow(node.getWindow().get(), context));
        }

        return builder.toString();
    }

    private String joinExpressions(List<Expression> expressions) {
        return Joiner.on(", ").join(expressions.stream().map((e) -> process(e, null)).iterator());
    }

    private static String formatQualifiedName(QualifiedName name) {
        List<String> parts = new ArrayList<>();
        for (String part : name.getParts()) {
            parts.add(formatIdentifier(part));
        }
        return Joiner.on('.').join(parts);
    }

    private static String formatIdentifier(String s) {
        // TODO: handle escaping properly
        return '"' + s + '"';
    }
    
    private String visitFilter(Expression node, Void context) {
        return "(WHERE " + process(node, context) + ')';
    }

    //////////////////////////////////////////////
    public static String formatExpression(Expression expression, Optional<List<Expression>> parameters) {
        return new ExpressionConverter(parameters).process(expression, null);
    }

    static String formatStringLiteral(String s) {
        s = s.replace("'", "''");
        if (isAsciiPrintable(s)) {
            return "'" + s + "'";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("U&'");
        PrimitiveIterator.OfInt iterator = s.codePoints().iterator();
        while (iterator.hasNext()) {
            int codePoint = iterator.nextInt();
            checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", s);
            if (isAsciiPrintable(codePoint)) {
                char ch = (char) codePoint;
                if (ch == '\\') {
                    builder.append(ch);
                }
                builder.append(ch);
            } else if (codePoint <= 0xFFFF) {
                builder.append('\\');
                builder.append(String.format("%04X", codePoint));
            } else {
                builder.append("\\+");
                builder.append(String.format("%06X", codePoint));
            }
        }
        builder.append("'");
        return builder.toString();
    }

    static String formatOrderBy(OrderBy orderBy, Optional<List<Expression>> parameters) {
        return "ORDER BY " + formatSortItems(orderBy.getSortItems(), parameters);
    }

    static String formatSortItems(List<SortItem> sortItems, Optional<List<Expression>> parameters) {
        return Joiner.on(", ").join(sortItems.stream().map(sortItemFormatterFunction(parameters)).iterator());
    }

    static String formatGroupBy(List<GroupingElement> groupingElements) {
        return formatGroupBy(groupingElements, Optional.empty());
    }

    static String formatGroupBy(List<GroupingElement> groupingElements, Optional<List<Expression>> parameters) {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        for (GroupingElement groupingElement : groupingElements) {
            String result = "";
            if (groupingElement instanceof SimpleGroupBy) {
                Set<Expression> columns = ImmutableSet.copyOf(((SimpleGroupBy) groupingElement).getColumnExpressions());
                if (columns.size() == 1) {
                    result = formatExpression(getOnlyElement(columns), parameters);
                } else {
                    result = formatGroupingSet(columns, parameters);
                }
            } else if (groupingElement instanceof GroupingSets) {
                result = format("GROUPING SETS (%s)", Joiner.on(", ").join(((GroupingSets) groupingElement).getSets()
                        .stream().map(ExpressionConverter::formatGroupingSet).iterator()));
            } else if (groupingElement instanceof Cube) {
                result = format("CUBE %s", formatGroupingSet(((Cube) groupingElement).getColumns()));
            } else if (groupingElement instanceof Rollup) {
                result = format("ROLLUP %s", formatGroupingSet(((Rollup) groupingElement).getColumns()));
            }
            resultStrings.add(result);
        }
        return Joiner.on(", ").join(resultStrings.build());
    }

    private static boolean isAsciiPrintable(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!isAsciiPrintable(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAsciiPrintable(int codePoint) {
        if (codePoint >= 0x7F || codePoint < 0x20) {
            return false;
        }
        return true;
    }

    private static String formatGroupingSet(Set<Expression> groupingSet, Optional<List<Expression>> parameters) {
        return format("(%s)",
                Joiner.on(", ").join(groupingSet.stream().map(e -> formatExpression(e, parameters)).iterator()));
    }

    private static String formatGroupingSet(List<QualifiedName> groupingSet) {
        return format("(%s)", Joiner.on(", ").join(groupingSet));
    }

    private static Function<SortItem, String> sortItemFormatterFunction(Optional<List<Expression>> parameters) {
        return input -> {
            StringBuilder builder = new StringBuilder();

            builder.append(formatExpression(input.getSortKey(), parameters));

            switch (input.getOrdering()) {
            case ASCENDING:
                builder.append(" ASC");
                break;
            case DESCENDING:
                builder.append(" DESC");
                break;
            default:
                throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
            }

            switch (input.getNullOrdering()) {
            case FIRST:
                builder.append(" NULLS FIRST");
                break;
            case LAST:
                builder.append(" NULLS LAST");
                break;
            case UNDEFINED:
                // no op
                break;
            default:
                throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
            }

            return builder.toString();
        };
    }
}
