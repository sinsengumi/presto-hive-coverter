package net.sinsengumi.presto_hive_coverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Joiner;

public class HiveQueryConverter {

    public boolean isConvertableFuncionCall(QualifiedName functionName) {
        List<String> convertableFunctions = Arrays.asList("json_extract_scalar", "replace");
        return convertableFunctions.contains(functionName.toString());
    }

    public String convertFunction(QualifiedName functionName, List<Expression> arguments) {
        if (!isConvertableFuncionCall(functionName)) {
            throw new RuntimeException("Not Support function. " + functionName);
        }

        StringBuilder builder = new StringBuilder();
        if (functionName.toString().equals("json_extract_scalar")) {
            String argumentsStr = joinExpressions(arguments);
            return builder.append(formatQualifiedName(QualifiedName.of("get_json_object"))).append('(').append(argumentsStr).toString();
        } else if (functionName.toString().equals("replace")) {
            return "aa";
        } else {
            throw new RuntimeException("Not Support function. " + functionName);
        }
    }

    private String joinExpressions(List<Expression> expressions) {
        return Joiner.on(", ").join(expressions.stream().map(e -> e.toString()).iterator());
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
}
