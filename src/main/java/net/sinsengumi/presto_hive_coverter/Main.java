package net.sinsengumi.presto_hive_coverter;

import java.util.Optional;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;

public class Main {

    public static void main(String[] args) {
        String prestoSql = "SELECT json_EXTRACT_SCALAR(JSON, '$.timestamp'), LOG_DATE, replace(os_version, 'iPhone') FROM HIVE.HOGE_LOG WHERE LOG_DATE='20180328' AND LOG_HOUR='23' AND LOG_TYPE='type1' ORDER BY JSON_EXTRACT_SCALAR(JSON, '$.timestamp'), LOG_DATE LIMIT 10";
        System.out.println(formatSql(prestoSql));
        System.out.println("=================");
        System.out.println(convertSql(prestoSql));
        System.out.println("Finished");
    }

    private static String convertSql(String prestoSql) {
        StringBuilder builder = new StringBuilder();

        SqlParser sqlParser = new SqlParser();
        Statement statement = sqlParser.createStatement(prestoSql, new ParsingOptions());
        HiveQueryConvertVisitor visitor = new HiveQueryConvertVisitor(builder, Optional.empty());
        visitor.process(statement, 0);

        return builder.toString();
    }

    private static String formatSql(String prestoSql) {
        SqlParser sqlParser = new SqlParser();
        Statement statement = sqlParser.createStatement(prestoSql, new ParsingOptions());
        System.out.println(statement.toString());
        return SqlFormatter.formatSql(statement, Optional.empty());
    }
}
