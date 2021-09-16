package org.bklab.quark.operation.jdbc.builder.sql;

import dataq.core.operation.OperationContext;
import org.bklab.quark.util.time.LocalDateTimeFormatter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlWhereConditionBuilder {
    private final List<String> conditions;
    private final OperationContext context;

    public SqlWhereConditionBuilder(List<String> conditions, OperationContext context) {
        this.conditions = conditions;
        this.context = context;
    }

    public <V> SqlWhereConditionBuilder addCondition(String name, String fieldName) {
        return addCondition(name, value -> "`" + fieldName + "` = '" + value + "'");
    }

    public <V> SqlWhereConditionBuilder addLikeCondition(String name, String fieldName) {
        return addCondition(name, value -> "`" + fieldName + "` LIKE '%" + value + "%'");
    }

    public SqlWhereConditionBuilder addNumberCondition(String name, String fieldName) {
        return this.<Number>addCondition(name, value -> "`" + fieldName + "` = " + value + "");
    }

    public <V> SqlWhereConditionBuilder addCondition(String name, Function<V, String> function) {
        Optional.ofNullable(getContext().<V>getObject(name)).ifPresent(value -> conditions.add(function.apply(value)));
        return this;
    }

    public <V> SqlWhereConditionBuilder addInCondition(String name, String filedName) {
        return addInCondition(name, filedName, String::valueOf);
    }

    public <V> SqlWhereConditionBuilder addInCondition(String name, String filedName, Function<V, String> function) {
        Optional.ofNullable(getContext().<Collection<V>>getObject(name)).ifPresent(items -> conditions.add(
                items.isEmpty()
                ? "`" + filedName + "` != `" + filedName + "`"
                : "`" + filedName + "` IN ('" + items.stream().map(function).collect(Collectors.joining("', '")) + "')")
        );
        return this;
    }

    public SqlWhereConditionBuilder addInNumberCondition(String name, String filedName) {
        return this.<Number>addInNumberCondition(name, filedName, a -> a);
    }

    public <V> SqlWhereConditionBuilder addInNumberCondition(String name, String filedName, Function<V, Number> function) {
        Optional.ofNullable(getContext().<Collection<V>>getObject(name)).ifPresent(items -> conditions.add(
                items.isEmpty()
                ? "`" + filedName + "` != `" + filedName + "`"
                : "`" + filedName + "` IN (" + items.stream().map(function).map(String::valueOf).collect(Collectors.joining(", ")) + ")")
        );
        return this;
    }

    public <V> SqlWhereConditionBuilder addRangeCondition(String minName, String maxName, String filedName, Function<V, String> function) {
        return this
                .<V>addCondition(minName, value -> "`" + filedName + "` >= '" + function.apply(value) + "'")
                .<V>addCondition(maxName, value -> "`" + filedName + "` <= '" + function.apply(value) + "'")
                ;
    }

    public SqlWhereConditionBuilder addLocalDateTimeRangeCondition(String minName, String maxName, String filedName) {
        return this.<LocalDateTime>addRangeCondition(minName, maxName, filedName, LocalDateTimeFormatter::Short);
    }

    public SqlWhereConditionBuilder addLocalDateRangeCondition(String minName, String maxName, String filedName) {
        return this.<LocalDate>addRangeCondition(minName, maxName, filedName, LocalDateTimeFormatter::Short);
    }

    public SqlWhereConditionBuilder addLocalTimeRangeCondition(String minName, String maxName, String filedName) {
        return this.<LocalTime>addRangeCondition(minName, maxName, filedName, LocalDateTimeFormatter::Short);
    }

    public List<String> getConditions() {
        return conditions;
    }

    public OperationContext getContext() {
        return context;
    }
}
