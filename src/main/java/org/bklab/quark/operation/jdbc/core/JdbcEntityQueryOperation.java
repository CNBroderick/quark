package org.bklab.quark.operation.jdbc.core;

import dataq.core.operation.OperationResult;
import org.bklab.quark.entity.dao.IEntityRowMapper;
import org.bklab.quark.entity.dao.PreparedStatementHelper;
import org.bklab.quark.operation.core.HasQueryEntities;
import org.bklab.quark.operation.core.HasWhereCondition;
import org.bklab.quark.operation.jdbc.builder.sql.SqlWhereConditionBuilder;

import java.sql.Connection;
import java.util.List;

public abstract class JdbcEntityQueryOperation<T, E extends JdbcEntityQueryOperation<T, E>> extends JdbcCoreOperation<E> implements HasQueryEntities<T>, HasWhereCondition {

    private IEntityRowMapper<T> rowMapper;

    public JdbcEntityQueryOperation() {
    }

    public JdbcEntityQueryOperation(IEntityRowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
    }

    @Override
    public OperationResult doExecute() throws Exception {
        return successResult(queryEntities(getDBAccess().getConnection()));
    }

    public String createSelectSql(String tableName, String... selectFields) {
        return "SELECT " + (selectFields.length > 0 ? String.join(", ", selectFields) : "*") + " FROM " + tableName + " " + createWhereCondition();
    }

    public List<T> queryEntities(Connection connection, String sql, Object... parameters) throws Exception {
        return queryEntities(connection, rowMapper, sql, parameters);
    }

    public List<T> queryEntities(Connection connection, IEntityRowMapper<T> rowMapper, String sql, Object... parameters) throws Exception {
        printSql("查询", sql, parameters);
        return new PreparedStatementHelper(connection, sql).executeQuery(parameters).asList(rowMapper);
    }

    @Override
    public List<String> createWhereConditions(List<String> conditions) {
        initWhereCondition(new SqlWhereConditionBuilder(conditions, getContext()));
        return conditions;
    }

    protected abstract void initWhereCondition(SqlWhereConditionBuilder builder);
}
