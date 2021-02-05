package org.bklab.quark.entity.dao;

import com.google.gson.GsonBuilder;
import org.bklab.quark.element.HasPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings("DuplicatedCode")
public class PreparedStatementHelper {

    private final PreparedStatement statement;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private boolean closeAfterExecute = false;

    public PreparedStatementHelper(PreparedStatement statement) {
        this.statement = statement;
    }

    public PreparedStatementHelper(Connection connection, String sql) throws Exception {
        this.statement = connection.prepareStatement(sql);
    }

    public PreparedStatementHelper(Connection connection, String sql, int autoGeneratedKeys) throws Exception {
        this.statement = connection.prepareStatement(sql, autoGeneratedKeys);
    }

    public static PreparedStatementHelper createGeneratedKey(Connection connection, String sql) throws Exception {
        return new PreparedStatementHelper(connection, sql, PreparedStatement.RETURN_GENERATED_KEYS);
    }

    public PreparedStatementHelper closeAfterExecute() {
        this.closeAfterExecute = true;
        return this;
    }

    public int insert(Object... parameters) throws Exception {
        return executeUpdate(parameters);
    }

    public int insert(Consumer<Integer> autoGenerateKeyConsumer, Object... parameters) throws Exception {
        int i = executeUpdate(parameters);
        new ResultSetHelper(statement.getGeneratedKeys()).setEntityGeneratedKeys(autoGenerateKeyConsumer);
        return i;
    }

    public int executeUpdate(Object... parameters) throws SQLException {
        return executeUpdate(true, parameters);
    }

    public int executeUpdate(boolean commit, Object... parameters) throws SQLException {
        try {
            insertParameters(parameters);
            int a = statement.executeUpdate();
            if (commit) statement.getConnection().commit();
            return a;
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("执行更新失败\n" + statement + "\n" + new GsonBuilder().setPrettyPrinting().create().toJson(parameters) + "\n", e);
            throw e;
        } finally {
            if (closeAfterExecute) close();
        }
    }

    public ResultSetHelper executeQuery(Object... parameters) throws Exception {
        try {
            insertParameters(parameters);
            return new ResultSetHelper(statement.executeQuery());
        } catch (SQLException e) {
            logger.error("执行查询失败\n" + statement + "\n" + new GsonBuilder().setPrettyPrinting().create().toJson(parameters) + "\n", e);
            throw e;
        }
    }

    public PreparedStatementHelper addBatch(Object... parameters) throws SQLException {
        HasPreparedStatement.get().insertPsParameter(statement, parameters);
        statement.addBatch();
        return this;
    }

    public <T> PreparedStatementHelper addBatch(Collection<T> entities, Function<T, Object[]> parametersSupplier) throws SQLException {
        for (T entity : entities) {
            if (entity == null) continue;
            addBatch(parametersSupplier.apply(entity));
        }
        return this;
    }

    public <T> List<T> executeInsertBatch(List<T> entities, BiConsumer<T, Integer> idSetter) throws SQLException {
        return executeInsertBatch(true, entities, idSetter);
    }

    public <T> List<T> executeInsertBatch(boolean commit, List<T> entities, BiConsumer<T, Integer> idSetter) throws SQLException {
        int[] ids = executeBatch(commit);
        for (int i = 0; i < ids.length; i++) {
            if (entities.size() > i) {
                T t = entities.get(i);
                if (t != null) idSetter.accept(t, ids[i]);
            }
        }
        return entities;
    }

    public int[] executeBatch() throws SQLException {
        return executeBatch(true);
    }

    public int[] executeBatch(boolean commit) throws SQLException {
        try {
            int[] a = statement.executeBatch();
            if (commit) statement.getConnection().commit();
            return a;
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("执行批量更新失败\n" + statement + "\n", e);
            throw e;
        } finally {
            if (closeAfterExecute) close();
        }
    }

    private void insertParameters(Object... parameters) throws SQLException {
        if (parameters == null) return;
        for (int i = 0; i < parameters.length; i++) {
            Object parameter = parameters[i];
            if (parameter instanceof Integer) statement.setInt(i + 1, (Integer) parameter);
            else if (parameter instanceof Long) statement.setLong(i + 1, (Long) parameter);
            else if (parameter instanceof Double) statement.setDouble(i + 1, (Double) parameter);
            else if (parameter instanceof Float) statement.setFloat(i + 1, (Float) parameter);
            else statement.setObject(i + 1, parameter);
        }
    }

    public void close() throws SQLException {
        try {
            if (statement != null && !statement.isClosed()) statement.close();
        } catch (SQLException e) {
            logger.error("关闭statements、resultSet失败。", e);
            throw e;
        }
    }
}
