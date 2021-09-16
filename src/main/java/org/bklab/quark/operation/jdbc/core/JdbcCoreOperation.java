package org.bklab.quark.operation.jdbc.core;

import dataq.core.jdbc.DBAccess;
import dataq.core.operation.AbstractOperation;
import dataq.core.operation.JdbcOperation;
import dataq.core.operation.OperationContext;
import org.bklab.quark.element.HasReturnThis;
import org.bklab.quark.util.security.ExamineUtil;
import org.bklab.quark.util.time.RunningTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class JdbcCoreOperation<E extends JdbcCoreOperation<E>> extends JdbcOperation implements HasReturnThis<E> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final String operationId = ExamineUtil.md5().calc(UUID.randomUUID().toString()).substring(24);
    private RunningTime runningTime;
    protected boolean keepJdbcAlive = false;
    private Supplier<Connection> connectionSupplier;
    private Set<Connection> createdConnections = new LinkedHashSet<>();

    public JdbcCoreOperation<E> keepJdbcAlive() {
        this.keepJdbcAlive = true;
        return this;
    }

    public JdbcCoreOperation<E> setConnectionSupplier(Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
        return this;
    }

    public Connection getConnection() throws Exception {
        if (connectionSupplier != null) {
            Connection connection = connectionSupplier.get();
            createdConnections.add(connection);
            return connection;
        }
        return super.getDBAccess().getConnection();
    }

    @Override
    public E setDBAccess(DBAccess dbAccess) {
        super.setDBAccess(dbAccess);
        return thisObject();
    }

    @Override
    public void beforeExecute() {
        this.runningTime = new RunningTime();
        super.beforeExecute();
    }

    @Override
    public void afterExecute() {
        super.afterExecute();
        logger.trace("[" + operationId + "] 执行[" + getOperationName() + "]完成，用时：" + runningTime.getMillis());
        if (!keepJdbcAlive) {
            try {
                Field field = DBAccess.class.getDeclaredField("connection");
                field.setAccessible(true);
                Connection connection = (Connection) field.get(getDBAccess());
                if (connection != null && connection.isClosed()) connection.close();
                createdConnections.forEach(this::close);
            } catch (Exception e) {
                LoggerFactory.getLogger(getClass()).debug("关闭链接失败", e);
            }
        }
    }

    private void close(Connection connection) {
        if (connection == null || keepJdbcAlive) return;
        try {
            if (connection.isClosed()) return;
            connection.rollback();
            connection.close();
        } catch (Exception e) {
            LoggerFactory.getLogger(getClass()).debug("关闭链接失败", e);
        }
    }

    @Override
    public OperationContext getContext() {
        return super.getContext();
    }

    @Override
    public E setContext(OperationContext context) {
        super.setContext(context);
        return thisObject();
    }

    @Override
    public E setParam(String name, Object value) {
        super.setParam(name, value);
        return thisObject();
    }

    @Override
    public AbstractOperation setForbiddenValues(String... values) {
        return super.setForbiddenValues(values);
    }

    public String getOperationName() {
        return Optional.ofNullable(getContext().getOperationName()).orElse(getClass().getSimpleName());
    }

    @Override
    public E setOperationName(String operationName) {
        super.setOperationName(operationName == null ? getClass().getSimpleName() : operationName);
        return thisObject();
    }

    protected <V> void addWhereCondition(List<String> conditions, String name, Function<V, String> function) {
        Optional.ofNullable(getContext().<V>getObject(name)).ifPresent(value -> conditions.add(function.apply(value)));
    }

    protected String compressSql(String sql) {
        return Objects.requireNonNull(sql, "sql is null").replaceAll("\n", " ").replaceAll("\t", " ").replaceAll(" +", " ");
    }

    protected void printSql(String operationName, String sql, Object... parameters) {
        logger.trace("[" + operationId + "] 执行数据库" + operationName + "操作\n\tSQL: " + compressSql(sql) + IntStream.range(0, parameters.length)
                .mapToObj(i -> "\n\tparameters[" + (i + 1) + "] = " + String.valueOf(parameters[i]).replaceAll("\n", " "))
                .collect(Collectors.joining())
        );
    }

}
