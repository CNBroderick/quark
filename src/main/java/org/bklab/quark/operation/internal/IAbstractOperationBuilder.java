package org.bklab.quark.operation.internal;

import dataq.core.operation.AbstractOperation;
import dataq.core.operation.OperationContext;
import dataq.core.operation.OperationResult;
import org.bklab.quark.util.json.GsonJsonObjectUtil;
import org.bklab.quark.util.security.ExamineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface IAbstractOperationBuilder<E extends IAbstractOperationBuilder<E>> {

    Map<String, Object> getParameterMap();

    List<BiConsumer<AbstractOperation, Exception>> getExceptionConsumers();

    default void beforeInit() throws Exception {

    }

    default E addCallingClass() {
        StackTraceElement stackTraceElement = findCallingClass(new Throwable().getStackTrace(), 1);

        if (stackTraceElement == null) {
            getParameterMap().remove("CALLING-OPERATION-POSITION");
            getParameterMap().remove("CALLING-OPERATION-FILE");
            getParameterMap().remove("CALLING-OPERATION-LINE");
            return thisObject();
        }

        getParameterMap().put("CALLING-OPERATION-POSITION", stackTraceElement.getClassName() + "." + stackTraceElement.getMethodName() + ":" + stackTraceElement.getLineNumber());
        getParameterMap().put("CALLING-OPERATION-FILE", stackTraceElement.getFileName());
        getParameterMap().put("CALLING-OPERATION-LINE", stackTraceElement.getLineNumber());
        return thisObject();
    }

    private StackTraceElement findCallingClass(StackTraceElement[] stackTraceElements, int position) {
        if (position < 0 || stackTraceElements.length <= position) return null;
        StackTraceElement stackTraceElement = stackTraceElements[position];
        return !stackTraceElement.getClass().isAssignableFrom(getClass())
               ? stackTraceElement : findCallingClass(stackTraceElements, position + 1);
    }

    default String getParameterMapPrettyJson(OperationContext operationContext) {
        return new GsonJsonObjectUtil(getParameterMap(operationContext)).pretty();
    }

    default Map<String, Object> getParameterMap(OperationContext operationContext) {
        try {
            Field paraMap = OperationContext.class.getDeclaredField("paraMap");
            paraMap.setAccessible(true);
            @SuppressWarnings("unchecked") Map<String, Object> parameterMap = (Map<String, Object>) paraMap.get(operationContext);
            if (parameterMap != null) return parameterMap;
        } catch (Exception e) {
            logger().trace("获取OperationContext.paraMap失败", e);
        }
        return Collections.emptyMap();
    }

    default void callExceptionConsumers(AbstractOperation operation, Exception exception) {
        getExceptionConsumers().forEach(a -> {
            try {
                a.accept(operation, exception);
            } catch (Exception e) {
                String exceptionString = createExceptionString(exception);
                String calc = ExamineUtil.md5().calc(exceptionString);
                logger().warn("处理异常[" + calc + "]时发生错误，待处理异常：\n", exception);
                logger().warn("处理异常[" + calc + "]时发生错误，新抛出异常：\n", e);
            }
        });
    }

    default String createExceptionString(Exception exception) {
        StringWriter writer = new StringWriter();
        exception.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    default Logger logger() {
        return LoggerFactory.getLogger(getClass());
    }

    default void beforeInitSafely() {
        try {
            beforeInit();
        } catch (Exception e) {
            LoggerFactory.getLogger(getClass()).trace("ignore errors: ", e);
        }
    }

    default E thisObject() {
        //noinspection unchecked
        return (E) this;
    }

    default E add(String name, Object value) {
        getParameterMap().put(name, value);
        return thisObject();
    }

    default E mv(String oldName, String newName) {
        getParameterMap().put(newName, getParameterMap().get(oldName));
        getParameterMap().remove(oldName);
        return thisObject();
    }

    default E rm(String... names) {
        for (String name : names) {
            getParameterMap().remove(name);
        }
        return thisObject();
    }

    default E cp(String oldName, String newName) {
        getParameterMap().put(newName, getParameterMap().get(oldName));
        return thisObject();
    }

    default E add(Map<String, Supplier<Object>> map) {
        map.forEach((k, v) -> Optional.ofNullable(v).map(Supplier::get)
                .map(a -> a instanceof String ? ((String) a).trim() : a)
                .filter(a -> !(a instanceof String) || !((String) a).isBlank())
                .ifPresent(o -> getParameterMap().put(k, o)));
        return thisObject();
    }

    default E put(Map<String, Object> map) {
        getParameterMap().putAll(map);
        return thisObject();
    }

    default <T> Optional<T> getOptionalValue(HasAbstractOperation hasAbstractOperation) {
        return createAndExecute(hasAbstractOperation).map(OperationResult::asObject);
    }

    default <T> T executeQuery(HasAbstractOperation abstractOperation, Supplier<T> orElseGet) {
        return this.<T>getOptionalValue(abstractOperation).orElseGet(orElseGet);
    }

    default <T> List<T> executeQueryList(HasAbstractOperation hasAbstractOperation) {
        return this.createAndExecute(hasAbstractOperation).map(OperationResult::<T>asList).orElse(Collections.emptyList());
    }

    default <T> Collection<T> executeQueryCollection(HasAbstractOperation hasAbstractOperation) {
        return this.createAndExecute(hasAbstractOperation).map(OperationResult::<Collection<T>>asObject).orElse(Collections.emptyList());
    }

    default <T> T execute(HasAbstractOperation abstractOperation) {
        return createAndExecute(abstractOperation).<T>map(OperationResult::asObject).orElse(null);
    }

    default boolean execute(HasAbstractOperation abstractOperation, Consumer<OperationResult> successConsumer) {
        return createAndExecute(abstractOperation).stream()
                .peek(operationResult -> operationResult.ifSuccess(successConsumer))
                .map(OperationResult::isSuccess).findFirst().orElse(false);
    }

    default boolean executeUpdate(HasAbstractOperation abstractOperation) {
        return createAndExecute(abstractOperation).map(OperationResult::isSuccess).orElse(false);
    }

    private Optional<OperationResult> createAndExecute(HasAbstractOperation hasAbstractOperation) {
        AbstractOperation operation = create0(hasAbstractOperation);
        beforeExecuteOperation(hasAbstractOperation, operation);
        Optional<OperationResult> operationResult = execute(operation);
        afterExecuteOperation(hasAbstractOperation, operation);
        return operationResult;
    }

    private Optional<OperationResult> execute(AbstractOperation abstractOperation) {
        try {
            OperationResult operationResult = abstractOperation.execute();
            checkOperationResultException(abstractOperation, operationResult);
            if (operationResult.isSuccess()) return Optional.of(operationResult);
        } catch (Exception e) {
            callExceptionConsumers(abstractOperation, e);
        }
        return Optional.empty();
    }

    default void beforeExecuteOperation(HasAbstractOperation hasAbstractOperation, AbstractOperation abstractOperation) {

    }

    default void afterExecuteOperation(HasAbstractOperation hasAbstractOperation, AbstractOperation abstractOperation) {

    }

    default AbstractOperation create(HasAbstractOperation hasAbstractOperation) {
        AbstractOperation operation = create0(hasAbstractOperation);
        beforeExecuteOperation(hasAbstractOperation, operation);
        return operation;
    }

    private AbstractOperation create0(HasAbstractOperation abstractOperation) {
        return abstractOperation.createAbstractOperation(addCallingClass().getParameterMap());
    }

    default E peek(Consumer<E> consumer) {
        consumer.accept(thisObject());
        return thisObject();
    }

    default E compute(boolean value, Consumer<E> consumer) {
        if (value) consumer.accept(thisObject());
        return thisObject();
    }

    default <T> T getParameterMapValue(String key) {
        //noinspection unchecked
        return (T) getParameterMap().get(key);
    }

    private void checkOperationResultException(AbstractOperation abstractOperation, OperationResult operationResult) {
        checkOperationResultException(abstractOperation, operationResult, null);
    }

    private void checkOperationResultException(AbstractOperation abstractOperation, OperationResult operationResult, Exception e) {
        if (operationResult != null && operationResult.isException()) {
            LoggerFactory.getLogger(operationResult.getClass()).error(
                    "执行失败[" + abstractOperation.getContext().getOperationName() + '-'
                    + abstractOperation.getClass().getName() + "]", operationResult.getException());
            callExceptionConsumers(abstractOperation, operationResult.getException());
        }
        if (e != null) LoggerFactory.getLogger(getClass()).error("执行[" + abstractOperation.getContext().getOperationName() + "]失败。", e);
    }

    default E addExceptionConsumer(BiConsumer<AbstractOperation, Exception> exceptionBiConsumer) {
        getExceptionConsumers().add(exceptionBiConsumer);
        return thisObject();
    }
}
