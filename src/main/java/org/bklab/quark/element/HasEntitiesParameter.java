package org.bklab.quark.element;

import dataq.core.operation.OperationContext;
import org.bklab.quark.util.mysql.InflectWord;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public interface HasEntitiesParameter<T> {

    default List<T> getEntityList(OperationContext operationContext, String entityName) {
        return new ArrayList<>(getEntities(operationContext, entityName));
    }

    default List<T> getEntityList(OperationContext operationContext, String entityName, String entitiesName) {
        return new ArrayList<>(getEntities(operationContext, entityName, entitiesName));

    }

    default Collection<T> getEntities(OperationContext operationContext, String entityName) {
        return getEntities(operationContext, entityName, InflectWord.getInstance().pluralize(entityName));
    }

    default Collection<T> getEntities(OperationContext operationContext, String entityName, String entitiesName) {
        List<T> list = new ArrayList<>();
        getEntities(operationContext, entityName, entitiesName, list::add, list::addAll);
        return list;
    }

    default void getEntities(OperationContext operationContext, String entityName, String entitiesName, Consumer<T> entityConsumer, Consumer<Collection<T>> entitiesConsumer) {
        T entity = operationContext.getObject(entityName);
        Optional.ofNullable(entity).ifPresent(entityConsumer);

        if (operationContext.getObject(entitiesName) instanceof Collection<?>) {
            Collection<T> entities = operationContext.getObject(entitiesName);
            Optional.ofNullable(entities).map(r -> r.stream().filter(Objects::nonNull)
                    .collect(Collectors.toList())).ifPresent(entitiesConsumer);
        }
    }
}
