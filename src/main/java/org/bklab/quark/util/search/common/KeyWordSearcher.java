package org.bklab.quark.util.search.common;

import dataq.core.data.schema.Record;
import org.bklab.quark.util.time.LocalDateTimeFormatter;
import org.bklab.quark.util.time.LocalDateTools;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings("Duplicates")
public class KeyWordSearcher<T> implements BiFunction<String, Collection<T>, Collection<T>>, Predicate<String> {

    private final T entity;

    public KeyWordSearcher(T entity) {
        this.entity = entity;
    }

    public KeyWordSearcher() {
        this.entity = null;
    }

    public boolean matchJava5(String keyword) {
        if (keyword == null && entity == null) return true;
        if (keyword == null || entity == null) return false;
        for (Field field : entity.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            try {
                Object o = field.get(entity);
                if (o != null && String.valueOf(o).toLowerCase().contains(keyword.toLowerCase()))
                    return true;
            } catch (IllegalAccessException ignore) {
            }
        }
        return false;
    }

    public boolean matchSpaceConditions(String keyword) {
        return keyword == null || matchConditions(keyword.replaceAll(" +", " ").split(" "));
    }

    public boolean matchConditions(String... keywords) {
        return keywords == null || Stream.of(keywords).filter(Objects::nonNull).map(String::strip).filter(a -> !a.isBlank()).allMatch(this::match);
    }

    /**
     * 二级深度搜索
     */
    public boolean match(String keyword) {
        if (keyword == null && entity == null) return true;
        if (keyword == null || entity == null) return false;
        if (entity instanceof String) return ((String) entity).contains(keyword);
        if (entity instanceof Number) return ("" + entity).contains(keyword);
        if (entity instanceof LocalDate) return (LocalDateTimeFormatter.Short((LocalDate) entity)).contains(keyword);
        if (entity instanceof LocalTime) return (LocalDateTimeFormatter.Short((LocalTime) entity)).contains(keyword);
        if (entity instanceof LocalDateTime) return (LocalDateTimeFormatter.Short((LocalDateTime) entity)).contains(keyword);
        if (entity instanceof Record) return matchKeyword((Record) entity, keyword);
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .peek(field -> field.setAccessible(true))
                .anyMatch(field -> {
                    try {
                        Object o = field.get(entity);
                        if (o == null) return false;
                        SkipSearch skipSearch = field.getAnnotation(SkipSearch.class);
                        if (skipSearch != null && skipSearch.skip()) {
                            return false;
                        }
                        if (o instanceof String || o instanceof Number)
                            return String.valueOf(o).toLowerCase().contains(keyword.toLowerCase());
                        if (o instanceof Collection || o.getClass().isEnum())
                            return o.toString().toLowerCase().contains(keyword.toLowerCase());
                        if (o instanceof LocalDate) {
                            return LocalDateTools.toChineseString((LocalDate) o).contains(keyword)
                                    || LocalDateTools.toShortString((LocalDate) o).contains(keyword);
                        }
                        if (o instanceof LocalDateTime) {
                            return DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").format((LocalDateTime) o).contains(keyword)
                                    || DateTimeFormatter.ofPattern("uuuu 年 MM月 dd日 HH时 mm分 ss秒").format((LocalDateTime) o).contains(keyword);
                        }
                        if (o instanceof Record) return matchKeyword((Record) o, keyword);
                        return new KeyWordSearcher<>(o).matchDirectly(keyword);
                    } catch (IllegalAccessException e) {
                        return false;
                    }
                });
    }

    public boolean matchKeyword(Record r, String keyword) {
        return IntStream.range(0, r.getSchema().size()).mapToObj(r::getObject)
                .anyMatch(a -> Objects.toString(a, "").contains(keyword));
    }

    /**
     * 一级快速搜索
     */
    public boolean matchDirectly(String keyword) {
        if (keyword == null || entity == null) return true;
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .peek(field -> field.setAccessible(true))
                .anyMatch(field -> {
                    try {
                        Object o = field.get(entity);
                        return o != null && String.valueOf(o).toLowerCase().contains(keyword.toLowerCase());
                    } catch (IllegalAccessException e) {
                        return false;
                    }
                });
    }

    @Override
    public Collection<T> apply(String keyword, Collection<T> ts) {
        if (keyword == null || ts == null) return ts;
        List<T> list = new ArrayList<>();
        ts.stream().filter(t -> new KeyWordSearcher<>(t).match(keyword)).forEach(list::add);
        ts.clear();
        ts.addAll(list);
        return ts;
    }

    @Override
    public boolean test(String keyword) {
        return match(keyword);
    }

    @Retention(RetentionPolicy.RUNTIME)
    public @interface SkipSearch {
        boolean skip() default true;
    }
}
