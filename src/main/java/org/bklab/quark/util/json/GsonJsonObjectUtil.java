package org.bklab.quark.util.json;

import com.google.gson.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class GsonJsonObjectUtil implements Supplier<JsonObject> {

    private final JsonObject jsonObject;

    public GsonJsonObjectUtil(String json) {
        this.jsonObject = json == null ? new JsonObject() : createGsonBuilder().create().fromJson(json, JsonObject.class);
    }

    public GsonJsonObjectUtil(JsonObject jsonObject) {
        this.jsonObject = jsonObject == null ? new JsonObject() : jsonObject;
    }

    public GsonJsonObjectUtil(Object object) {
        this(createGsonBuilder().create().toJson(object));
    }

    public static List<GsonJsonObjectUtil> fromArray(JsonArray array) {
        List<GsonJsonObjectUtil> jsonObjectUtils = new ArrayList<>();
        for (JsonElement element : array) {
            if (element.isJsonObject()) jsonObjectUtils.add(new GsonJsonObjectUtil(element.getAsJsonObject()));
        }
        return jsonObjectUtils;
    }

    public static GsonBuilder createGsonBuilder() {
        return new GsonBuilder()
                .registerTypeAdapter(LocalDateTime.class, (JsonSerializer<LocalDateTime>) (src, typeOfSrc, context) -> new JsonPrimitive(src.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))))
                .registerTypeAdapter(LocalDate.class, (JsonSerializer<LocalDate>) (src, typeOfSrc, context) -> new JsonPrimitive(src.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))))
                .registerTypeAdapter(LocalDateTime.class, (JsonDeserializer<LocalDateTime>) (json, type, jsonDeserializationContext) -> {
                    String datetime = json.getAsJsonPrimitive().getAsString();
                    return LocalDateTime.parse(datetime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                }).registerTypeAdapter(LocalDate.class, (JsonDeserializer<LocalDate>) (json, type, jsonDeserializationContext) -> {
                    String datetime = json.getAsJsonPrimitive().getAsString();
                    return LocalDate.parse(datetime, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                });
    }

    public static Optional<GsonJsonObjectUtil> createOptional(String json) {
        try {
            return Optional.ofNullable(createGsonBuilder().create().fromJson(json, JsonObject.class)).map(GsonJsonObjectUtil::new);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public static GsonJsonObjectUtil create(String json) throws JsonSyntaxException {
        return new GsonJsonObjectUtil(createGsonBuilder().create().fromJson(json, JsonObject.class));
    }

    public boolean getAsBoolean(String name) {
        return getAsBoolean(name, false);
    }

    public boolean getAsBoolean(String name, boolean defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsBoolean).orElse(defaultValue);
    }

    public int getInt(String name) {
        return getInt(name, 0);
    }

    public int getInt(String name, int defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsInt).orElse(defaultValue);
    }

    public boolean isJsonNull(String name) {
        return jsonObject.get(name).isJsonNull();
    }

    public JsonNull jsonNull(String name) {
        return jsonNull(name, null);
    }

    public JsonNull jsonNull(String name, JsonNull defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonNull).map(JsonElement::getAsJsonNull).orElse(defaultValue);
    }

    public float getFloat(String name) {
        return getFloat(name, 0f);
    }

    public float getFloat(String name, float defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsFloat).orElse(defaultValue);
    }

    public JsonArray jsonArray(String name) {
        return jsonArray(name, null);
    }

    public JsonArray jsonArray(String name, JsonArray defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonArray).map(JsonElement::getAsJsonArray).orElse(defaultValue);
    }

    public JsonPrimitive jsonPrimitive(String name) {
        return jsonPrimitive(name, null);
    }

    public JsonPrimitive jsonPrimitive(String name, JsonPrimitive defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsJsonPrimitive).orElse(defaultValue);
    }

    public String string(String name) {
        return string(name, null);
    }

    public String string(String name, String defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsString).orElse(defaultValue);
    }

    public long getLong(String name) {
        return getLong(name, 0L);
    }

    public long getLong(String name, long defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsLong).orElse(defaultValue);
    }

    public JsonObject jsonObject(String name) {
        return jsonObject(name, null);
    }

    public JsonObject jsonObject(String name, JsonObject defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonObject).map(JsonElement::getAsJsonObject).orElse(defaultValue);
    }

    public GsonJsonObjectUtil gsonJsonObjectUtil(String name, JsonObject defaultValue) {
        return new GsonJsonObjectUtil(Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonObject).map(JsonElement::getAsJsonObject).orElse(defaultValue));
    }

    public GsonJsonObjectUtil get(String name, JsonObject defaultValue) {
        return gsonJsonObjectUtil(name, defaultValue);
    }

    public GsonJsonObjectUtil gsonJsonObjectUtil(String name) {
        return new GsonJsonObjectUtil(Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonObject).map(JsonElement::getAsJsonObject).orElse(null));
    }

    public GsonJsonObjectUtil get(String name) {
        return gsonJsonObjectUtil(name);
    }

    public byte getByte(String name) {
        return getByte(name, (byte) 0);
    }

    public byte getByte(String name, byte defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsByte).orElse(defaultValue);
    }

    @Deprecated
    public char character(String name) {
        return character(name, (char) 0);
    }

    @Deprecated
    public char character(String name, char defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsCharacter).orElse(defaultValue);
    }

    public Number number(String name) {
        return number(name, null);
    }

    public Number number(String name, Number defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsNumber).orElse(defaultValue);
    }

    public double getDouble(String name) {
        return getDouble(name, 0);
    }

    public double getDouble(String name, double defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsDouble).orElse(defaultValue);
    }

    public BigDecimal bigDecimal(String name) {
        return bigDecimal(name, null);
    }

    public BigDecimal bigDecimal(String name, BigDecimal defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsBigDecimal).orElse(defaultValue);
    }

    public BigInteger bigInteger(String name) {
        return bigInteger(name, null);
    }

    public BigInteger bigInteger(String name, BigInteger defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsBigInteger).orElse(defaultValue);
    }

    public short getShort(String name) {
        return getShort(name, (short) 0);
    }

    public short getShort(String name, short defaultValue) {
        return Optional.ofNullable(jsonObject.get(name)).filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsShort).orElse(defaultValue);
    }

    public String pretty() {
        return createGsonBuilder().setPrettyPrinting().create().toJson(jsonObject);
    }

    public Map<String, String> toMap() {
        return jsonObject.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> Optional.ofNullable(e.getValue()).map(JsonElement::toString).orElse("")));
    }

    @Override
    public JsonObject get() {
        return jsonObject;
    }

    @Override
    public String toString() {
        return pretty();
    }
}
