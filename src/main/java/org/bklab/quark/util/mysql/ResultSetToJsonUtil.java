package org.bklab.quark.util.mysql;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.codec.binary.Hex;
import org.bklab.quark.entity.dao.ResultSetHelper;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.List;
import java.util.Optional;

public class ResultSetToJsonUtil {

    private final ResultSet resultSet;
    private final ResultSetMetaData metaData;

    public ResultSetToJsonUtil(ResultSet resultSet) throws Exception {
        this(resultSet, resultSet.getMetaData());
    }

    public ResultSetToJsonUtil(ResultSet resultSet, ResultSetMetaData metaData) {
        this.resultSet = resultSet;
        this.metaData = metaData;
    }

    public JsonObject toJsonObject() throws Exception {
        return getJsonObjects().stream().findFirst().orElse(null);
    }

    public JsonArray toJsonArray() throws Exception {
        return getJsonObjects().stream().collect(JsonArray::new, JsonArray::add, JsonArray::addAll);
    }

    private List<JsonObject> getJsonObjects() throws Exception {
        return new ResultSetHelper(resultSet).asList(r -> {
            JsonObject jsonObject = new JsonObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                jsonObject.add(metaData.getColumnName(i), fieldToJsonElement(r, metaData, i).orElse(null));
            }
            return jsonObject;
        });
    }

    private Optional<JsonElement> fieldToJsonElement(final ResultSet resultSet, final ResultSetMetaData metaData, final int column) throws SQLException {
        final int columnType = metaData.getColumnType(column);
        final Optional<JsonElement> jsonElement;
        switch (columnType) {
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.DECIMAL:
            case Types.NUMERIC:
                jsonElement = Optional.ofNullable((Number) resultSet.getObject(column)).map(JsonPrimitive::new);
                break;
            case Types.CHAR:
                throw new UnsupportedOperationException("TODO: " + JDBCType.valueOf(columnType));
            case Types.BLOB: {
                jsonElement = Optional.ofNullable(resultSet.getBlob(column)).map(this::convertInputStream).map(JsonPrimitive::new);
                break;
            }
            case Types.NCLOB:
            case Types.CLOB: {
                jsonElement = Optional.ofNullable(resultSet.getClob(column)).map(this::convertInputStream).map(JsonPrimitive::new);
                break;
            }
            default:
                jsonElement = Optional.ofNullable(resultSet.getString(column)).map(JsonPrimitive::new);
        }
        return jsonElement;
    }

    private String convertInputStream(Clob blob) {
        try {
            return convertInputStream(blob.getAsciiStream());
        } catch (Exception e) {
            return "";
        }
    }

    private String convertInputStream(Blob blob) {
        try {
            return convertInputStream(blob.getBinaryStream());
        } catch (Exception e) {
            return "";
        }
    }

    private String convertInputStream(InputStream inputStream) {
        try {
            return new String(Hex.encodeHex(inputStream.readAllBytes()));
        } catch (IOException e) {
            return "";
        }
    }
}
