package com.google.cloud.spanner.pgadapter.metadata;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JSONUtils {
    /**
     * Simple getter which transforms the result into the desired type.
     *
     * @param jsonObject The input object.
     * @param key The reference key.
     * @return The JSON value referenced by the key in String type.
     */
    public static String getJSONString(JSONObject jsonObject, String key) {
        return getJSONObject(jsonObject, key).toString();
    }

    /**
     * Simple getter which transforms the result into the desired type.
     *
     * @param jsonObject The input object.
     * @param key The reference key.
     * @return The JSON value referenced by the key in JSONArray type.
     */
    public static JSONArray getJSONArray(JSONObject jsonObject, String key) {
        return (JSONArray) getJSONObject(jsonObject, key);
    }

    /**
     * Simple getter which transforms the result into the desired type. If the key does not exist,
     * throw an exception.
     *
     * @param jsonObject The input object.
     * @param key The reference key.
     * @return The JSON value referenced by the key in JSONObject type.
     * @throws IllegalArgumentException If the key doesn't exist.
     */
    public static Object getJSONObject(JSONObject jsonObject, String key) {
        Object result = jsonObject.get(key);
        if (result == null) {
            throw new IllegalArgumentException(
                    String.format("A '%s' key must be specified in the JSON definition.", key)
            );
        }
        return result;
    }
}
