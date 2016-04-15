package org.apache.hadoop.yarn.applications.narwhal.config;

import java.util.ArrayList;
import java.util.List;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class NarwhalConfigParser {
    private NarwhalConfigBuilder builder;

    public NarwhalConfigParser(NarwhalConfigBuilder builder) {
        this.builder = builder;
    }

    public void parse(String body) throws JSONException {
        JSONObject obj = new JSONObject(body);
        if (obj.has("name"))
            builder.setName(obj.getString("name"));
        if (obj.has("cpus"))
            builder.setCpus(obj.getDouble("cpus"));
        if (obj.has("mem"))
            builder.setMem(obj.getDouble("mem"));
        if (obj.has("instances"))
            builder.setInstances(obj.getInt("instances"));
        if (obj.has("cmd"))
            builder.setCmd(obj.getString("cmd"));
        if (obj.has("args")) {
            JSONArray jsonArray = obj.getJSONArray("args");
            List<String> list = new ArrayList<>();
            for (int i=0; i<jsonArray.length(); i++) {
                list.add(jsonArray.getString(i));
            }
            builder.setArgs(list);
        }
        if (obj.has("image"))
            builder.setImage(obj.getString("image"));
        if (obj.has("local"))
            builder.setLocal(obj.getBoolean("local"));
    }
}
