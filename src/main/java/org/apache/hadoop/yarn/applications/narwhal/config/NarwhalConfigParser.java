package org.apache.hadoop.yarn.applications.narwhal.config;

import java.util.ArrayList;
import java.util.List;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class NarwhalConfigParser {

    public static NarwhalConfig parse(String body) throws JSONException, BuilderException {
        NarwhalConfig.Builder builder = new NarwhalConfig.Builder();
        JSONObject obj = new JSONObject(body);
        if (obj.has("name"))
            builder.name(obj.getString("name").trim());
        //TODO(zyluo): The builder has to read YARNConfig() to fill in defaults
        //             for missing values of cpus, mem -> parse(String body, YARNConfig defaults)
        if (obj.has("cpus"))
            builder.cpus(obj.getDouble("cpus"));
        if (obj.has("mem"))
            builder.mem(obj.getDouble("mem"));
        if (obj.has("instances"))
            builder.instances(obj.getInt("instances"));
        if (obj.has("cmd"))
            builder.cmd(obj.getString("cmd").trim());
        if (obj.has("args")) {
            JSONArray jsonArray = obj.getJSONArray("args");
            List<String> list = new ArrayList<>();
            for (int i=0; i<jsonArray.length(); i++) {
                String temp = jsonArray.getString(i).trim();
                if (!temp.isEmpty())
                    list.add(temp);
            }
            builder.args(list);
        }
        if (obj.has("engine")) {
            JSONObject engineObj = obj.getJSONObject("engine");
            EngineConfig engineConfig = parseEngineConfig(engineObj);
            builder.engineConfig(engineConfig);
        }
        return builder.build();
    }

    private static EngineConfig parseEngineConfig(JSONObject obj) throws JSONException, BuilderException {
        EngineConfig.Builder engineBuilder = new EngineConfig.Builder();
        if (obj.has("type"))
            engineBuilder.type(obj.getString("type").trim());
        if (obj.has("image"))
            engineBuilder.image(obj.getString("image").trim());
        if (obj.has("localImage"))
            engineBuilder.localImage(obj.getBoolean("localImage"));
        if (obj.has("volumes")) {
            JSONArray volumesObjArray = obj.getJSONArray("volumes");
            for (int i = 0; i < volumesObjArray.length(); i++) {
                JSONObject volumeObj = volumesObjArray.getJSONObject(i);
                engineBuilder.addVolume(new VolumeConfig.Builder()
                        .containerPath(volumeObj.getString("containerPath"))
                        .hostPath(volumeObj.getString("hostPath"))
                        .mode(volumeObj.getString("mode")).build());
            }
        }
        return engineBuilder.build();
    }
}
