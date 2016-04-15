package org.apache.hadoop.yarn.applications.narwhal;

import org.apache.hadoop.yarn.applications.narwhal.config.BuilderException;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfig;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfigBuilder;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfigParser;
import org.codehaus.jettison.json.JSONException;

/**
 * This is the Narwhal Client for submitting the AM
 */
public class NClient {
    public void run(String body) {
        NarwhalConfigBuilder builder = new NarwhalConfigBuilder();
        try {
            new NarwhalConfigParser(builder).parse(body);
            NarwhalConfig config = builder.build();
            System.out.println(config.getName());
            System.out.println(config.getCpus());
            System.out.println(config.getMem());
            System.out.println(config.getInstances());
            System.out.println(config.getCmd());
            System.out.println(config.getArgs());
            System.out.println(config.getImage());
            System.out.println(config.isLocal());
        } catch (BuilderException e) {
            //TODO
        } catch (JSONException je) {
            //TODO
        }
    }
}
