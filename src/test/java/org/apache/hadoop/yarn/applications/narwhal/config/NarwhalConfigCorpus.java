package org.apache.hadoop.yarn.applications.narwhal.config;

/**
 * Created by zyluo on 6/7/16.
 */
public class NarwhalConfigCorpus {
    public final static String readmeInput = new StringBuilder()
            .append("{\n")
            .append("  \"name\": \"hello-docker\",\n")
            .append("  \"cpus\": 2,\n")
            .append("  \"mem\": 32,\n")
            .append("  \"instances\": 2,\n")
            .append("  \"cmd\": \"echo \\\"hello docker\\\"\",\n")
            .append("  \"engine\": {\n")
            .append("    \"type\": \"DOCKER\",\n")
            .append("    \"image\": \"centos\",\n")
            .append("    \"localImage\": true\n")
            .append("  }\n")
            .append("}")
            .toString();

    public final static String readmeOutput = new StringBuilder()
            .append("name: hello-docker\n")
            .append("cpus: 2.0\n")
            .append("mem: 32.0\n")
            .append("instances: 2\n")
            .append("cmd: echo \"hello docker\"\n")
            .append("engine_type: DOCKER\n")
            .append("engine_image: centos\n")
            .append("engine_localImage: true\n")
            .toString();
}
