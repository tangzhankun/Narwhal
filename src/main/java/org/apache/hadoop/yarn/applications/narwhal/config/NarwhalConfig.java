package org.apache.hadoop.yarn.applications.narwhal.config;

import java.io.Serializable;
import java.util.List;

public class NarwhalConfig implements Serializable {

    private String name;

    private double cpus;

    private double mem;

    private int instances;

    private String cmd;

    private List<String> args;

    private String image;

    private boolean local;

    NarwhalConfig(String name, double cpus, double mem, int instances, String cmd, List<String> args, String image, boolean local) {
        this.name = name;
        this.cpus = cpus;
        this.mem = mem;
        this.instances = instances;
        this.cmd = cmd;
        this.args = args;
        this.image = image;
        this.local = local;
    }

    public String getName() {
        return name;
    }

    public double getCpus() {
        return cpus;
    }

    public double getMem() {
        return mem;
    }

    public int getInstances() {
        return instances;
    }

    public String getCmd() {
        return cmd;
    }

    public List<String> getArgs() {
        return args;
    }

    public String getImage() {
        return image;
    }

    public boolean isLocal() {
        return local;
    }
}
