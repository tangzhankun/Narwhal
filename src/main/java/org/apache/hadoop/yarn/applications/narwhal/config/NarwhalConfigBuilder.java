package org.apache.hadoop.yarn.applications.narwhal.config;

import java.util.List;

public class NarwhalConfigBuilder {
    private String name;

    private double cpus;

    private double mem;

    private int instances;

    private String cmd;

    private List<String> args;

    private String image;

    private boolean localImage;

    public NarwhalConfig build() throws BuilderException {
        check();
        return new NarwhalConfig(name, cpus, mem, instances, cmd, args, image, localImage);
    }

    private void check() throws BuilderException {
        //TODO(zyluo): The builder has to read YARNConfig() to fill in defaults
        //for missing values of name, cpus, mem, instances
        if (cmd != null && args != null)
            throw new BuilderException("cmd and args cannot both exist");

        if (cmd == null && args.isEmpty())
            throw new BuilderException("no args");

        if (args == null && cmd.isEmpty())
            throw new BuilderException("empty command");

        //TODO(zyluo): image cannot be empty
    }

    String getName() {
        return name;
    }

    void setName(String name) {
        this.name = name.trim();
    }

    double getCpus() {
        return cpus;
    }

    void setCpus(double cpus) {
        this.cpus = cpus;
    }

    double getMem() {
        return mem;
    }

    void setMem(double mem) {
        this.mem = mem;
    }

    int getInstances() {
        return instances;
    }

    void setInstances(int instances) {
        this.instances = instances;
    }

    String getCmd() {
        return cmd;
    }

    void setCmd(String cmd) {
        this.cmd = cmd.trim();
    }

    List<String> getArgs() {
        return args;
    }

    void setArgs(List<String> args) {
        this.args = args;
    }

    String getImage() {
        return image;
    }

    void setImage(String image) {
        this.image = image.trim();
    }

    boolean isLocalImage() {
        return localImage;
    }

    void setLocalImage(boolean localImage) {
        this.localImage = localImage;
    }

}
