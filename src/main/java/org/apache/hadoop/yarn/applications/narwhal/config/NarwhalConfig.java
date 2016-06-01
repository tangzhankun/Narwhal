package org.apache.hadoop.yarn.applications.narwhal.config;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

public class NarwhalConfig implements Serializable {

    private final String name;

    private final double cpus;

    private final double mem;

    private final int instances;

    private final String cmd;

    private final List<String> args;

    private final EngineConfig engineConfig;

    private NarwhalConfig(Builder builder) {
        this.name = builder.name;
        this.cpus = builder.cpus;
        this.mem = builder.mem;
        this.instances = builder.instances;
        this.cmd = builder.cmd;
        this.args = builder.args;
        this.engineConfig = builder.engineConfig;
    }

    public String toString() {
        return new StringBuilder()
                .append("name: " + this.name + "\n")
                .append("cpus: " + this.cpus + "\n")
                .append("mem: " + this.mem + "\n")
                .append("instances: " + this.instances + "\n")
                .append("cmd: " + this.cmd + "\n")
                .append("engine_type: " + this.engineConfig.getType() + "\n")
                .append("engine_image: " + this.engineConfig.getImage() + "\n")
                .append("engine_localImage: " + this.engineConfig.isLocalImage() + "\n")
                .toString();
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

    public String getEngineType() {
        return engineConfig.getType();
    }

    public String getEngineImage() {
        return engineConfig.getImage();
    }

    public boolean isEngineLocalImage() {
        return engineConfig.isLocalImage();
    }

    static class Builder {

        private String name;

        private double cpus;

        private double mem;

        private Integer instances;

        private String cmd;

        private List<String> args;

        private EngineConfig engineConfig;

        Builder name(String name) throws BuilderException {
            if (name.isEmpty())
                throw new BuilderException("invalid application name");
            this.name = name;
            return this;
        }

        Builder cpus(double cpus) throws BuilderException {
            if (cpus < 0.1)
                throw new BuilderException("invalid number of cpus");
            this.cpus = cpus;
            return this;
        }

        Builder mem(double mem) throws BuilderException {
            if (mem < 0.1)
                throw new BuilderException("invalid size of memory");
            this.mem = mem;
            return this;
        }

        Builder instances(int instances) throws BuilderException {
            if (instances < 1)
                throw new BuilderException("invalid number of instances");
            this.instances = instances;
            return this;
        }

        Builder cmd(String cmd) throws BuilderException {
            if (cmd.isEmpty())
                throw new BuilderException("invalid command");
            this.cmd = cmd;
            return this;
        }

        Builder args(List<String> args) throws BuilderException {
            if (args.isEmpty())
                throw new BuilderException("invalid arguments");
            this.args = args;
            return this;
        }

        Builder engineConfig(EngineConfig engineConfig) {
            this.engineConfig = engineConfig;
            return this;
        }

        synchronized NarwhalConfig build() throws BuilderException {
            if (this.name == null)
                this.name = UUID.randomUUID().toString();
            if (this.instances == null)
                throw new BuilderException("missing number of instances");
            if (this.cmd != null && this.args != null)
                throw new BuilderException("cmd and args cannot both exist");
            if (this.engineConfig == null)
                throw new BuilderException("missing engine info");
            return new NarwhalConfig(this);
        }
    }
}
