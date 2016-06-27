package org.apache.hadoop.yarn.applications.narwhal.config;

import java.io.Serializable;
import java.util.Arrays;

public class VolumeConfig implements Serializable{

    private final String containerPath;

    private final String hostPath;

    private final String mode;

    private VolumeConfig(Builder builder) {
        this.containerPath = builder.containerPath;
        this.hostPath = builder.hostPath;
        this.mode = builder.mode;
    }

    String getContainerPath() {
        return containerPath;
    }

    String getHostPath() {
        return hostPath;
    }

    String getMode() {
        return mode;
    }

    static class Builder {

        private static final String READ_ONLY = "RO";

        private static final String[] MODES = {READ_ONLY};

        private String containerPath;

        private String hostPath;

        private String mode;

        Builder containerPath(String containerPath) throws BuilderException {
            if (containerPath.isEmpty())
                throw new BuilderException("Invalid container path");
            this.containerPath = containerPath;
            return this;
        }

        Builder hostPath(String hostPath) throws BuilderException {
            if (hostPath.isEmpty())
                throw new BuilderException("Invalid host path");
            this.hostPath = hostPath;
            return this;
        }

        Builder mode(String mode) throws BuilderException {
            if (!Arrays.asList(MODES).contains(mode))
                throw new BuilderException("Invalid mount mode");
            this.mode = mode;
            return this;
        }

        synchronized VolumeConfig build() throws BuilderException {
            return new VolumeConfig(this);
        }

    }
}
