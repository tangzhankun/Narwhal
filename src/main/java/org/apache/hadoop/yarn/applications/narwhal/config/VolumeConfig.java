package org.apache.hadoop.yarn.applications.narwhal.config;

import java.io.Serializable;
import java.util.Arrays;

public class VolumeConfig implements Serializable{

    private final String containerPath;

    private final String hostPath;

    private final String mountMode;

    private VolumeConfig(Builder builder) {
        this.containerPath = builder.containerPath;
        this.hostPath = builder.hostPath;
        this.mountMode = builder.mountMode;
    }

    String getContainerPath() {
        return containerPath;
    }

    String getHostPath() {
        return hostPath;
    }

    String getMountMode() {
        return mountMode;
    }

    static class Builder {

        private static final String READ_ONLY_MOUNT = "RO";

        private static final String[] MOUNT_MODES = {READ_ONLY_MOUNT};

        private String containerPath;

        private String hostPath;

        private String mountMode;

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

        Builder mountMode(String mountMode) throws BuilderException {
            if (!Arrays.asList(MOUNT_MODES).contains(mountMode))
                throw new BuilderException("Invalid mount mode");
            this.mountMode = mountMode;
            return this;
        }

        synchronized VolumeConfig build() throws BuilderException {
            return new VolumeConfig(this);
        }

    }
}
