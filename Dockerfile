ARG JOBS=6
ARG PROFILE=sona
ARG TAG=11.0.17-11.60.19
# First stage is the build environment
FROM registry.gitlab.com/sonaproject/zulu-openjdk:${TAG} as builder
MAINTAINER Jian Li <gunine@sk.com>

# Set the environment variables
ENV HOME /root
ENV BUILD_NUMBER docker
ENV JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8
ENV ONOS_VERSION 2.7
ENV ONOS_BRANCH onos-2.7

# Install dependencies
ENV BUILD_DEPS \
    ca-certificates \
    zip \
    python \
    python3 \
    git \
    bzip2 \
    build-essential \
    curl \
    unzip

RUN apt-get update && apt-get install git-review -y ${BUILD_DEPS}

# Install Bazelisk, which will download the version of bazel specified in
# .bazelversion
ARG TARGETOS TARGETARCH
RUN curl -L -o bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.29.0/bazelisk-linux-${TARGETARCH}
RUN chmod +x bazelisk && mv bazelisk /usr/bin

# Copy in the source
RUN mkdir -p /src/onos
COPY . /src/onos/

#RUN ls /src/onos
# Remove SONA apps sources
RUN rm -rf /src/onos/apps/openstack*
RUN rm -rf /src/onos/apps/k8s-*

COPY sona.bzl /src/onos/tools/build/bazel/sona.bzl

RUN rm -rf /src/onos/BUILD
COPY BUILD-sona /src/onos/BUILD

RUN sed -i 's/modules.bzl/sona.bzl/g' /src/onos/BUILD

# Download and patch ONOS core changes which affect ONOS
RUN git clone https://github.com/sonaproject/onos-sona-patch.git patch && \
    cp patch/${ONOS_VERSION}/*.patch /src/onos/ && \
    cp patch/patch.sh /src/onos/

# Build ONOS
# We extract the tar in the build environment to avoid having to put the tar
# in the runtime environment - this saves a lot of space
# FIXME - dependence on ONOS_ROOT and git at build time is a hack to work around
# build problems
WORKDIR /src/onos
RUN ./patch.sh

# Download latest SONA app sources
RUN mkdir -p /tmp/onos
COPY . /tmp/onos/
RUN cp -R /tmp/onos/apps/openstack* /src/onos/apps && \
    cp -R /tmp/onos/apps/k8s-* /src/onos/apps && \
    cp -R /tmp/onos/apps/kubevirt* /src/onos/apps

ARG JOBS
ARG PROFILE

WORKDIR /src/onos

RUN git log -10

RUN rm -rf /src/onos/WORKSPACE
COPY WORKSPACE /src/onos/WORKSPACE

RUN rm -rf /src/onos/tools/gui/package.json
COPY package.json /src/onos/tools/gui/package.json

RUN cat WORKSPACE-docker >> WORKSPACE && bazelisk build onos \
    --jobs ${JOBS} \
    --verbose_failures \
    --java_runtime_version=dockerjdk_11 \
    --tool_java_runtime_version=dockerjdk_11 \
    --define profile=${PROFILE}

# We extract the tar in the build environment to avoid having to put the tar in
# the runtime stage. This saves a lot of space.
RUN mkdir /output
RUN tar -xf bazel-bin/onos.tar.gz -C /output --strip-components=1

# Second stage is the runtime environment
FROM registry.gitlab.com/sonaproject/zulu-openjdk:${TAG}-jre

LABEL org.label-schema.name="ONOS" \
      org.label-schema.description="SDN Controller" \
      org.label-schema.usage="http://wiki.onosproject.org" \
      org.label-schema.url="http://onosproject.org" \
      org.label-scheme.vendor="Open Networking Foundation" \
      org.label-schema.schema-version="1.0"

RUN apt-get update -y && \
        apt-get install wget curl libhyperic-sigar-java -y

COPY lib/libsigar-aarch64-linux.so /root/onos/apache-karaf-4.2.14/lib/libsigar-aarch64-linux.so

# Install ONOS in /root/onos
COPY --from=builder /output/ /root/onos/
WORKDIR /root/onos

# Ports
# 6653 - OpenFlow
# 6640 - OVSDB
# 8181 - GUI
# 8101 - ONOS CLI
# 9876 - ONOS intra-cluster communication
EXPOSE 6653 6640 8181 8101 9876 9300

RUN   touch apps/org.onosproject.gui/active && \
      touch apps/org.onosproject.drivers/active && \
      touch apps/org.onosproject.drivers.ovsdb/active && \
      touch apps/org.onosproject.openflow-base/active && \
      touch apps/org.onosproject.openstacknetworking/active && \
      touch apps/org.onosproject.openstacktroubleshoot/active && \
      #touch apps/org.onosproject.k8s-networking/active && \
      touch apps/org.onosproject.kubevirt-networking/active

# Get ready to run command
ENTRYPOINT ["./bin/onos-service"]
CMD ["server"]
