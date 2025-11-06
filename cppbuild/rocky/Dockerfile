ARG VERSION="9"
FROM rockylinux:${VERSION} AS builder

RUN yum update -y && yum install -y https://cdn.azul.com/zulu/bin/zulu-repo-1.0.0-1.noarch.rpm && \
    yum update -y && yum install -y \
    tar \
    zlib-devel \
    libuuid-devel \
    git \
    findutils \
    openssl-devel \
    zulu17-jdk

RUN yum groupinstall 'Development Tools' -y

ENV JAVA_HOME=/usr/lib/jvm/java-17-zulu-openjdk \
    BUILD_JAVA_HOME=/usr/lib/jvm/java-17-zulu-openjdk \
    BUILD_JAVA_VERSION=17 \
    GRADLE_OPTS="-Dorg.gradle.daemon=false -Dorg.gradle.java.installations.auto-detect=false -Dorg.gradle.warning.mode=fail"

ARG USER_ID="1000"
ARG GROUP_ID="1000"
RUN groupadd --gid $GROUP_ID --non-unique --system athena
RUN adduser --uid $USER_ID --system --create-home --gid $GROUP_ID athena

USER athena
WORKDIR /opt/aeron

FROM builder AS essentials-test
ENTRYPOINT cppbuild/cppbuild --c-warnings-as-errors --cxx-warnings-as-errors --package
