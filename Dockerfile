ARG METABASE_VERSION=v0.37.4

FROM adoptopenjdk/openjdk11:jdk-11.0.9.1_1-alpine-slim as builder

ARG CLOJURE_CLI_VERSION=1.10.1.763
# the clojure installer script needs curl
RUN apk add --no-cache curl
RUN apk add --no-cache bash
ADD https://download.clojure.org/install/linux-install-${CLOJURE_CLI_VERSION}.sh ./cli-linux-install.sh
RUN chmod +x cli-linux-install.sh
RUN ./cli-linux-install.sh

# install leiningen
ADD https://raw.github.com/technomancy/leiningen/stable/bin/lein /usr/local/bin/lein
RUN chmod +x /usr/local/bin/lein

RUN apk add --no-cache git
WORKDIR /app/metabse-repo

RUN git clone https://github.com/metabase/metabase.git
RUN cd metabase && lein install-for-building-drivers

WORKDIR /app/metabase-cb-driver
# Copy the src code
COPY . ./

# ENV CLASSPATH=/app/metabase.jar
ARG LEIN_SNAPSHOTS_IN_RELEASE=true
ARG DEBUG=1
RUN lein uberjar

FROM metabase/metabase:${METABASE_VERSION} as runner

COPY --from=builder /app/metabase-cb-driver/target/uberjar/couchbase.metabase-driver.jar /plugins

