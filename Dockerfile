ARG METABASE_VERSION=v0.37.4

FROM metabase/metabase:${METABASE_VERSION} as builder
ARG CLOJURE_CLI_VERSION=1.10.1.763

WORKDIR /app/metabase-cb-driver

# the clojure installer script needs curl
RUN apk add --no-cache curl
ADD https://download.clojure.org/install/linux-install-${CLOJURE_CLI_VERSION}.sh ./cli-linux-install.sh
RUN chmod +x cli-linux-install.sh
RUN ./cli-linux-install.sh


ADD https://raw.github.com/technomancy/leiningen/stable/bin/lein /usr/local/bin/lein
RUN chmod +x /usr/local/bin/lein
RUN lein upgrade

# Copy the src code
COPY . ./

RUN lein uberjar

FROM metabase/metabase:${METABASE_VERSION} as runner

COPY --from=builder /app/metabase-cb-driver/target/uberjar/couchbase.metabase-driver.jar /plugins
  
