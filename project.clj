(defproject metabase/couchbase-driver "1.0.9"
  :min-lein-version "2.5.0"

  :dependencies
  [[com.jayway.jsonpath/json-path "2.4.0"]
   [earthen/clj-cb "0.3.1"]
   [trptcolin/versioneer "0.2.0"]]

  :jvm-opts
  ["-XX:+IgnoreUnrecognizedVMOptions"
   "--add-modules java.xml.bind"]

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     [metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot :all
    :omit-source true
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "couchbase.metabase-driver.jar"}})
