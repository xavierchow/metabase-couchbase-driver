(ns metabase.driver.couchbase.parameters
  (:require [clojure.string :as cs]
            [clojure.tools.logging :as log]
            [metabase.mbql.util :as mbql.u]))

(defn build-tag-value [tag v]
  (cond
    (= (:type tag) :text)      (str "\"" v "\"")
    (= (:type tag) :number)    v
    (= (:type tag) :dimension) (str "[" (clojure.string/join "," (map #(str "\"" % "\"") v)) "]")
    :else                      v)
  )
(defn parameters->value [parameters tag]
  (let [parameter (first (filter (fn [p]
                                   (= (:name tag) (mbql.u/match-one (:target p) [_ [:template-tag n]] n )) ) parameters))

        v (:value parameter)]
    (if (nil? v)
      (build-tag-value tag (:default tag))
      (build-tag-value tag v))
    ))

(defn substitute-native-parameters
  [_ query]
  (let [tags (:template-tags query)
        parameters (:parameters query)
        q (:query query)]

    (log/info (format  "substitute-native-parameters tags %s" tags))
    {:query (reduce (fn [q tag]

                      (cs/replace q (str "{{" (:name tag) "}}") (parameters->value parameters tag)))
                    q (vals tags))}))

