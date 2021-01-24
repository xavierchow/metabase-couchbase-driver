(ns metabase.driver.couchbase.parameters
  (:require [clojure.string :as cs]
            [clojure.tools.logging :as log]))

(defn parameters->value [parameters tag]
  (let [v (:value (first (filter (fn [p]
                                   (= (:target p) [:variable [:template-tag (:name tag)]])) parameters)))]
    (cond
      (= (:type tag) :text) (str "\"" v "\"")
      (= (:type tag) :number) v
      :else v)))

(defn substitute-native-parameters
  [_ query]
  (let [tags (:template-tags query)
        parameters (:parameters query)
        q (:query query)]

    (log/info (format  "substitute-native-parameters tags %s" tags))
    {:query (reduce (fn [q tag]

                      (cs/replace q (str "{{" (:name tag) "}}") (parameters->value parameters tag)))
                    q (vals tags))}))

