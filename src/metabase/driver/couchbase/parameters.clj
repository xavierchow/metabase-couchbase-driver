(ns metabase.driver.couchbase.parameters
  (:require [clojure.string :as cs]
            [clojure.tools.logging :as log]))

;; { :query "select  COUNT(*) total, o.state  from `blueOrder` o\nwhere `_type` = \"Order\" and `type` in [ \"HYPE\" ] and ANY v IN o.products SATISFIES v.articleNo = {{article}} END\nGROUP BY o.state",
;;   :template-tags {"article" {:id "c70ab780-34a7-a3b8-f3b0-dc6fdb0336fa", :name "article", :display-name "Article", :type :text}},
;;   :parameters [{:type :category, :target [:variable [:template-tag "article"]], :value "GZ8872"}]}

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

