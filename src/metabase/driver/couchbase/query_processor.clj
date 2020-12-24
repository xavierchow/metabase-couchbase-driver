(ns metabase.driver.couchbase.query-processor
  (:refer-clojure :exclude [==])
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [metabase.driver.couchbase.util :as cu]
            )
  (:import [com.jayway.jsonpath JsonPath Predicate]))

(declare compile-expression compile-function)

(defn json-path
  [query body]
  (JsonPath/read body query (into-array Predicate [])))

(defn compile-function
  [[operator & arguments]]
  (case (keyword operator)
    :count count
    :sum   #(reduce + (map (compile-expression (first arguments)) %))
    :float #(Float/parseFloat ((compile-expression (first arguments)) %))
    (throw (Exception. (str "Unknown operator: " operator)))))

(defn compile-expression
  [expr]
  (cond
    (string? expr)  (partial json-path expr)
    (number? expr)  (constantly expr)
    (vector? expr)  (compile-function expr)
    :else           (throw (Exception. (str "Unknown expression: " expr)))))

(defn aggregate
  [rows metrics breakouts]
  (let [breakouts-fns (map compile-expression breakouts)
        breakout-fn   (fn [row] (for [breakout breakouts-fns] (breakout row)))
        metrics-fns   (map compile-expression metrics)]
    (for [[breakout-key breakout-rows] (group-by breakout-fn rows)]
      (concat breakout-key (for [metrics-fn metrics-fns]
                             (metrics-fn breakout-rows))))))

(defn extract-fields
  [rows fields]
  (let [fields-fns (map compile-expression fields)]
    (for [row rows]
      (for [field-fn fields-fns]
        (field-fn row)))))

(defn extract-columns
  [rows]
  (into [] (map #(hash-map :name (name %)) (keys (first rows)))))

(defn key-names
  [rows]
  (into [] (map name (keys (first rows)))))

; (key-names [{:amount 4500, :sku "2825"}])
; (extract-columns [{:amount 4500, :sku "2825"}])

(defn extract-values
  [rows cols]
  (into [] (map (fn[r] (into [] (map #(get r (keyword %)) cols))) rows)))

(extract-values [{:amount 4500, :sku "2825"}] ["amount" "sku"])

(defn execute-query [conn native-query respond]
  (log/info "native-query" native-query)
  (let [stmt    (:query native-query)
        result  (cu/n1ql-query conn stmt)
        rows    (:rows result)
        columns (or (:cols native-query) (key-names rows))]
    (log/info (format "columns %s" columns))
    (log/info (format  "query-result %s" (:rows result)))
    (respond {:cols (into [] (map #(hash-map :name %) columns))}
             (extract-values rows columns))))
