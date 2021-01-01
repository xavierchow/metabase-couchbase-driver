(ns metabase.driver.couchbase
  "Couchbase  driver, since it's nosql database which doesn't 1-to-1 map to the db/table like relational db,
   here is the mapping releationship: `database` -> `couchbase bucket`, `table` -> `document with certian _type`"
  (:require [clojure.tools.logging :as log]
            [metabase.driver :as driver]
            [metabase.models.metric :as metric :refer [Metric]]
            [metabase.driver.couchbase.query-processor :as couchbase.qp]
            [metabase.driver.couchbase.util :as cu]
            [metabase.query-processor.store :as qp.store]
            [metabase.util :as u]
            [metabase.driver.couchbase.parameters :as parameters]))

(defn find-first
  [f coll]
  (first (filter f coll)))

(defn table-def->field
  [table-def name]
  (find-first #(= (:name %) name) (:fields table-def)))

(defn mbql-field->expression
  [table-def expr]
  (let [field (table-def->field table-def (:field-name expr))]
    (or (:expression field) (:name field))))

(defn mbql-aggregation->aggregation
  [table-def mbql-aggregation]
  (if (:field mbql-aggregation)
    [(:aggregation-type mbql-aggregation)
     (mbql-field->expression table-def (:field mbql-aggregation))]
    [(:aggregation-type mbql-aggregation)]))

(def json-type->base-type
  {:string  :type/Text
   :number  :type/Float
   :boolean :type/Boolean})

(defn name->base-type
  "use name to build the base-type, e.g. `Datetime` -> `:type/Datetime`"
  [name]
  (if name (keyword (str "type/" name)) nil))


(driver/register! :couchbase)

(defmethod driver/supports? [:couhbase :basic-aggregations] [_ _] false)
(defmethod driver/supports? [:couchbase :native-parameters] [_ _]  true)
(defmethod driver/supports? [:couhbase :set-timezone] [_ _] true)

(defmethod driver/can-connect? :couchbase [_ details]
  (couchbase.qp/ping (cu/getconn details)))

(defmethod driver/describe-database :couchbase [_ database]
  (log/info (format  "describe-database:  %s" (:details database)))
  (cu/getconn (:details database))
  (let [table-defs (cu/database-table-defs database)]
    {:tables (set (for [table-def table-defs]
                    {:name   (:name table-def)
                     :schema (:schema table-def)}))}))

(defmethod driver/describe-table :couchbase [_ database table]
  (log/info (format  "describe-table: %s" (:name table)))
  (let [table-def  (cu/database-table-def database (:name table))]
    {:name   (:name table-def)
     :schema (:schema table-def)
     :fields (set (for [field (:fields table-def)]
                    {:name          (:name field)
                     :database-type (:type field)
                     :database-position (:database-position field)
                     :base-type     (or (name->base-type (:base-type field))
                                        (json-type->base-type (keyword (:type field))))}))}))

;; sample query {:database 3, :query {:source-table 8, :fields [[:field-id 37] [:field-id 39] [:field-id 38]], :limit 2000}, :type :query, ...}
(defmethod driver/mbql->native :couchbase [_ query]
  (log/info
    (u/format-color 'blue
        (format  "mbql->native query %s" query)))
  (couchbase.qp/mbql->native query))

(defmethod driver/substitute-native-parameters :couchbase
  [driver inner-query]
  (log/info (format  "substitute-native-parameters query %s" (:query inner-query)))
  inner-query
  ;; (parameters/substitute-native-parameters driver inner-query)
  )

(defmethod driver/execute-reducible-query :couchbase [_ {native-query :native} _ respond]
  (log/info
    (u/format-color 'blue
        (format  "execute-reducible-query native-query %s" native-query)))
  (let [database (qp.store/database)
        details (:details database)]
    (couchbase.qp/execute-query (cu/getconn details) native-query respond)))
