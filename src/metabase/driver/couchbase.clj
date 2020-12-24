(ns metabase.driver.couchbase
  "Couchbase  driver, since it's nosql database which doesn't 1-to-1 map to the db/table like relational db,
   here is the mapping releationship: `database` -> `couchbase bucket`, `table` -> `document with certian _type`"
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [clojure.string :as cs]
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

(defn- database-definitions
  [database]
  (json/parse-string (:definitions (:details database)) keyword))
;; {  "tables": [ {"name": "order", "schema": "Order", "fields": [ { "name": "id", "type": "string","database-position": 0}, { "name": "sku", "type": "string","database-position": 1 },{ "name": "amount", "type": "number","database-position": 2 }]}, {"name": "record"}]}
(defn- database-table-defs
  [database]
  (or (:tables (database-definitions database)) []))

(defn- database-table-def
  [database table-name]
  (first (filter #(= (:name %) table-name) (database-table-defs database))))

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

(defn where-clause
  [table]
  (let [type (:schema table)]
    (str "WHERE _type = \"" type "\" ")))

(defn limit
  [{limit :limit}]
  (str " LIMIT " limit ";"))

(defn select-fields
  "fields format: [[:field-id 40] [:field-id 41] [:field-id 42]]"
  [fields]
  ;; (qp.store/field 10) ;; get Field 10
  (map (comp :name #(qp.store/field (second %))) fields)
  )


(defn select-clause
  [fields bucket]
  (let [alias "b"
        columns (vec (select-fields fields))
        subject (cs/join "," (map #(str alias "." %) columns))]
    (log/info (format  "select clause subject %s" subject))
    (str "SELECT "  subject  " FROM " "`" bucket "` " alias " ")
    )
  ;; "SELECT b.sku, b.amount FROM `blueOrder` b "
  )


(driver/register! :couchbase)

(defmethod driver/supports? [:couhbase :basic-aggregations] [_ _] false)
(defmethod driver/supports? [:couchbase :native-parameters] [_ _]  true)

(defmethod driver/can-connect? :couchbase [_ details]
  (log/info (format  "Database can-connected? %s" details))
  ;; TODO SELECT 1?
  true)

(defmethod driver/describe-database :couchbase [_ database]
  (log/info (format  "Describe-database  %s" (:details database)))
  (cu/getconn (:details database))
  (let [table-defs (database-table-defs database)]
    {:tables (set (for [table-def table-defs]
                    {:name   (:name table-def)
                     :schema (:schema table-def)}))}))

(defmethod driver/describe-table :couchbase [_ database table]
  (log/info (format  "Describe-table %s" (:name table)))
  (let [table-def  (database-table-def database (:name table))]
    {:name   (:name table-def)
     :schema (:schema table-def)
     :fields (set (for [field (:fields table-def)]
                    {:name          (:name field)
                     :database-type (:type field)
                     :database-position (:database-position field)
                     :base-type     (or (:base_type field)
                                        (json-type->base-type (keyword (:type field))))}))}))

;; sample query
;; {:database 3, :query {:source-table 8, :fields [[:field-id 37] [:field-id 39] [:field-id 38]], :limit 2000}, :type :query, :middleware {:js-int-to-string? true, :add-default-userland-constraints? true}, :info {:executed-by 1, :context :ad-hoc, :nested? false, :query-hash #object["[B" 0x30bea3d "[B@30bea3d"]}, :constraints {:max-results 10000, :max-results-bare-rows 2000}}
;;
(defmethod driver/mbql->native :couchbase [_ query]
  (log/info (format  "mbql->native query %s" query))
  (let [database    (qp.store/database)
        table       (qp.store/table (:source-table (:query query)))
        fields   (:fields (:query query))
        select   (select-clause fields (:dbname (:details database)))]

    (log/info (format  "mbql->native database tables from qp.store %s" (:details database)))

    {:query (str select (where-clause (database-table-def database (:name table))) (limit (:query query))) 
     :cols (vec (select-fields fields))
     :mbql? true}))

(defmethod driver/substitute-native-parameters :couchbase
  [driver inner-query]
  (log/info (format  "substitute-native-parameters query %s" (:query inner-query)))
  inner-query
  ;; (parameters/substitute-native-parameters driver inner-query)
  )

(defmethod driver/execute-reducible-query :couchbase [_ {native-query :native} _ respond]
  (log/info (format  "execute-reducible-query native-query %s" native-query))
  (let [database (qp.store/database)
        details (:details database)]
    (couchbase.qp/execute-query (cu/getconn details) native-query respond)
    )
  )
