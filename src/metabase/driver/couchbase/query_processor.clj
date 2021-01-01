(ns metabase.driver.couchbase.query-processor
  (:refer-clojure :exclude [==])
  (:require [clojure.string :as cs]
            [clojure.tools.logging :as log]
            [metabase.util :as u]
            [metabase.driver.couchbase.util :as cu]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.query-processor.store :as qp.store])
  )

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

;; (extract-values [{:amount 4500, :sku "2825"}] ["amount" "sku"])

(defmulti ^:private select-field first)
(defmethod select-field :datetime-field
  [[_ field-clause unit]]
  (if (= unit :default)
    (select-field field-clause)))

(defmethod select-field :field-id
  [[_ field-id]]
  (qp.store/field field-id))

(defmulti ^:private parse-filter first)

(defmethod parse-filter :=  [[_ field value]] (str (:name (select-field field)) " = \"" (second value) "\""))
(defmethod parse-filter :!=  [[_ field value]] (str (:name (select-field field)) " != \"" (second value) "\""))

(defmethod parse-filter :and [[_ & args]] (cs/join " AND " (mapv parse-filter args)))


;;  {:type :query, :query {:source-table 11, :filter [:between [:datetime-field [:field-id 47] :day] [:relative-datetime -30 :day] [:relative-datetime -1 :day]], :fields [[:field-id 46] [:field-id 49] [:field-id 48] [:field-id 51] [:datetime-field [:field-id 47] :default] [:field-id 50] [:field-id 52] [:field-id 54] [:field-id 53]], :limit 2000},
(defn where-clause
  [table inner-query]
  (let [type (:schema table)]
    (log/info
      (u/format-color 'red
                      (format  "where-clause filter %s" (:filter inner-query))))

    (if (:filter inner-query)
      (str "WHERE _type = \"" type "\" " "AND " (parse-filter (:filter inner-query)))
      (str "WHERE _type = \"" type "\" " ))))

(defn limit
  [{limit :limit}]
  (str " LIMIT " (min 10000 limit)";")) 


(defn normalize-col
  [field]
  (let [field-name (:name field)]
    (if (cs/includes? field-name ".")
      (last (cs/split field-name #"\."))
      field-name)))

;;   fields sample: [[:field-id 40] [:field-id 41] [:field-id 42] [:datetime-field [:field-id 43] :default]]
(defn select-fields
  [fields]
  (map select-field fields))

(defn select-clause
  [fields bucket]
  (let [alias   "b"
        flds    (select-fields fields)
        columns (map #(if (= (:special_type %) :type/PK) "Meta().`id`" (str alias "." (:name %))) flds)
        subject (cs/join "," columns)]
    (str "SELECT "  subject  " FROM " "`" bucket "` " alias " ")))


(defn mbql->native
  "sample: mbql->native query {:database 5, :query {:source-table 11, :fields [[:field-id 46] [:field-id 49] [:field-id 48] [:field-id 51] [:datetime-field [:field-id 47] :default] [:field-id 50] [:field-id 52] [:field-id 54] [:field-id 53]], :limit 2000}, :type :query}"
  [query]
  (let [database (qp.store/database)
        table    (qp.store/table (mbql.u/query->source-table-id query))
        table-def (cu/database-table-def database (:name table))
        inner-query (:query query)
        fields   (:fields inner-query)
        select   (select-clause fields (:dbname (:details database)))]
    {:query (str select (where-clause table-def inner-query) (limit (:query query)))
     :cols  (vec (map normalize-col (select-fields fields)))
     :mbql? true})
  )

(defn execute-query [conn native-query respond]
  (log/info "native-query" native-query)
  (let [stmt    (:query native-query)
        result  (cu/n1ql-query conn stmt)
        rows    (:rows result)
        columns (or (:cols native-query) (key-names rows))]
    (log/info (format "columns %s" columns))
    ;; (log/info (format  "query-result %s" (:rows result)))
    (respond {:cols (into [] (map #(hash-map :name %) columns))}
             (extract-values rows columns))))

(defn ping [conn]
  (= (first (:rows (cu/n1ql-query conn "SELECT 1"))) {:$1 1}))
