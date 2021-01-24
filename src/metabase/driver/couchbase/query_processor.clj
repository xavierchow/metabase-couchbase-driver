(ns metabase.driver.couchbase.query-processor
  (:refer-clojure :exclude [==])
  (:require [clojure.string :as cs]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [metabase.util :as u]
            [metabase.driver.couchbase.util :as cu]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor.store :as qp.store]))

(defn extract-columns
  [rows]
  (into [] (map #(hash-map :name (name %)) (keys (first rows)))))

(defn- key-names
  [rows]
  (into [] (map name (keys (first rows)))))

(defn- extract-values
  [rows cols]
  (into [] (map (fn [r] (into [] (map #(get r (keyword %)) cols))) rows)))

(defn- wrap-str
  [v]
  (str "\"" v "\""))

(defn- wrap-parenthesis
  [v]
  (str "(" v ")"))

(defmulti ^:private rhs-value
  "[:value 219900 {:base_type :type/Float, :special_type nil, :database_type \"number\", :name \"amount\"}]"
  (fn [[_ _ meta]] (:base_type meta)))

(defmethod rhs-value :type/Float
  [v]
  (second v))

(defmethod rhs-value :type/Text
  [v]
  (-> v second wrap-str))

(defmulti ^:private select-field first)
(defmethod select-field :datetime-field
  [[_ field-clause unit]]
  (if (= unit :default)
    (select-field field-clause)))

(defmethod select-field :field-id
  [[_ field-id]]
  (qp.store/field field-id))

(defmulti ^:private parse-filter first)

(defmethod parse-filter :=  [[_ field value]]
  (let [lhs (-> field select-field :name)]
    (if (nil? (second value))
      (str lhs " IS MISSING")
      (str lhs " = " (rhs-value value)))))
(defmethod parse-filter :!=  [[_ field value]]
  (let [lhs (-> field select-field :name)]
    (if (nil? (second value))
      (str lhs " IS NOT MISSING")
      (str lhs " != " (rhs-value value)))))

(defmethod parse-filter :contains [[_ field value]] (str "CONTAINS(" (:name (select-field field)) ", " (-> value second wrap-str) ")"))
(defmethod parse-filter :starts-with [[_ field value]] (str (:name (select-field field)) " LIKE " (wrap-str (str (-> value second) "%"))))
(defmethod parse-filter :ends-with [[_ field value]] (str "ANY i IN SUFFIXES(" (-> field select-field :name) ") SATISFIES i =  " (-> value second wrap-str) " END"))

(defmethod parse-filter :>  [[_ field value]] (str (:name (select-field field)) " > " (rhs-value value)))
(defmethod parse-filter :>=  [[_ field value]] (str (:name (select-field field)) " >= " (rhs-value value)))
(defmethod parse-filter :<  [[_ field value]] (str (:name (select-field field)) " < " (rhs-value value)))
(defmethod parse-filter :<=  [[_ field value]] (str (:name (select-field field)) " <= " (rhs-value value)))
(defmethod parse-filter :between  [[_ field value1 value2]] (str (parse-filter [:> field value1]) " AND " (parse-filter [:< field value2])))

(defmethod parse-filter :and [[_ & args]] (cs/join " AND " (mapv parse-filter args)))
(defmethod parse-filter :or [[_ & args]] (-> (cs/join " OR " (mapv parse-filter args)) wrap-parenthesis))
(defmethod parse-filter :not [[_ & args]] (str "NOT " (apply parse-filter args)))


;;  {:type :query, :query {:source-table 11, :filter [:between [:datetime-field [:field-id 47] :day] [:relative-datetime -30 :day] [:relative-datetime -1 :day]], :fields [[:field-id 46] [:field-id 49] [:field-id 48] [:field-id 51] [:datetime-field [:field-id 47] :default] [:field-id 50] [:field-id 52] [:field-id 54] [:field-id 53]], :limit 2000},


(defn where-clause
  [table inner-query]
  (let [type (:schema table)]
    (log/info
     (u/format-color 'red
                     (format  "where-clause filter %s" (:filter inner-query))))

    (if (:filter inner-query) ;; TODO if-let
      (str "WHERE _type = \"" type "\" " "AND " (parse-filter (:filter inner-query)) " ")
      (str "WHERE _type = \"" type "\" "))))

(defn limit
  [{limit :limit}]
  (str "LIMIT " (min 10000 limit) ";"))

(defn normalize-col
  [field]
  (let [field-name (:name field)
        ; replace `.` with underscore and remove `[]`
        sanitized (cs/replace field-name #"\.|\[|\]"  {"." "_" "[" "" "]" ""})]
    (if (cs/includes? sanitized "`")
      (cs/replace sanitized #"`" "")
      sanitized)))

;;   fields sample: [[:field-id 40] [:field-id 41] [:field-id 42] [:datetime-field [:field-id 43] :default]]
(defn select-fields
  [fields]
  (map select-field fields))

(defmulti ^:private build-n1ql (fn [q & _] (mbql.u/match-one q
                                                             {:aggregation _}
                                                             :agg

                                                             {:fields _}
                                                             :raw

                                                             {:breakout _}
                                                             :raw)))

(defmethod build-n1ql :raw
  [q bucket table-def]
  (let [alias   "b"
        flds    (select-fields (:fields q))
        columns (map #(if (= (:special_type %) :type/PK) "Meta().`id`" (str alias "."  (:name %) " AS " (normalize-col %))) flds)
        subject (cs/join "," columns)]

    (log/info (format "flds %s" (vec (select-fields (:fields q)))))
    {:query (str "SELECT "  subject  " FROM " "`" bucket "` " alias " " (where-clause table-def q) (limit q))
     :cols  (vec (map normalize-col flds))
     :mbql? true}))

(defmethod build-n1ql :agg
  [q bucket table-def]
  (let [alias   "b"
        name (mbql.u/match-one q [:aggregation-options _ n] (:name n))
        breakout (:breakout q)
        by (cs/join ", " (map #(str alias "." (:name %) " AS " (normalize-col %)) (select-fields breakout)))
        groupby (str "GROUP BY " (cs/join ", " (map #(str alias "." (:name %)) (select-fields breakout))))]
    {:query (str "SELECT COUNT(*) " name ", " by " FROM " "`" bucket "` " alias " " (where-clause table-def q) groupby)
     :cols (conj (vec (map normalize-col (select-fields breakout))) name)
     :mbql? true}))

(defn mbql->native
  [query]
  (let [database (qp.store/database)
        db-name (-> database :details :dbname)
        table    (qp.store/table (mbql.u/query->source-table-id query))
        table-def (cu/database-table-def database (:name table))
        inner-query (:query query)]
    (build-n1ql inner-query db-name table-def)))

(defn execute-query [conn native-query respond]
  (log/info "native-query" native-query)
  (let [stmt    (:query native-query)
        result  (cu/n1ql-query conn stmt)
        rows    (:rows result)
        columns (or (:cols native-query) (key-names rows))
        errors (:errors result)]
    (log/info (format "first rows %s" (first rows)))
    (log/info (format "columns %s" columns))
    (log/info (format "execute-query result.errors %s" errors))

    (if (empty? errors)
       ;; (log/info (format  "query-result %s" (:rows result)))
      (respond {:cols (into [] (map #(hash-map :name %) columns))}
               (extract-values rows columns))
      (let [err (-> errors first .toString)]
        (throw (ex-info (str "Error running query -" err)
                        (json/parse-string  err true)))))))

(defn ping [conn]
  (= (first (:rows (cu/n1ql-query conn "SELECT 1"))) {:$1 1}))
