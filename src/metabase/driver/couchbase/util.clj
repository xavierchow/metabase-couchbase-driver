(ns metabase.driver.couchbase.util
  (:require [earthen.clj-cb.cluster :as c]
            [earthen.clj-cb.bucket :as b]
            [cheshire.core :as json]))

(def conn (atom nil))

(defn create-conn
  [database]
  (let [cluster (c/create (:host database))]
    (c/authenticate cluster (:user database) (:password database))
    (c/open-bucket cluster ( :dbname database ))))

(defn getconn
  ([database]
   (if-not @conn (reset! conn (create-conn database))) ;; TODO or use swap! ?
   @conn)
  ([] @conn))

(defn n1ql-query
  "Run the simple n1ql query with statement"
  [conn stmt]
  (b/query conn stmt))


;; databbase configurations

(defn database-definitions
  [database]
  (json/parse-string (:definitions (:details database)) keyword))

;; {  "tables": [ {"name": "order", "schema": "Order", "fields": [ { "name": "id", "type": "string","database-position": 0}, { "name": "sku", "type": "string","database-position": 1 },{ "name": "amount", "type": "number","database-position": 2 }]}, {"name": "record"}]}
(defn database-table-defs
  [database]
  (or (:tables (database-definitions database)) []))

(defn database-table-def
  "given a database and table name, get the table definition"
  [database table-name]
  (first (filter #(= (:name %) table-name) (database-table-defs database))))

