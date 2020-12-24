(ns metabase.driver.couchbase.util
  (:require [earthen.clj-cb.cluster :as c]
            [earthen.clj-cb.bucket :as b]))



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
