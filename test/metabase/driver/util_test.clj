(ns metabase.driver.util-test
  (:require  [clojure.test :refer [deftest testing is]]
             [metabase.driver.couchbase.util :as cu]))

(def database {:details {:dbname "test-bucket", :host "localhost", :user "Administrator", :password "password", :definitions "{\"tables\":[{\"name\": \"order\", \"schema\": \"Order\", \"fields\": [  { \"name\": \"id\", \"type\": \"string\",\"database-position\": 0, \"pk?\": true},  { \"name\": \"type\", \"type\": \"string\",\"database-position\": 1 }, { \"name\": \"state\", \"type\": \"string\",\"database-position\": 2 }]}]}"}})

(deftest datbase-def
  (testing "database-definitions"
    (is (= {:tables [{:name "order" :schema "Order"
                      :fields [{:name "id" :type "string" :database-position 0 :pk? true},
                               {:name "type" :type "string" :database-position 1},
                               {:name "state" :type "string" :database-position 2}]}]}
           (cu/database-definitions database))))

  (testing "database-table-def"
    (is (= {:name   "order" :schema "Order"
            :fields [{:name "id" :type "string" :database-position 0 :pk? true},
                     {:name "type" :type "string" :database-position 1},
                     {:name "state" :type "string" :database-position 2}]}
           (cu/database-table-def database "order")))))
