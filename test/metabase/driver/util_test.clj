(ns metabase.driver.util-test
  (:require  [clojure.test :refer [deftest testing is]]
             [metabase.driver.couchbase.util :as cu]))

(def database {:details {:dbname "test-bucket", :host "localhost", :user "Administrator", :password "password", :definitions "{\"tables\":[{\"name\": \"order\", \"schema\": \"Order\", \"fields\": [  { \"name\": \"id\", \"type\": \"string\",\"database-position\": 0, \"pk?\": true},  { \"name\": \"type\", \"type\": \"string\",\"database-position\": 1 }, { \"name\": \"state\", \"type\": \"string\",\"database-position\": 2 }]}]}"}})

(deftest database-def
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


(deftest n1ql-stmt-whitelist
  (testing "allow SELECT"
    (is (= true
           (cu/stmt-allowed? "SELECT * FROM `dummy` "))))
  (testing "allow lowercase select"
    (is (= true
           (cu/stmt-allowed? "select * FROM `dummy` "))))
  (testing "should allow line-break"
    (is (= true
           (cu/stmt-allowed? "SELECT * FROM `dummy` \n LIMIT 500"))))
  (testing "don't allow UPDATE"
    (is (= false
           (cu/stmt-allowed? "DELETE FROM `dummy`WHERE city =  (SELECT MAX(city) FROM `dummy`)"))))
  (testing "don't allow sub SELECT"
    (is (= false
           (cu/stmt-allowed? "UPDATE `dummy` set foo = 5")))))

