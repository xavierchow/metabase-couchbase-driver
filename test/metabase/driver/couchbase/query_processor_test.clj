(ns metabase.driver.couchbase.query-processor-test
  (:require [metabase.driver.couchbase.query-processor :as cqp]
            [metabase.query-processor.store :as qp.store]
            [clojure.test :refer [deftest testing is]]))

(def basic-query {:database 5, :query {:source-table 11,
                                       :fields [[:field-id 47] [:field-id 48] [:field-id 49]],
                                       :limit 2000},
                  :type :query})

(def agg-query {:database 5, :query {:source-table 11,
                                     :aggregation [[:aggregation-options [:count] {:name "count"}]],
                                     :breakout [[:field-id 49]],
                                     :order-by [[:asc [:field-id 49]]]}})

(def agg-multiple-query {:database 5, :query {:source-table 11,
                                              :aggregation  [[:aggregation-options [:count] {:name "count"}]],
                                              :breakout  [[:field-id 49] [:field-id 48]],
                                              :order-by [[:asc [:field-id 49]] [:asc [:field-id 48]]]}})

(def database {:details {:dbname "test-bucket", :host "localhost", :user "Administrator", :password "password", :definitions "{\"tables\":[{\"name\": \"order\", \"schema\": \"Order\", \"fields\": [  { \"name\": \"id\", \"type\": \"string\",\"database-position\": 0, \"pk?\": true},  { \"name\": \"type\", \"type\": \"string\",\"database-position\": 1 }, { \"name\": \"state\", \"type\": \"string\",\"database-position\": 2 }]}]}"}})

(deftest query-processor
  (testing "mbql->native"
    (with-redefs [qp.store/database (fn [] database)
                  qp.store/table (fn [_] {:name "order"})
                  qp.store/field (fn [id] (case id
                                            47 {:name "id" :special_type :type/PK}
                                            48 {:name "type"}
                                            49 {:name "state"}))]
      (is (= {:query "SELECT Meta().`id`,b.type,b.state FROM `test-bucket` b WHERE _type = \"Order\" LIMIT 2000;"
              :cols   ["id" "type" "state"]
              :mbql? true}

             (cqp/mbql->native basic-query)))))

  (testing "mbql->native agg"
    (with-redefs [qp.store/database (fn [] database)
                  qp.store/table    (fn [_] {:name "order"})
                  qp.store/field    (fn [id] (case id
                                               48 {:name "type"}
                                               49 {:name "state"}))]
      (is (= {:query "SELECT COUNT(*) count, b.state FROM `test-bucket` b WHERE _type = \"Order\" GROUP BY b.state"
              :cols  ["state" "count"]
              :mbql? true}

             (cqp/mbql->native agg-query)))))

  (testing "mbql->native multiple-agg"
    (with-redefs [qp.store/database (fn [] database)
                  qp.store/table    (fn [_] {:name "order"})
                  qp.store/field    (fn [id] (case id
                                               48 {:name "type"}
                                               49 {:name "state"}))]
      (is (= {:query "SELECT COUNT(*) count, b.state, b.type FROM `test-bucket` b WHERE _type = \"Order\" GROUP BY b.state, b.type"
              :cols  ["state" "type" "count"]
              :mbql? true}

             (cqp/mbql->native agg-multiple-query))))))
