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

;; :aggregation [[:aggregation-options [:sum [:field 54 nil]] {:name "sum"}]]
;; :aggregation [[:aggregation-options [:sum [:field 54 nil]] {:name "sum"}] [:aggregation-options [:count] {:name "count"}]]
(def agg-multiple-query {:database 5, :query {:source-table 11,
                                              :aggregation  [[:aggregation-options [:count] {:name "count"}]],
                                              :breakout  [[:field-id 49] [:field-id 48]],
                                              :order-by [[:asc [:field-id 49]] [:asc [:field-id 48]]]}})

(def agg-multiple-break-options {:database 5, :query {:source-table 11,
                                                      :aggregation [[:aggregation-options [:sum [:field 49 nil]] {:name "sum"}]
                                                                    [:aggregation-options [:count] {:name "count"}]]
                                              :breakout     [[:field-id 49] [:field-id 48]],
                                              :order-by     [[:asc [:field-id 49]] [:asc [:field-id 48]]]}})

(def database {:details {:dbname "test-bucket", :host "localhost", :user "Administrator", :password "password", :definitions "{\"tables\":[{\"name\": \"order\", \"schema\": \"Order\", \"fields\": [  { \"name\": \"id\", \"type\": \"string\",\"database-position\": 0, \"pk?\": true},  { \"name\": \"type\", \"type\": \"string\",\"database-position\": 1 }, { \"name\": \"state\", \"type\": \"string\",\"database-position\": 2 }]}]}"}})

(deftest query-processor
  (testing "mbql->native"
    (with-redefs [qp.store/database (fn [] database)
                  qp.store/table (fn [_] {:name "order"})
                  qp.store/field (fn [id] (case id
                                            47 {:name "id" :special_type :type/PK}
                                            48 {:name "type"}
                                            49 {:name "state"}))]
      (is (= {:query "SELECT Meta().`id`,b.type AS type,b.state AS state FROM `test-bucket` b WHERE _type = \"Order\" LIMIT 2000;"
              :cols   ["id" "type" "state"]
              :mbql? true}

             (cqp/mbql->native basic-query)))))

  (testing "mbql->native agg"
    (with-redefs [qp.store/database (fn [] database)
                  qp.store/table    (fn [_] {:name "order"})
                  qp.store/field    (fn [id] (case id
                                               48 {:name "type"}
                                               49 {:name "state"}))]
      (is (= {:query "SELECT COUNT(*) count, b.state AS state FROM `test-bucket` b WHERE _type = \"Order\" GROUP BY b.state"
              :cols  ["state" "count"]
              :mbql? true}

             (cqp/mbql->native agg-query)))))

  (testing "mbql->native multiple-agg"
    (with-redefs [qp.store/database (fn [] database)
                  qp.store/table    (fn [_] {:name "order"})
                  qp.store/field    (fn [id] (case id
                                               48 {:name "type"}
                                               49 {:name "state"}))]
      (is (= {:query "SELECT COUNT(*) count, b.state AS state, b.type AS type FROM `test-bucket` b WHERE _type = \"Order\" GROUP BY b.state, b.type"
              :cols  ["state" "type" "count"]
              :mbql? true}

             (cqp/mbql->native agg-multiple-query)))
      (is (= {:query "SELECT SUM(state) sum, COUNT(*) count, b.state AS state, b.type AS type FROM `test-bucket` b WHERE _type = \"Order\" GROUP BY b.state, b.type"
              :cols  ["state" "type" "sum" "count"]
              :mbql? true}

             (cqp/mbql->native agg-multiple-break-options)))))

  (testing "aggregation-options-n1ql"
    (with-redefs [qp.store/field    (fn [id] (case id
                                               49 {:name "state"}))]
      (is (= {:name "count" :n1ql "COUNT(*) count"}
           (cqp/aggregation-options-n1ql [:aggregation-options [:count] {:name "count"}])
           ))
      (is (= {:name "sum" :n1ql "SUM(state) sum"}
           (cqp/aggregation-options-n1ql [:aggregation-options [:sum [:field 49 nil]] {:name "sum"}])
           ))))

  (testing "aggregation-ns-n1ql"
    (with-redefs [qp.store/field (fn [id] (case id
                                            49 {:name "state"}))]
      (is (= {:name ["count"] :n1ql "COUNT(*) count, "}
             (cqp/aggregation-n1ql [[:aggregation-options [:count] {:name "count"}]])
             ))
      (is (= {:name ["count" "sum"] :n1ql "COUNT(*) count, SUM(state) sum, "}
             (cqp/aggregation-n1ql [[:aggregation-options [:count] {:name "count"}]
                                    [:aggregation-options [:sum [:field 49 nil]] {:name "sum"}]
                                    ])
             ))
      ))


  (testing "where-clause"
    (is (= "WHERE _type = \"foo\" "
           (cqp/where-clause {:schema "foo"} nil)))
    (is (= "WHERE type = \"foo\" "
           (cqp/where-clause {:schema "type:foo"} nil))))
  (testing "normalize-col"
    (is (= "foo"
           (cqp/normalize-col {:name "foo"})))
    (is (= "foo_bar"
           (cqp/normalize-col {:name "foo.bar"})))
    (is (= "foo0_bar"
           (cqp/normalize-col {:name "foo[0].bar"})))
    (is (= "foo_bar"
           (cqp/normalize-col {:name "`foo`.bar"})))))

