(ns metabase.driver.couchbase.parameters-test
  (:require [metabase.driver.couchbase.parameters :as sut]
            [clojure.test :refer [deftest testing is] :as t]))

(def query {:query        "SELECT * FROM `posts` WHERE ref = {{article}}",
            :template-tags {"article" {:id "c70ab780-34a7-a3b8-f3b0-dc6fdb0336fa",
                                       :name "article", :display-name "Article", :type :text}},
            :parameters    [{:type :category, :target [:variable [:template-tag "article"]], :value "GZ8872"}]})

(def two-tags [{:type :category, :target [:variable [:template-tag "article"]], :value "GZ8872"}
               {:type :category, :target [:variable [:template-tag "author"]], :value "GX5231"}])

(def number-type (-> query (assoc-in [:template-tags "article" :type] :number)
                     (assoc-in [:parameters 0 :value] "123")))

(deftest substitute-native-parameters
  (testing "substitute text"
    (is (= {:query "SELECT * FROM `posts` WHERE ref = \"GZ8872\""}
           (sut/substitute-native-parameters nil query))))
  (testing "substitute number"
    (is (= {:query "SELECT * FROM `posts` WHERE ref = 123"}
           (sut/substitute-native-parameters nil number-type)))))

(deftest parameters->value

  (testing "parameters->value"
    (is (= "\"GZ8872\""
           (sut/parameters->value (:parameters query) (get-in query [:template-tags "article"])))))

  (testing "parameters->value parameters has multiple tags"
    (is (= "\"GZ8872\""
           (sut/parameters->value two-tags (get-in query [:template-tags "article"])))))

  (testing "parameters->value number"
    (is (= "123"
           (sut/parameters->value (:parameters number-type) (get-in number-type [:template-tags "article"]))))))
