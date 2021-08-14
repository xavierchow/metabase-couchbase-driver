(ns metabase.driver.couchbase.parameters-test
  (:require [metabase.driver.couchbase.parameters :as sut]
            [clojure.test :refer [deftest testing is] :as t]))

(def query {:query        "SELECT * FROM `posts` WHERE ref = {{article}}",
            :template-tags {"article" {:id "c70ab780-34a7-a3b8-f3b0-dc6fdb0336fa",
                                       :name "article", :display-name "Article", :type :text}},
            :parameters    [{:type :category, :target [:variable [:template-tag "article"]], :value "GZ8872"}]})

(def two-tags [{:type :category, :target [:variable [:template-tag "article"]], :value "GZ8872"}
               {:type :category, :target [:variable [:template-tag "author"]], :value "G5231"}])

(def number-type (-> query (assoc-in [:template-tags "article" :type] :number)
                     (assoc-in [:parameters 0 :value] "123")))

(def dimension-type (-> query (assoc-in [:template-tags "article" :type] :dimension)
                        (assoc-in [:parameters 0 :target]
                                  [:dimension [:template-tag "article"]])
                        (assoc-in [:parameters 0 :value] ["FOO"]) ))
(def dimension-type-two-values (-> query (assoc-in [:template-tags "article" :type] :dimension)
                                   (assoc-in [:parameters 0 :target] [:dimension [:template-tag "article"]])
                        (assoc-in [:parameters 0 :value] ["FOO" "BAR"])))



(deftest substitute-native-parameters
  (testing "substitute text"
    (is (= {:query "SELECT * FROM `posts` WHERE ref = \"GZ8872\""}
           (sut/substitute-native-parameters nil query))))
  (testing "substitute number"
    (is (= {:query "SELECT * FROM `posts` WHERE ref = 123"}
           (sut/substitute-native-parameters nil number-type))))
  (testing "substitute dimension"
    (is (= {:query "SELECT * FROM `posts` WHERE ref = [\"FOO\"]"}
           (sut/substitute-native-parameters nil dimension-type))))
  (testing "substitute dimension two values"
    (is (= {:query "SELECT * FROM `posts` WHERE ref = [\"FOO\" \"BAR\"]"}
           (sut/substitute-native-parameters nil dimension-type-two-values)))))

(deftest parameters->value

  (testing "parameters->value"
    (is (= "\"GZ8872\""
           (sut/parameters->value (:parameters query) (get-in query [:template-tags "article"])))))

  (testing "parameters->value parameters has multiple tags"
    (is (= "\"GZ8872\""
           (sut/parameters->value two-tags (get-in query [:template-tags "article"])))))

  (testing "parameters->value number"
    (is (= "123"
           (sut/parameters->value (:parameters number-type) (get-in number-type [:template-tags "article"])))))
  (testing "parameters->value dimension"
    (is (= "[\"FOO\"]"
           (sut/parameters->value (:parameters dimension-type) (get-in dimension-type [:template-tags "article"])))))
  (testing "parameters->value dimension with multiple values"
    (is (= "[\"FOO\",\"BAR\"]"
           (sut/parameters->value (:parameters dimension-type-two-values) (get-in dimension-type-two-values [:template-tags "article"]))))))
