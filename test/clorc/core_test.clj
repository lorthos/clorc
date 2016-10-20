(ns clorc.core-test
  (:require [clojure.test :refer :all]
            [clorc.core :refer :all])
  (:import (org.apache.hadoop.conf Configuration)))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0
           (read-all
             (make-reader (Configuration.)
                          "test-resources/TestOrcFile.test1.orc"))))))


