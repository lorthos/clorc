(ns clorc.core-test
  (:require [clojure.test :refer :all]
            [clorc.core :refer :all])
  (:import (org.apache.hadoop.conf Configuration)))

(deftest a-test
  (testing "FIXME, I fail."
    (is (=
          [[0
            1
            1024
            65536
            9223372036854775807
            1.0
            -15.0
            "bytes"
            "bytes"
            "Struct"
            "List"
            "Map"]
           [1
            100
            2048
            65536
            9223372036854775807
            2.0
            -5.0
            "bytes"
            "bytes"
            "Struct"
            "List"
            "Map"]]
          (orc-scan
            (make-reader (Configuration.)
                         "test-resources/TestOrcFile.test1.orc"))))))


