(ns clorc.core-test
  (:require [clojure.test :refer :all]
            [clorc.core :refer :all]
            [clorc.orc-utils :as u])
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
          (u/orc-scan
            (make-reader (Configuration.)
                         "test-resources/TestOrcFile.test1.orc"))))))


(deftest b-test
  (testing "FIXME, I fail."
    (is (=
          '({"boolean1" 0
             "byte1"    1
             "bytes1"   "bytes"
             "double1"  -15.0
             "float1"   1.0
             "int1"     65536
             "list"     "List"
             "long1"    9223372036854775807
             "map"      "Map"
             "middle"   "Struct"
             "short1"   1024
             "string1"  "bytes"}
             {"boolean1" 1
              "byte1"    100
              "bytes1"   "bytes"
              "double1"  -5.0
              "float1"   2.0
              "int1"     65536
              "list"     "List"
              "long1"    9223372036854775807
              "map"      "Map"
              "middle"   "Struct"
              "short1"   2048
              "string1"  "bytes"})
          (orc->seq
            (make-reader (Configuration.)
                         "test-resources/TestOrcFile.test1.orc"))))))