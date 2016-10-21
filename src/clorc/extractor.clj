(ns clorc.extractor
  (:import (org.apache.hadoop.hive.ql.exec.vector ColumnVector BytesColumnVector DecimalColumnVector StructColumnVector LongColumnVector DoubleColumnVector ListColumnVector MapColumnVector)))

(defprotocol VectorItem
  (extract [^ColumnVector v index type-str]))

(defn getv [^ColumnVector cv index]
  (nth (.-vector cv) index))

; protocols can be extended to existing types and user defined types
(extend-protocol VectorItem
  BytesColumnVector
  (extract [cv i type-str]
    (println type-str)
    (if (nil? type-str)
      "bytes"
      (if (= type-str "boolean")
        (= (nth (getv cv i) 0) 1)
        "bytes")))
  DecimalColumnVector
  (extract [cv i type-str]
    (getv cv i))
  StructColumnVector
  (extract [v i type-str]
    "Struct")
  LongColumnVector
  (extract [cv i type-str]
    (getv cv i))
  DoubleColumnVector
  (extract [cv i type-str]
    (getv cv i))
  ListColumnVector
  (extract [cv i type-str]
    "List")
  MapColumnVector
  (extract [cv i type-str]
    ;(zipmap
    ;  (extract (.-keys cv) 0)
    ;  (extract (.-values cv) 0))
    ;
    "Map")
  )
;
;
;(defmacro extend-protocol-with-arrays []
;  (let [ba (symbol "[B")]
;    `(extend-protocol VectorItem
;       ~ba
;       (extract [this#] "<bytes>")
;       )))
;
;(extend-protocol-with-arrays)

;(defprotocol ValueExtractor
;  (extract [obj]))
;
;(extend-protocol ValueExtractor
;  nil
;  (extract [o] nil)
;  String
;  (extract [o] o)
;  Long
;  (extract [o] o)
;  (class (byte-array 1 1))
;  (extract [o] "<bytes>"))
