(ns clorc.extractor
  (:import (org.apache.hadoop.hive.ql.exec.vector ColumnVector BytesColumnVector DecimalColumnVector StructColumnVector LongColumnVector DoubleColumnVector ListColumnVector MapColumnVector)))

(defprotocol VectorItem
  (extract [^ColumnVector v index]))


; protocols can be extended to existing types and user defined types
(extend-protocol VectorItem
  BytesColumnVector
  (extract [v i] "bytes")
  DecimalColumnVector
  (extract [cv i] (nth (.-vector cv) i))
  StructColumnVector
  (extract [v i] "Struct")
  LongColumnVector
  (extract [cv i] (nth (.-vector cv) i))
  DoubleColumnVector
  (extract [cv i] (nth (.-vector cv) i))
  ListColumnVector
  (extract [v i] "List")
  MapColumnVector
  (extract [cv i]
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
