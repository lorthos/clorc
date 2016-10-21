(ns clorc.orc-utils
  (:require [clorc.extractor :as e])
  (:import (org.apache.hadoop.hive.ql.exec.vector ColumnVector VectorizedRowBatch)
           (org.apache.orc RecordReader)))

(defn orc-scan
  "should be lazy and not in memory"
  [reader]
  (let [rows ^RecordReader (.rows reader)
        batch
        ^VectorizedRowBatch (-> reader
                                .getSchema
                                .createRowBatch)
        cols (.-cols batch)
        items (atom [])]
    (while (.nextBatch rows batch)
      (doall
        (map
          (fn [index]
            (swap! items conj (into []
                                    (map
                                      (fn [^ColumnVector col]
                                        (e/extract col index))
                                      cols)))
            )
          (range 0 (.size batch)))))
    (.close rows)
    @items))
