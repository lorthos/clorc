(ns clorc.core
  (:require [clorc.extractor :as e])
  (:import (org.apache.orc OrcFile RecordReader Reader)
           (org.apache.hadoop.fs Path FileSystem)
           (org.apache.hadoop.hive.ql.exec.vector VectorizedRowBatch BytesColumnVector ColumnVector)))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(defn ^ColumnVector find-col
  [index cols]
  (nth cols index))

(defn make-reader [conf path]
  (OrcFile/createReader
    (Path. path)
    (-> (OrcFile/readerOptions conf)
        (.filesystem (FileSystem/getLocal conf)))))


(defn orc-seq [reader & cols]

  )

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
    (println cols)
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

