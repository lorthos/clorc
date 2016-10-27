(ns clorc.core
  (:require [clorc.extractor :as e])
  (:import (org.apache.orc OrcFile RecordReader)
           (org.apache.hadoop.fs Path FileSystem)
           (org.apache.hadoop.hive.ql.exec.vector VectorizedRowBatch ColumnVector)
           (java.util Iterator)))


(defn ^ColumnVector find-col
  [index cols]
  (nth cols index))

(defn make-reader [conf path]
  (OrcFile/createReader
    (Path. path)
    (-> (OrcFile/readerOptions conf)
        (.filesystem (FileSystem/getLocal conf)))))

(defn get-field-names
  [^RecordReader reader]
  (map keyword
       (-> reader
           .getSchema
           .getFieldNames)))

(defn get-field-types
  [^RecordReader reader]
  (map #(-> %
            .getCategory
            .getName)
       (-> reader
           .getSchema
           .getChildren)))

(defn orc->seq [reader]
  (let [rows ^RecordReader (.rows reader)

        current-batch (atom ^VectorizedRowBatch
                            (-> reader
                                .getSchema
                                .createRowBatch))

        current-col-index (atom 0)
        current-row (atom nil)
        hasNext? (atom (<= 0 (.size @current-batch)))
        names (get-field-names reader)
        types (get-field-types reader)
        ]
    (.nextBatch rows @current-batch)
    (filter
      (complement nil?)
      (iterator-seq
        (reify Iterator
          (hasNext [this] @hasNext?)
          (next [this]
            (do
              (if (and
                    (< 0 (.size @current-batch))
                    (< @current-col-index (.size @current-batch)))
                (do
                  (reset!
                    current-row
                    (zipmap
                      names
                      (into []
                            (map
                              (fn [^ColumnVector col]
                                (e/extract col @current-col-index
                                           (nth types
                                                @current-col-index)))
                              (.-cols @current-batch))))
                    )
                  (swap! current-col-index inc))
                (do
                  (.nextBatch rows @current-batch)
                  (if (< 0 (.size @current-batch))
                    (reset! current-col-index 0)
                    (reset! hasNext? false))))
              (if (< 0 (.size @current-batch))
                @current-row
                nil))))))))


