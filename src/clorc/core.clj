(ns clorc.core
  (:import (org.apache.orc OrcFile RecordReader Reader)
           (org.apache.hadoop.fs Path FileSystem)
           (org.apache.hadoop.hive.ql.exec.vector VectorizedRowBatch BytesColumnVector ColumnVector)))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(defn make-reader [conf path]
  (let [reader
        (OrcFile/createReader
          (Path. path)
          (-> (OrcFile/readerOptions conf)
              (.filesystem (FileSystem/getLocal conf))))
        batch
        ^VectorizedRowBatch (-> reader
                                .getSchema
                                .createRowBatch)]
    (println (.numCols batch))
    (println (.nextBatch (.rows reader) batch))
    (println (.size batch))
    reader))


(defn read-all [reader]
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
                                        (try
                                          (-> col
                                              (.-vector)
                                              (nth index)
                                              (.toString))
                                          (catch Exception e
                                            "NOT_SUPPORTED")))
                                      cols)))
            )
          (range 0 (.size batch)))))
    (.close rows)
    @items)
  )

