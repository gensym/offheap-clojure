(ns org.gensym.ohic.ride-records.record-cache
  (:require [clojure.java.io :as io]
            [org.gensym.ohic.ride-records.off-heap-ride-records :as ohr]
            [org.gensym.ohic.ride-records.divvy-ride-records :as records]))

(defn load-cached-records []
  (if-let [dir-rs (io/resource "data")]
    (let [dir (io/file dir-rs)
          file (io/file (.getPath dir) "ride_record_collection")]
      (if (.exists file)
        (with-open [r (io/input-stream file)]
          (ohr/deserialize r ))
        (let [record-collection (ohr/make-record-collection (records/load-from-files))]
          (with-open [w (io/output-stream file)]
            (ohr/serialize record-collection w)
            record-collection))))
    (throw (java.io.FileNotFoundException "data directory is missing"))))

(defn clean-cache []
  (if-let [resource (io/resource "data/ride_record_collection")]
    (.delete (io/file resource))))


(defn make-cache []
  (load-cached-records))
