(ns org.gensym.ohic.benchmark.harness
  (:gen-class
   :name org.gensym.ohic.benchmark.harness
   :methods [#^{:static true} [countUniqueBikes [Object] int]
             #^{:static true} [countUniqueBikesTransduce [Object] int]
            #^{:static true} [countUniqueBikesOffHeap [Object] int]
             #^{:static true} [countUniqueBikesOffHeapNth [Object] int]
             #^{:static true} [countUniqueBikesOffHeapTransduce [Object] int]
             #^{:static true} [countMinutesRidden [Object int] int]
             #^{:static true} [countMinutesRiddenLoop [Object int] int]
             #^{:static true} [countMinutesRiddenTransduce [Object int] int]
             #^{:static true} [countMinutesRiddenOffHeap [Object int] int]
             #^{:static true} [countMinutesRiddenOffHeapNth [Object int] int]
             #^{:static true} [countMinutesRiddenOffHeapNthKeyword [Object int] int]
             #^{:static true} [countMinutesRiddenOffHeapTransduce [Object int] int]
             #^{:static true} [loadRecords [] Object]
             #^{:static true} [unloadRecords [Object] Object]
             ])
  (:import [org.joda.time Interval DateTime])
  (:require
   [org.gensym.ohic.ride-records.examples :as ex]
   [org.gensym.ohic.ride-records.off-heap-ride-records :as ohr]
   [org.gensym.ohic.ride-records.divvy-ride-records :as records]
   [org.gensym.ohic.ride-records.record-cache :as cache]))

(set! *warn-on-reflection* true)

(defn load-records-from [recs]
  (let [record-coll (ohr/make-record-collection recs)]
    {:records recs
     :offheap record-coll}))

(defn -loadRecords []
  {:records (records/load-from-files)
   :offheap (cache/load-cached-records)})

(defn -unloadRecords [records]
  (ohr/dispose (:offheap records)))

(defn -countUniqueBikes [records]
  (ex/count-unique-bikes (:records records)))

(defn -countUniqueBikesTransduce [records]
  (ex/count-unique-bikes-transduce (:records records)))

(defn -countUniqueBikesOffHeap [records]
  (ex/count-unique-bikes-off-heap (:offheap records)))

(defn -countUniqueBikesOffHeapNth [records]
  (ex/count-unique-bikes-off-heap-nth (:offheap records)))

(defn -countUniqueBikesOffHeapTransduce [records]
  (ex/count-unique-bikes-off-heap-transduce (:offheap records)))

(defn -countMinutesRidden [records station-id]
  (ex/count-minutes-ridden (:records records) station-id))

(defn -countMinutesRiddenLoop [records station-id]
  (ex/count-minutes-ridden-loop (:records records) station-id))

(defn -countMinutesRiddenTransduce [records station-id]
  (ex/count-minutes-ridden-transduce (:records records) station-id))

(defn -countMinutesRiddenOffHeap [records station-id]
  (ex/count-minutes-ridden-off-heap (:offheap records) station-id))

(defn -countMinutesRiddenOffHeapNth [records station-id]
  (ex/count-minutes-ridden-off-heap-nth (:offheap records) station-id))

(defn -countMinutesRiddenOffHeapNthKeyword [records station-id]
  (ex/count-minutes-ridden-off-heap-nth-keyword (:offheap records) station-id))

(defn -countMinutesRiddenOffHeapTransduce [records station-id]
  (ex/count-minutes-ridden-off-heap-transduce (:offheap records) station-id))

