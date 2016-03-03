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
  (:require [org.gensym.ohic.ride-records.divvy-ride-records :as records]
            [org.gensym.ohic.ride-records.off-heap-ride-records :as ohr]
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
  (loop [i 0
         coll (:records records)
         ids #{}]
    (if (empty? coll)
      (count ids)
      (recur (inc i)
             (rest coll)
             (conj ids (:bike-id (first coll)))))))

(defn -countUniqueBikesTransduce [records]
  (count (transduce  (map :bike-id) conj #{} (:records records))))

(defn -countUniqueBikesOffHeap [records]
  (let [num-records (count (:offheap records))
        unsafe (ohr/unsafe (:offheap records))
        s ohr/object-size]
    (loop [i 0
           offset (ohr/address (:offheap records))
           ids #{}]
      (if (= i num-records)
        (count ids)
        (recur (inc i)
               (+ offset ohr/object-size)
               (conj ids (ohr/get-bike-id unsafe offset)))))))

(defn -countUniqueBikesOffHeapNth [records]
  (let [num-records (count (:offheap records))
        coll (:offheap records)]
    (loop [i 0
           ids #{}]
      (if (= i num-records)
        (count ids)
        (let [record (nth coll i)]
          (recur (inc i)
                 (conj ids (ohr/get-bike-id record))))))))

(defn -countUniqueBikesOffHeapTransduce [records]
  (count (transduce (map :bike-id) conj #{} (:offheap records))))

(defn -countMinutesRidden [records station-id]
  (->>
   (:records records)
   (filter #(or (= station-id (:from-station-id %))
                (= station-id (:to-station-id %))))
   (map (fn [record] (/ (- (:stop-time record) (:start-time record))
                        60000)))
   (reduce +)))

(defn -countMinutesRiddenLoop [records station-id]
  (loop [coll (:records records)
         sum 0]
    (if (empty? coll)
      sum
      (recur (rest coll)
             (let [rec (first coll)]
               (if (or (= station-id (:from-station-id rec))
                       (= station-id (:to-station-id rec)))
                 (long (+ sum (/ (- (:stop-time rec) (:start-time rec)) 60000)))
                 (long sum)))))))

(defn -countMinutesRiddenTransduce [records station-id]
  (transduce (comp (filter  #(or (= station-id (:from-station-id %))
                                 (= station-id (:to-station-id %))))
                   (map (fn [record] (/ (- (:stop-time record) (:start-time record))
                                        60000))))
             + 0 (:records records)))

(defn -countMinutesRiddenOffHeap [records station-id]
  (let [num-records (count (:offheap records))
        unsafe (ohr/unsafe (:offheap records))
        s ohr/object-size]
    (loop [i 0
           offset (ohr/address (:offheap records))
           sum 0]
      (if (= i num-records)
        sum
        (recur (inc i)
               (+ offset s)
               (if (or (= station-id (ohr/get-from-station-id unsafe offset))
                       (= station-id (ohr/get-to-station-id unsafe offset)))
                 (long (+ sum (/ (- (ohr/get-stop-time unsafe offset)
                                    (ohr/get-start-time unsafe offset)) 60000)))
                 (long sum)))))))

(defn -countMinutesRiddenOffHeapNth [records station-id]
  (let [num-records (count (:offheap records))
        coll (:offheap records)]
    (loop [i 0
           sum 0]
      (if (= i num-records)
        sum
        (recur (inc i)
               (let [rec (nth coll i)]
                 (if (or (= station-id (:from-station-id rec))
                         (= station-id (:to-station-id rec)))
                   (long (+ sum (/ (- (:stop-time rec) (:start-time rec)) 60000)))
                   (long sum))))))))

(defn -countMinutesRiddenOffHeapNthKeyword [records station-id]
  (let [num-records (count (:offheap records))
        coll (:offheap records)]
    (loop [i 0
           sum 0]
      (if (= i num-records)
        sum
        (recur (inc i)
               (let [rec (nth coll i)]
                 (if (or (= station-id (:from-station-id rec))
                         (= station-id (:to-station-id rec)))
                   (long (+ sum (/ (- (:stop-time rec) (:start-time rec)) 60000)))
                   (long sum))))))))

(defn -countMinutesRiddenOffHeapTransduce [records station-id]
  (transduce (comp (filter  #(or (= station-id (:from-station-id %))
                                 (= station-id (:to-station-id %))))
                   (map (fn [record] (/ (- (:stop-time record) (:start-time record))
                                        60000))))
             + 0 (:offheap records)))

