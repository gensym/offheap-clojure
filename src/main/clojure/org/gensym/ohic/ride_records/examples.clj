(ns org.gensym.ohic.ride-records.examples
 (:require [org.gensym.ohic.ride-records.off-heap-ride-records :as ohr]))

;; On-heap records (seq)
(defn count-unique-bikes [records]
  (loop [i 0
         coll records
         ids #{}]
    (if (empty? coll)
      (count ids)
      (recur (inc i)
             (rest coll)
             (conj ids (:bike-id (first coll)))))))

(defn count-unique-bikes-transduce [records]
  (count (transduce  (map :bike-id) conj #{} records)))

;; Off-heap records - RecordCollection
(defn count-unique-bikes-off-heap [records]
  (let [num-records (count records)
        unsafe (ohr/unsafe records)
        s ohr/object-size]
    (loop [i 0
           offset (ohr/address records)
           ids #{}]
      (if (= i num-records)
        (count ids)
        (recur (inc i)
               (+ offset ohr/object-size)
               (conj ids (ohr/get-bike-id unsafe offset)))))))

(defn count-unique-bikes-off-heap-nth [records]
  (let [num-records (count records)
        coll records]
    (loop [i 0
           ids #{}]
      (if (= i num-records)
        (count ids)
        (let [record (nth coll i)]
          (recur (inc i)
                 (conj ids (ohr/get-bike-id record))))))))

(defn count-unique-bikes-off-heap-transduce [records]
  (count (transduce (map :bike-id) conj #{} records)))


;; On-heap records (seq)
(defn count-minutes-ridden [records station-id]
  (->>
   records
   (filter (fn [record]
             (or (= station-id (:from-station-id record))
                 (= station-id (:to-station-id record)))))
   (map (fn [record] (/ (- (:stop-time record)
                           (:start-time record))
                        60000)))
   (reduce +)))

(defn count-minutes-ridden-loop [records station-id]
  (loop [coll records
         sum 0]
    (if (empty? coll)
      sum
      (recur (rest coll)
             (let [rec (first coll)]
               (if (or (= station-id (:from-station-id rec))
                       (= station-id (:to-station-id rec)))
                 (+ sum (/ (- (:stop-time rec) (:start-time rec)) 60000))
                 sum))))))

(defn count-minutes-ridden-transduce [records station-id]
  (transduce (comp (filter  #(or (= station-id (:from-station-id %))
                                 (= station-id (:to-station-id %))))
                   (map (fn [record] (/ (- (:stop-time record) (:start-time record))
                                        60000))))
             + 0 records))

;; Offheap records (RecordCollection)
(defn count-minutes-ridden-off-heap [records station-id]
  (let [num-records (count records)
        unsafe (ohr/unsafe records)
        s ohr/object-size]
    (loop [i 0
           offset (ohr/address records)
           sum 0]
      (if (= i num-records)
        sum
        (recur (inc i)
               (+ offset s)
               (if (or (= station-id (ohr/get-from-station-id unsafe offset))
                       (= station-id (ohr/get-to-station-id unsafe offset)))
                 (+ sum (/ (- (ohr/get-stop-time unsafe offset)
                              (ohr/get-start-time unsafe offset)) 60000))
                 sum))))))

(defn count-minutes-ridden-off-heap-nth [records station-id]
  (let [num-records (count records)
        coll records]
    (loop [i 0
           sum 0]
      (if (= i num-records)
        sum
        (recur (inc i)
               (let [rec (nth coll i)]
                 (if (or (= station-id (ohr/get-from-station-id rec))
                         (= station-id (ohr/get-to-station-id rec)))
                   (+ sum (/ (- (ohr/get-stop-time rec)
                                (ohr/get-start-time rec))
                             60000))
                   sum)))))))

(defn count-minutes-ridden-off-heap-nth-keyword [records station-id]
  (let [num-records (count records)
        coll records]
    (loop [i 0
           sum 0]
      (if (= i num-records)
        sum
        (recur (inc i)
               (let [rec (nth coll i)]
                 (if (or (= station-id (:from-station-id rec))
                         (= station-id (:to-station-id rec)))
                   (+ sum (/ (- (:stop-time rec) (:start-time rec)) 60000))
                   sum)))))))

(defn count-minutes-ridden-off-heap-transduce [records station-id]
  (transduce (comp (filter (fn [record]
                             (or (= station-id (:from-station-id record))
                                 (= station-id (:to-station-id record)))))
                   (map (fn [record] (/ (- (:stop-time record) (:start-time record))
                                        60000))))
             + 0 records))


