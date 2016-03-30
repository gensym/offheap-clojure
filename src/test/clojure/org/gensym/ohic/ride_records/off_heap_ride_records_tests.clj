(ns org.gensym.ohic.ride-records.off-heap-ride-records-tests
  (:require [clojure.test :refer :all]
            [org.gensym.ohic.ride-records.off-heap-ride-records :as recs]))


(defn test-record [trip-id
                   bike-id
                   from-station
                   to-station
                   start-time
                   stop-time
                   user-type]
  {:trip-id trip-id
   :bike-id bike-id
   :from-station-id from-station
   :to-station-id to-station
   :start-time (.getTime start-time)
   :stop-time  (.getTime stop-time)
   :user-type user-type})

(def on-heap-records
  [
   (test-record 1 12 42 23  #inst "2014-03-25T18:00:48" #inst "2014-03-25T18:23:22" :member)
   (test-record 2 13  3 1   #inst "2014-04-12T12:12:04" #inst "2014-04-12T13:01:52" :customer)
   (test-record 3 11 18 42  #inst "2014-09-01T08:52:54" #inst "2014-09-01T09:11:37" :member)
   (test-record 4 12 42 15  #inst "2015-03-11T10:33:19" #inst "2015-03-11T10:38:00" :customer)
   (test-record 5 99 42 42  #inst "2015-03-13T17:21:11" #inst "2015-03-13T17:54:37" :customer)
   ])


(deftest record-collection-tests
  (testing "should return a record collection"
    (let [record-collection (recs/make-record-collection on-heap-records)]
      (is (= 5 (count record-collection)))
      (is (= 11 (:bike-id (nth record-collection 2))))))

  (testing "should be reducible"
    (let [record-collection (recs/make-record-collection on-heap-records)]
      (is (integer? (reduce
                     (fn [m v] (+ m (- (:stop-time v) (:start-time v))))
                     0
                     record-collection)))))

  (testing "reducible should respect early-termination of first element"
    (let [record-collection (recs/make-record-collection on-heap-records)]
      (is (= :reduced (reduce
                       (fn [m v] (reduced :reduced))
                       :init-val
                       record-collection)))))

  (testing "reducible should respect early-termination of initial elements with no init"
    (let [record-collection (recs/make-record-collection on-heap-records)]
      (is (= :reduced (reduce
                       (fn
                         ([m v] :not-reduced)
                         ([m] (reduced :reduced)))
                       record-collection)))))
  (testing "reducible should respect early-termination of subsequent elements with no init"
    (let [record-collection (recs/make-record-collection on-heap-records)]
      (is (= :reduced (reduce
                       (fn
                         ([m v] (if (= :init m)
                                  (reduced :reduced)
                                  :not-reduced))
                         ([m] :init))
                       record-collection)))))
  (testing "reducible should work for empty collection"
    (is (= :reduced (reduce (fn [] :reduced)  (recs/make-record-collection []))))
    (is (= :reduced (reduce (fn [] (reduced :reduced))  (recs/make-record-collection []))))))

