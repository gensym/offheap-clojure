(ns org.gensym.ohic.ride-records.examples-tests
  (:require [clojure.test :refer :all]
            [org.gensym.ohic.ride-records.examples :as ex]
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

(deftest count-unique-bikes-on-heap-tests
  (testing "should count unique bikes"
    (is (= 4 (ex/count-unique-bikes on-heap-records)))
    (is (= 0 (ex/count-unique-bikes []))))

  (testing "should count unique bikes with transduce"
    (is (= 4 (ex/count-unique-bikes-transduce on-heap-records)))
    (is (= 0 (ex/count-unique-bikes-transduce [])))))

(deftest count-unique-bikes-off-heap-tests
  (let [offheap-records (recs/make-record-collection on-heap-records)
        empty-records (recs/make-record-collection [])]

    (testing "should count unique bikes offheap"
      (is (= 4 (ex/count-unique-bikes-off-heap offheap-records)))
      (is (= 0 (ex/count-unique-bikes-off-heap empty-records))))

    (testing "should count unique bikes offheap with indexable"
      (is (= 4 (ex/count-unique-bikes-off-heap-nth offheap-records)))
      (is (= 0 (ex/count-unique-bikes-off-heap-nth empty-records))))

    (testing "should count unique bikes offheap with transduce"
      (is (= 4 (ex/count-unique-bikes-off-heap-transduce offheap-records)))
      (is (= 0 (ex/count-unique-bikes-off-heap-transduce empty-records))))))

(deftest count-minutes-ridden-on-heap-tests
  (testing "should count minutes ridden"
    (is (= 79 (int (ex/count-minutes-ridden on-heap-records 42))))
    (is (= 0 (int (ex/count-minutes-ridden [] 42)))))

  (testing "should count minutes ridden with a loop"
    (is (= 79 (int (ex/count-minutes-ridden-loop on-heap-records 42))))
    (is (= 0 (int (ex/count-minutes-ridden-loop [] 42)))))

  (testing "should count minutes ridden with transduce"
    (is (= 79 (int (ex/count-minutes-ridden-transduce on-heap-records 42))))
    (is (= 0 (int (ex/count-minutes-ridden-transduce [] 42))))))

(deftest count-minutes-ridden-off-heap-tests
  (let [off-heap-records (recs/make-record-collection on-heap-records)
        empty-records (recs/make-record-collection [])]
    (testing "should count minutes ridden off heap"
      (is (= 79 (int (ex/count-minutes-ridden-off-heap off-heap-records 42))))
      (is (= 0 (int (ex/count-minutes-ridden-off-heap empty-records 42)))))

    (testing "should count minutes ridden off heap using indexable"
      (is (= 79 (int (ex/count-minutes-ridden-off-heap-nth off-heap-records 42))))
      (is (= 0 (int (ex/count-minutes-ridden-off-heap-nth empty-records 42)))))

    (testing "should count minutes ridden off heap using indexable and keywords"
      (is (= 79 (int (ex/count-minutes-ridden-off-heap-nth-keyword off-heap-records 42))))
      (is (= 0 (int (ex/count-minutes-ridden-off-heap-nth-keyword empty-records 42)))))

    (testing "should count minutes ridden off heap using transduce"
      (is (= 79 (int (ex/count-minutes-ridden-off-heap-nth-keyword off-heap-records 42))))
      (is (= 0 (int (ex/count-minutes-ridden-off-heap-nth-keyword empty-records 42)))))))

