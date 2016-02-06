(ns org.gensym.ohic.ride-records.divvy-ride-records
  (:require
   [clojure.string :as string]
   [org.gensym.ohic.util.dates :as dates]
   [org.gensym.ohic.util.performance-tools :as perf])
  (:import
   [java.io BufferedReader InputStreamReader File]
   [java.util.zip ZipFile ZipEntry ZipInputStream]))

(def datafile (clojure.java.io/resource "data/datachallenge.zip"))


(def default-masseuse
  {:stoptime (fn [v] [:stoptime (dates/from-2014-time-format v)])
   :starttime (fn [v] [:starttime (dates/from-2014-time-format v)])
   :usertype (fn [v] [:usertype (get {"Subscriber" "Member"} v v)])})

(def data-files
  {(clojure.java.io/resource "data/datachallenge.zip")
   [["datachallenge/Divvy_Stations_Trips_2013/Divvy_Trips_2013.csv"
     identity
     (merge default-masseuse
            {:stoptime (fn [v] [:stoptime (dates/from-2013-time-format v)])
             :starttime (fn [v] [:starttime (dates/from-2013-time-format v)])})]
    ["datachallenge/Divvy_Stations_Trips_2014/Divvy_Stations_Trips_2014_Q1Q2/Divvy_Trips_2014_Q1Q2.csv" reverse default-masseuse]
    ["datachallenge/Divvy_Stations_Trips_2014/Divvy_Stations_Trips_2014_Q3Q4/Divvy_Trips_2014-Q3-07.csv"  reverse default-masseuse]
    ["datachallenge/Divvy_Stations_Trips_2014/Divvy_Stations_Trips_2014_Q3Q4/Divvy_Trips_2014-Q3-0809.csv" reverse default-masseuse]
    ["datachallenge/Divvy_Stations_Trips_2014/Divvy_Stations_Trips_2014_Q3Q4/Divvy_Trips_2014-Q4.csv" reverse default-masseuse]]

   (clojure.java.io/resource "data/Divvy_Trips_2015-Q1Q2.zip")
   [["Divvy_Trips_2015-Q1.csv" reverse default-masseuse]
    ["Divvy_Trips_2015-Q2.csv" reverse default-masseuse]]})

(def whitelist #{:bikeid
                 :starttime
                 :stoptime
                 :trip-id
                 :from-station-id
                 :to-station-id
                 :usertype})

;; These are properties that are known to have a finite set of values.
;; Be specifiying them, we can potentially decrease the memory footprint
;; of the dataset by interning their values.
(def repeated-string-properties-masseuses
  (reduce (fn [m v] (assoc m v (fn [x] [v (perf/string x)]))) {}
          #{:bikeid
            :from-station-id
            :to-station-id
            :usertype}))

(defn massage-record [masseuse record]
  "Masseuse is a hashmap of keys to functions. Each function takes a single argument. For each key in record that has a function in masseuse, replace the the key-value pair in the record with that returned from call the function on the original value in the record."
  (reduce
   (fn [m [k v]]
     (let [[k-1 v-1] ((get masseuse k (fn [_] [k v])) v)]
       (assoc m k-1 v-1)))
   {}
   record))

(defn whitelist-record [whitelist record]
  (reduce
   (fn [m k]
     (if (whitelist k) m (dissoc m k)))
   record
   (keys record)))

(defn snake->kebab-case [s]
  (string/replace s "_" "-"))

(defn- to-map-seq [s]
  (let [keys (map (comp keyword snake->kebab-case) (string/split (first s) #","))]
    (map (fn [line] (zipmap keys (string/split line #",")))
         (rest s))))

(defn from-resource [resource]
  (let [path (.getPath resource)
        zipfile  (ZipFile. (.getPath resource))
        entries (enumeration-seq (.entries zipfile))
        ride-entries (filter #(re-find #"Divvy_Trips.*\.csv$" (.getName %)) entries)]
    (reduce (fn [m v] (assoc m (.getName v)
                             (line-seq (BufferedReader.
                                        (InputStreamReader. (.getInputStream zipfile v))))))
            {}
            ride-entries)))

(defn load-from-files []
  (mapcat (fn [[datafile internal-files]]
            (let [fileseqs (from-resource datafile)]
              (mapcat (fn [[filename collection-function data-mappings]]
                        (let [recs
                              (->> filename
                                   fileseqs
                                   (to-map-seq)
                                   (map (partial whitelist-record whitelist))
                                   (map #(massage-record (merge
                                                          repeated-string-properties-masseuses
                                                          data-mappings) %))
                                   collection-function)]
                          recs))
                      internal-files)))
          data-files))
