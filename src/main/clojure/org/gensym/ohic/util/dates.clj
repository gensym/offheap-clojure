(ns org.gensym.ohic.util.dates
  (:require [clj-time.format :as tf])
  (:import [java.util Date]))

(def time-formatter (tf/formatter "M/d/yyyy H:mm ZZZ"))
(def legacy-time-formatter (tf/formatter "yyyy-M-d H:m ZZZ"))

(defn from-millis [^long millis]
  (Date. millis))

(defn from-2014-time-format [timestr]
  (->> (str timestr " America/Chicago")
       (tf/parse time-formatter)))

(defn from-2013-time-format [timestr]
  (->> (str timestr " America/Chicago")
       (tf/parse legacy-time-formatter)))
