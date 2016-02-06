(ns org.gensym.ohic.ride-records.off-heap-ride-records
  (:require
   [clojure.string :as string]
   [org.gensym.ohic.ride-records.divvy-ride-records :as records]
   [org.gensym.ohic.util.dates :as dates]
   [org.gensym.ohic.util.integer-ids :as ids]
   [org.gensym.ohic.performance-tools :as perf])
  (:import [sun.misc Unsafe]
           [java.io InputStream OutputStream DataInputStream DataOutputStream]
           [org.joda.time DateTime]))

(def trip-id-offset 0)
(def from-station-id-offset 8)
(def to-station-id-offset 12)
(def bike-id-offset 16)
(def start-time-offset 20)
(def stop-time-offset 28)
(def user-type-offset 36)
(def object-size 38)

(defn get-trip-id [^Unsafe unsafe object-offset]
  (.getLong unsafe (+ object-offset trip-id-offset)))

(defn set-trip-id! [^Unsafe unsafe object-offset trip-id]
  (.putLong unsafe (+ object-offset trip-id-offset) trip-id))

(defn get-from-station-id [^Unsafe unsafe object-offset]
  (.getInt unsafe (+ object-offset from-station-id-offset)))

(defn set-from-station-id! [^Unsafe unsafe object-offset station-id]
  (.putInt unsafe (+ object-offset from-station-id-offset) station-id))

(defn get-to-station-id [^Unsafe unsafe object-offset]
  (.getInt unsafe (+ object-offset to-station-id-offset)))

(defn set-to-station-id! [^Unsafe unsafe object-offset station-id]
  (.putInt unsafe (+ object-offset to-station-id-offset) station-id))

(defn get-bike-id
  ([^Unsafe unsafe object-offset]
     (.getInt unsafe (+ object-offset bike-id-offset)))
  ([record]
     (let [^Unsafe unsafe (:unsafe record)]
       (.getInt unsafe (+ (:address record) bike-id-offset)))))

(defn set-bike-id! [^Unsafe unsafe object-offset  bike-id]
  (.putInt unsafe (+ object-offset bike-id-offset) bike-id))

(defn get-start-time [^Unsafe unsafe object-offset]
  (.getLong unsafe (+ object-offset start-time-offset)))

(defn set-start-time! [^Unsafe unsafe object-offset start-time]
  (.putLong unsafe (+ object-offset start-time-offset) start-time))

(defn get-stop-time [^Unsafe unsafe object-offset]
  (.getLong unsafe (+ object-offset stop-time-offset)))

(defn set-stop-time! [^Unsafe unsafe object-offset stop-time]
  (.putLong unsafe (+ object-offset stop-time-offset) stop-time))

(defn get-user-type [^Unsafe unsafe object-offset]
  (.getChar unsafe (+ object-offset user-type-offset)))

(defn set-user-type! [^Unsafe unsafe object-offset user-type]
  (.putChar unsafe (+ object-offset user-type-offset) user-type))

(defn- getUnsafe ^Unsafe []
  (let [f (.getDeclaredField Unsafe "theUnsafe")]
    (.setAccessible f true)
    (.get f nil)))

(def read-int (comp int read-string))
(def read-long read-string)
(def user-type->id {"Customer" \C
                    "Member" \M
                    "Dependent" \D})

(def id->user-type {\C "Customer"
                    \M "Member"
                    \D "Dependent"})

(def id->user-type-sym {\C :customer
                        \M :member
                        \D :dependent})


(defn starttime ^DateTime [loaded-record]
  (:starttime loaded-record))

(defn stoptime ^DateTime [loaded-record]
  (:stoptime loaded-record))

(defprotocol Serializable (serialize [this ^OutputStream output-stream]))

(defprotocol Disposable (dispose [this]))

(defprotocol Trimmable
  (trim-to [this start-index end-index]))

(defprotocol AddressableUnsafe
  (unsafe [this])
  (address [this]))

(defprotocol RecordObject
  (bike-id [this]))

(deftype Record [the-unsafe offset]
  Object
  (equals [this that]
    (if (not (= (type this) (type that)))
      false
      (loop [a-this (address this)
             a-that (address that)
             remaining object-size]
        (cond
         (= 0 remaining) true
         (not (= (.getByte (unsafe this) a-this)
                 (.getByte (unsafe that) a-that))) false
                 :else (recur (inc a-this) (inc a-that) (dec remaining))))))

  (hashCode [_]
    (loop [hash 1
           a offset
           remaining object-size]
      (cond
       (= 0 remaining) hash
       :else (recur (+ hash (* 17 (.getByte the-unsafe a)))
                    (inc a)
                    (dec remaining)))))

  (toString [this] (str
                    (reduce (fn [m v] (assoc m v (v this)))
                            {}
                            [:bikeid
                             :starttime
                             :stoptime
                             :from-station-id
                             :to-station-id
                             :user-type])))

  AddressableUnsafe
  (unsafe [_] the-unsafe)
  (address [_] offset)

  RecordObject
  (bike-id [_] (get-bike-id the-unsafe offset))

  clojure.lang.ILookup

  (valAt [this key not-found]
    (case key
      :bikeid (get-bike-id the-unsafe offset)
      :from-station-id (get-from-station-id the-unsafe offset)
      :to-station-id (get-to-station-id the-unsafe offset)
      :starttime (get-start-time the-unsafe offset)
      :stoptime (get-stop-time the-unsafe offset)
      :user-type (id->user-type-sym (get-user-type the-unsafe offset))
      not-found))
  (valAt [this key] (.valAt this key nil)))

(defn unsafe-reduce
  ([^Unsafe unsafe address num-records f]
     (if (= 0 num-records)
       (f)
       (loop [i 1
              offset address
              ret (f (Record. unsafe offset))]
         (if (= i num-records)
           ret
           (let [offset (+ offset object-size)
                 ret (f ret (Record. unsafe offset))]
             (if (reduced? ret)
               @ret
               (recur (inc i) offset ret)))))))
  ([unsafe address num-records f v]
     (loop [i 0
            offset address
            ret v]
       (if (= i num-records)
         ret
         (let [ret (f ret (Record. unsafe offset))]
           (if (reduced? ret)
             @ret
             (recur (inc i) (+ offset object-size) ret)))))))

(defn serialize-bytes [^OutputStream ostream ^Unsafe unsafe address num-bytes]
  (let [ostream (DataOutputStream. ostream)]
    (.writeLong ostream num-bytes)
    (dotimes [i num-bytes]
      (.write ostream (.getByte unsafe (+ address i))))))


;; root? indicates whether this is derived from an existing RecordCollection - i.e., whether its records are in the same memory space as that collection
(deftype RecordCollection [^Unsafe unsafe address num-records root?]

  clojure.core.protocols/CollReduce
  (coll-reduce [_ f] (unsafe-reduce unsafe address num-records f))
  (coll-reduce [_ f v] (unsafe-reduce unsafe address num-records f v))

  clojure.lang.Indexed
  (nth [_ i] (Record. unsafe (+ address (* i object-size)) ))

  (count [_] num-records)

  AddressableUnsafe
  (unsafe [_] unsafe)
  (address [_] address)

  Serializable
  (serialize [this output-stream]
    (serialize-bytes output-stream unsafe address (* num-records object-size)))

  Trimmable
  (trim-to [this start-index end-index]
    (RecordCollection. unsafe
                       (+ address (* start-index object-size))
                       (inc (- end-index start-index))
                       false))

  Disposable
  (dispose [_] (when root? (.freeMemory unsafe address))))

(defn deserialize-bytes [^InputStream istream ^Unsafe unsafe]
  (let [istream (DataInputStream. istream)
        num-bytes (.readLong istream)
        address (.allocateMemory unsafe num-bytes)]
    (try
      (do
        (dotimes [i num-bytes]
          (.putInt unsafe (+ address i) (.read istream)))
        (RecordCollection. unsafe address (/ num-bytes object-size) true))
      (catch Throwable t
        (do
          (.freeMemory unsafe address)
          (throw t))))))


(defn make-record-collection [loaded-records]
  (let [unsafe (getUnsafe)
        num-records (count loaded-records)
        required-size  (* object-size num-records)
        address  (.allocateMemory unsafe required-size)]

    (try
      (loop [idx 0
             records loaded-records
             offset address]
        (if (empty? records)
          (RecordCollection. unsafe address num-records true)
          (do
            (let [record (first records)]
              (try
                (set-trip-id! unsafe offset (read-long (:trip-id record)))
                (set-from-station-id! unsafe offset (read-int (:from-station-id record)))
                (set-to-station-id! unsafe offset (read-int (:to-station-id record)))
                (set-bike-id! unsafe offset (read-int (:bikeid record)))
                (set-start-time! unsafe offset (.getMillis (starttime record)))
                (set-stop-time! unsafe offset (.getMillis (stoptime record)))
                (set-user-type! unsafe offset (user-type->id (:usertype record)))
                (catch Exception e
                  (throw (RuntimeException.
                          (str "Failed parsing record " idx " (" record ")") e)))))
            (recur (inc idx)
                   (next records)
                   (long (+ offset object-size))))))

      (catch Throwable t
        (do
          (.freeMemory unsafe address)
          (throw t))))))

(defn make-empty-collection []
  (make-record-collection {}))

(defn read-record [record-collection index]
  (let [address (+ (:address record-collection) (* index object-size))
        unsafe (:unsafe record-collection)]
    {
     :trip-id (str (get-trip-id unsafe address))
     :from-station-id (str (get-from-station-id unsafe address))
     :to-station-id (str (get-to-station-id unsafe address))
     :bikeid (str (get-bike-id unsafe address))
     :starttime (dates/from-millis (get-start-time unsafe address))
     :stoptime (dates/from-millis (get-stop-time unsafe address))
     :usertype (id->user-type (get-user-type unsafe address))
     }))

(defn deserialize [input-stream]
  (deserialize-bytes input-stream (getUnsafe)))
