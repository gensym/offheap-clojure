(ns org.gensym.ohic.ride-records.off-heap-ride-records
  (:require
   [clojure.string :as string]
   [org.gensym.ohic.ride-records.divvy-ride-records :as records]
   [org.gensym.ohic.util.dates :as dates])
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

(defn get-trip-id
  ([^Unsafe unsafe object-offset]
     (.getLong unsafe (+ object-offset trip-id-offset)))
  ([record]
     (let [^Unsafe unsafe (unsafe record)]
       (.getInt unsafe (+ (address record) trip-id-offset)))))

(defn set-trip-id! [^Unsafe unsafe object-offset trip-id]
  (.putLong unsafe (+ object-offset trip-id-offset) trip-id))

(defn get-from-station-id
  ([^Unsafe unsafe object-offset]
     (.getInt unsafe (+ object-offset from-station-id-offset)))
  ([record]
     (let [^Unsafe unsafe (unsafe record)]
       (.getInt unsafe (+ (address record) from-station-id-offset)))))

(defn set-from-station-id! [^Unsafe unsafe object-offset station-id]
  (.putInt unsafe (+ object-offset from-station-id-offset) station-id))

(defn get-to-station-id
  ([^Unsafe unsafe object-offset]
     (.getInt unsafe (+ object-offset to-station-id-offset)))
  ([record]
     (let [^Unsafe unsafe (unsafe record)]
       (.getInt unsafe (+ (address record) to-station-id-offset)))))

(defn set-to-station-id! [^Unsafe unsafe object-offset station-id]
  (.putInt unsafe (+ object-offset to-station-id-offset) station-id))

(defn get-bike-id
  ([^Unsafe unsafe object-offset]
     (.getInt unsafe (+ object-offset bike-id-offset)))
  ([record]
     (let [^Unsafe unsafe (unsafe record)]
       (.getInt unsafe (+ (address record) bike-id-offset)))))

(defn set-bike-id! [^Unsafe unsafe object-offset  bike-id]
  (.putInt unsafe (+ object-offset bike-id-offset) bike-id))

(defn get-start-time
  ([^Unsafe unsafe object-offset]
     (.getLong unsafe (+ object-offset start-time-offset)))
  ([record]
     (let [^Unsafe unsafe (unsafe record)]
       (.getInt unsafe (+ (address record) start-time-offset)))))

(defn set-start-time! [^Unsafe unsafe object-offset start-time]
  (.putLong unsafe (+ object-offset start-time-offset) start-time))

(defn get-stop-time
  ([^Unsafe unsafe object-offset]
     (.getLong unsafe (+ object-offset stop-time-offset)))
  ([record]
     (let [^Unsafe unsafe (unsafe record)]
       (.getInt unsafe (+ (address record) stop-time-offset)))))

(defn set-stop-time! [^Unsafe unsafe object-offset stop-time]
  (.putLong unsafe (+ object-offset stop-time-offset) stop-time))

(defn get-user-type
  ([^Unsafe unsafe object-offset]
     (.getChar unsafe (+ object-offset user-type-offset)))
  ([record]
     (let [^Unsafe unsafe (unsafe record)]
       (.getInt unsafe (+ (address record) user-type-offset)))))

(defn set-user-type! [^Unsafe unsafe object-offset user-type]
  (.putChar unsafe (+ object-offset user-type-offset) user-type))

(defn- getUnsafe ^Unsafe []
  (let [f (.getDeclaredField Unsafe "theUnsafe")]
    (.setAccessible f true)
    (.get f nil)))

(def user-type->id {:customer \C
                    :member \M
                    :dependent \D})
(def id->user-type {\C :customer
                    \M :member
                    \D :dependent})

(defprotocol AddressableUnsafe
  (unsafe [this])
  (address [this]))

(deftype Record [the-unsafe offset]
  AddressableUnsafe
  (unsafe [_] the-unsafe)
  (address [_] offset)

  clojure.lang.ILookup

  (valAt [this key not-found]
    (case key
      :bike-id (get-bike-id the-unsafe offset)
      :from-station-id (get-from-station-id the-unsafe offset)
      :to-station-id (get-to-station-id the-unsafe offset)
      :start-time (get-start-time the-unsafe offset)
      :stop-time (get-stop-time the-unsafe offset)
      :user-type (id->user-type (get-user-type the-unsafe offset))
      not-found))

  (valAt [this key] (.valAt this key nil))

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
                            [:bike-id
                             :start-time
                             :stop-time
                             :from-station-id
                             :to-station-id
                             :user-type]))))

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
  ([^Unsafe unsafe address num-records f v]
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

(defprotocol Serializable (serialize [this ^OutputStream output-stream]))


(defprotocol Trimmable
  (trim-to [this start-index end-index]))

(defprotocol Disposable (dispose [this]))

;; root? indicates whether this is derived from an existing RecordCollection - i.e., whether its records are in the same memory space as that collection
(deftype RecordCollection [^Unsafe unsafe address num-records root?]

  AddressableUnsafe
  (unsafe [_] unsafe)
  (address [_] address)

  Disposable
  (dispose [_] (when root? (.freeMemory unsafe address)))

  clojure.lang.Indexed
  (nth [_ i] (Record. unsafe (+ address (* i object-size)) ))

  (count [_] num-records)

  clojure.core.protocols/CollReduce
  (coll-reduce [_ f] (unsafe-reduce unsafe address num-records f))
  (coll-reduce [_ f v] (unsafe-reduce unsafe address num-records f v))

  Serializable
  (serialize [this output-stream]
    (serialize-bytes output-stream unsafe address (* num-records object-size)))

  Trimmable
  (trim-to [this start-index end-index]
    (RecordCollection. unsafe
                       (+ address (* start-index object-size))
                       (inc (- end-index start-index))
                       false)))


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
                (set-trip-id! unsafe offset (:trip-id record))
                (set-from-station-id! unsafe offset (:from-station-id record))
                (set-to-station-id! unsafe offset (:to-station-id record))
                (set-bike-id! unsafe offset (:bike-id record))
                (set-start-time! unsafe offset (:start-time record))
                (set-stop-time! unsafe offset (:stop-time record))
                (set-user-type! unsafe offset (user-type->id (:user-type record)))
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

(defn deserialize [input-stream]
  (deserialize-bytes input-stream (getUnsafe)))
