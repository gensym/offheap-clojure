(ns org.gensym.ohic.util.performance-tools)

;; If you're going to use this, you'll probably want to set -XX:StringTableSize to something
;; larger than the default. See http://java-performance.info/string-intern-in-java-6-7-8/
(def intern-strings (Boolean/getBoolean "internStrings"))

(defn string [s]
  (if intern-strings
    (.intern s)
    s))
