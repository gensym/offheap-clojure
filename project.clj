(defproject offheap-clojure "0.1"
  :description "Examples and benchmarks for using offheap data with clojure"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0-RC1"]
                 [org.clojure/data.json "0.2.5"]
                 [clj-time "0.8.0"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.openjdk.jmh/jmh-core "1.9.3"]
                 [org.openjdk.jmh/jmh-generator-annprocess "1.9.3"]]

  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :test-paths ["src/test/clojure"]

  :profiles { :benchmark {:main org.openjdk.jmh.Main
                          :java-source-paths ["src/main/java" "src/benchmark/java"]
                          :prep-tasks [["compile" "org.gensym.ohic.benchmark.harness"] "javac" "compile"]}
             :build-cache { :jvm-opts ["-Xmx8g"] }
             :uberjar {:aot :all}}
  :aliases {
            "make-record-cache"
            ["with-profile" "build-cache" "trampoline" "run" "-m" "org.gensym.ohic.record-cache/make-cache"]

            "clean-record-cache"
            ["trampoline" "run" "-m" "org.gensym.ohic.record-cache/clean-cache"]}

  :main ^:skip-aot org.gensym.ohic.core
  :target-path "target/%s"
  ;; We want StringTableSize to be a prime number - See http://java-performance.info/string-intern-in-java-6-7-8/ for details
  :jvm-opts ["-DinternStrings=true" "-XX:StringTableSize=1000003" "-Xmx8g"])
