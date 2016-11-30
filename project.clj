(defproject osqi-raw-es "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [clj-kafka "0.2.8-0.8.1.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [cheshire "5.4.0"]
                 [yieldbot/torna "0.1.7-SNAPSHOT"]
                 [org.slf4j/slf4j-log4j12 "1.7.10"]
                 [clojurewerkz/elastisch "2.2.2"]]
  :main ^:skip-aot osqi-raw-es.core
  :plugins [[brightnorth/uberjar-deploy "1.0.1" ]]
  :profiles {:uberjar {:aot :all}})
