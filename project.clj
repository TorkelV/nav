(defproject nav "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx4G"]
  :main nav.core
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [http-kit "2.2.0"]
                 [compojure "1.6.1"]
                 [hickory "0.7.1"]
                 [ring "1.4.0-RC2"]
                 [org.clojure/java.jdbc "0.7.8"]
                 [org.postgresql/postgresql "9.4-1201-jdbc41"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/data.csv "0.1.4"]
                 [clojure-csv/clojure-csv "2.0.1"]]
  :plugins [[lein-ring "0.12.4" :exclusions [org.clojure/clojure]]]
  :ring {:handler nav.core/app})
