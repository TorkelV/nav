(ns nav.core
  (:require [org.httpkit.server :as server]
            [org.httpkit.client :as http]
            [hickory.core :as hick]
            [hickory.select :as s]
            [compojure.core :refer [routes POST GET ANY]]
            [clojure.data.json :as json]
            [clojure.java.jdbc :as sql]
            [clojure.string :as cstr]
            [clojure-csv.core :as csv]))

(def db "postgresql://localhost:5432/nav?user=postgres&password=123")

;;https://crossclj.info/ns/org.clojure/clojurescript/1.10.312/cljs.util.html#_distinct-by
(defn distinct-by
  ([f coll]
   (let [step (fn step [xs seen]
                (lazy-seq
                  ((fn [[x :as xs] seen]
                     (when-let [s (seq xs)]
                       (let [v (f x)]
                         (if (contains? seen v)
                           (recur (rest s) seen)
                           (cons x (step (rest s) (conj seen v)))))))
                    xs seen)))]
     (step coll #{}))))

(extend-protocol sql/IResultSetReadColumn
  org.postgresql.jdbc4.Jdbc4Array
  (result-set-read-column [pgobj metadata i]
    (vec (.getArray pgobj))))

(defn csv-data->maps [csv-data]
  (map #(zipmap (first csv-data) %)
       (rest csv-data)))


(def re-keywords '[["Java" #"(?i)[^A-zæøå]java[^s]"]
                   ["JavaScript" #"(?i)javascript|([^A-zæøå]JS[^A-zæøå@])"]
                   ["Clojure" #"(?i)clojure[^s]"]
                   ["ClojureScript" #"(?i)clojurescript"]
                   ["Python" #"(?i)python"]
                   ["Kotlin" #"(?i)kotlin"]
                   ["R" #"[^A](/|\(|,|\s|\.)R(/|\s|\.|,|\))[^3]"]
                   ["SQL" #"(?i)SQL"]
                   ["C#" #"(?i)(C.?#)|(C.?sharp)"]
                   ["F#" #"(?i)(F.?#)|(F.?sharp)"]
                   ["C++" #"(?i)c.?\+\+"]
                   ["C" #"(/|\(|,|\s|\.)C(/|\s|\.|,|\))\s*[^s#+]"]
                   ["PHP" #"(?i)([^A-zæøå]php7?[^A-zæøå])"]
                   ["Ruby" #"(?i)([^A-zæøå]ruby[^A-zæøå])"]
                   ["Scala" #"(?i)([^A-zæøå]scala[^A-zæøå])"]
                   ["TypeScript" #"(?i)typescript|([^A-zæøå]TS[^A-zæøå@])"]
                   ["LUA" #"(?i)([^A-zæøå]lua[^A-zæøå])"]
                   ["Go" #"([^A-zæøå](GO|Go)[^A-zæøå])"]
                   ["Haskell" #"(?i)haskell"]
                   ["Swift" #"(?i)([^A-zæøå]swift[^A-zæøå])"]
                   ["Objective C" #"(?i)([^A-zæøå](obj(ective)?.?c)[^A-zæøå])"]
                   ["CSS" #"(?i)([^A-zæøå]css3?[^A-zæøå])"]
                   ["HTML" #"(?i)([^A-zæøå]html5?[^A-zæøå])"]
                   ["XML" #"(?i)([^A-zæøå]XML[^A-zæøå])"]
                   ["Sparql" #"(?i)sparql"]
                   ["Xquery" #"(?i)xquery"]
                   ["Groovy" #"(?i)([^A-zæøå]groovy[^A-zæøå])"]
                   ["Angular" #"(?i)([^A-zæøå]angular.?(js)?[^A-zæøå])"]
                   ["React" #"(?i)([^A-zæøå]react.?(js)?[^A-zæøå])"]
                   ["Vue" #"(?i)([^A-zæøå]vue.?(js)?[^A-zæøå])"]
                   ["Spring" #"(?i)([^A-zæøå]spring(boot)?[^A-zæøå])"]
                   ["Datadog" #"(?i)([^A-zæøå]datadog[^A-zæøå])"]
                   ["jQuery" #"(?i)([^A-zæøå]jquery[^A-zæøå])"]
                   [".NET" #"(?i)([^A-zæøå]\.net[^A-zæøå])"]
                   ["ASP.NET" #"(?i)([^A-zæøå]asp\.net[^A-zæøå])"]
                   ["PostgreSQL" #"(?i)([^A-zæøå]postgres(ql)?[^A-zæøå])"]
                   ["MySQL" #"(?i)([^A-zæøå]mysql[^A-zæøå])"]
                   ["Jenkins" #"(?i)([^A-zæøå]jenkins[^A-zæøå])"]
                   ["Maven" #"(?i)([^A-zæøå]maven[^A-zæøå])"]
                   ["Docker" #"(?i)([^A-zæøå]docker[^A-zæøå])"]
                   ["GIT" #"(?i)([^A-zæøå]git(lab)?(hub)?[^A-zæøå])"]
                   ["Trello" #"(?i)([^A-zæøå]trello[^A-zæøå])"]
                   ["Jira" #"(?i)([^A-zæøå]jira|confluence[^A-zæøå])"]
                   ["Confluence" #"(?i)([^A-zæøå]jira|confluence[^A-zæøå])"]
                   ["Node.js" #"(?i)([^A-zæøå](node(.?js)?|npm|yarn)[^A-zæøå])"]
                   ["TDD" #"(?i)([^A-zæøå](tdd|test.?driven.?development)[^A-zæøå])"]
                   ["Scrum" #"(?i)[^A-zæøå]scrum"]
                   ["Kanban" #"(?i)([^A-zæøå]kanban[^A-zæøå])"]
                   ["AWS" #"(?i)([^A-zæøå](aws|amazon))"]
                   ["Azure" #"(?i)([^A-zæøå](azure)[^A-zæøå])"]
                   ["Guava" #"(?i)([^A-zæøå](guava)[^A-zæøå])"]
                   ["Linux" #"(?i)linux|ubuntu|unix"]
                   ["GIS" #"(?i)([^A-zæøå](gis)[^A-zæøå])"]
                   ["REST" #"([^A-zæøå](REST|Rest)[^A-zæøå])"]
                   ["Machine Learning" #"(?i)([^A-zæøå](maskinlæring|ml|machine learning)[^A-zæøå])"]
                   ["NLP" #"(?i)[^A-zæøå](NLP|natural language (processing)?)[^A-zæøå]"]
                   ["Artificial Intelligence" #"(?i)([^A-zæøå](ai|artificial intelligence|maskinlæring|ml|((machine|deep) learning)|NLP|natural language (processing)?)[^A-zæøå])"]
                   ["IOT" #"(?i)([^A-zæøå](iot|internet of things)[^A-zæøå])"]
                   ["Big Data" #"(?i)([^A-zæøå](big data|stordata)[^A-zæøå])"]
                   ["Unity" #"(?i)[^A-zæøå]unity"]
                   ["GraphQL" #"(?i)graphql"]
                   ["ETL" #"(?i)([^A-zæøå](etl|extract.?transform.?load)[^A-zæøå])"]
                   ["SaaS" #"(?i)([^A-zæøå](saas|software.?as.?a.?service)[^A-zæøå])"]
                   ["Cloud" #"(?i)([^A-zæøå](cloud|sky(en|basert)?)[^A-zæøå])"]
                   ["Frontend" #"(?i)front.?end"]
                   ["Backend" #"(?i)back.?end"]
                   ["Fullstack" #"(?i)full.?stack"]
                   ["Devops" #"(?i)[^A-zæøå](dev.?ops)"]
                   ["TensorFlow" #"(?i)[^A-zæøå](tensor.?flow)"]
                   ["Business Intelligence" #"(?i)([^A-zæøå](BI|Business.?Intelligence)[^A-zæøå])"]
                   ["iOS" #"(?i)([^A-zæøå]iOS[^A-zæøå])"]
                   ["Android" #"(?i)[^A-zæøå]Android"]
                   ["Gradle" #"(?i)([^A-zæøå](gradle)[^A-zæøå])"]
                   ["Nim" #"(?i)([^A-zæøå](nim)[^A-zæøå])"]
                   ["Cobol" #"(?i)([^A-zæøå](cobol)[^A-zæøå])"]
                   ["UML" #"(?i)([^A-zæøå](UML)[^A-zæøå])"]
                   ["Redux" #"(?i)([^A-zæøå](Redux)[^A-zæøå])"]
                   ["JSP" #"(?i)([^A-zæøå]JSP[^A-zæøå])"]
                   ["Bootstrap" #"(?i)([^A-zæøå]Bootstrap[^A-zæøå])"]
                   ["Akka" #"(?i)([^A-zæøå]Akka[^A-zæøå])"]
                   ["Laravel" #"(?i)([^A-zæøå]Laravel[^A-zæøå])"]
                   ["Swing" #"(?i)([^A-zæøå]Swing[^A-zæøå])"]
                   ["JavaFX" #"(?i)([^A-zæøå]Java.?FX[^A-zæøå])"]
                   ["Ember" #"(?i)([^A-zæøå]Ember[^A-zæøå])"]
                   ["SAP" #"(?i)([^A-zæøå]SAP[^A-zæøå])"]
                   ["RPG" #"(?i)([^A-zæøå]RPG[^A-zæøå])"]
                   ["Pascal" #"(?i)([^A-zæøå]pascal[^A-zæøå])"]
                   ["Delphi" #"(?i)([^A-zæøå]Delphi[^A-zæøå])"]
                   ["Concorde" #"(?i)([^A-zæøå]Concorde[^A-zæøå])"]
                   ["MATLAB" #"(?i)([^A-zæøå]matlab[^A-zæøå])"]
                   ["JBuilder" #"(?i)([^A-zæøå]jbuilder[^A-zæøå])"]
                   ["JRules" #"(?i)([^A-zæøå]jrules[^A-zæøå])"]
                   ["CRM" #"(?i)([^A-zæøå]CRM[^A-zæøå])"]
                   ["ERP" #"(?i)([^A-zæøå]ERP[^A-zæøå])"]
                   ["CRM" #"(?i)([^A-zæøå]CRM[^A-zæøå])"]
                   ["IBM" #"(?i)([^A-zæøå]IBM[^A-zæøå])"]
                   ["PLC" #"(?i)([^A-zæøå](PLS|PLC)[^A-zæøå])"]
                   ["Powershell" #"(?i)([^A-zæøå](Powershell)[^A-zæøå])"]
                   ])

(defn filter-ads-regex [regex column a]
  (remove #(nil? (re-find regex (column %))) a))


(defn save-keywords [keywords]
  (let [a (sql/query db ["select ad_id, concat(title,' ',description) as description from descs"])]
    (->> (map #(vec [(first %) (map :ad_id (filter-ads-regex (last %) :description a))]) keywords)
         (map (fn [v] (map #(assoc '{} :keyword (first v) :ad_id %) (last v))))
         (apply concat)
         (sql/insert-multi! db :keywords))))

(defn rename-keys-ads [ads]
  (clojure.set/rename-keys ads {"Bedrift Naring Primar Kode" :industry_code
                                "sistepubl_dato"             :last_published
                                "stillingsnummer"            :ad_number
                                "arbeidssted_landkode"       :country_code
                                "nav_enhet_kode"             :nav_enhet_kode
                                "arbeidssted_kommunenummer"  :kommunenr
                                "yrkesbetegnelse"            :proffesion_title
                                "registrert_dato"            :registered
                                "stillingstittel"            :job_title
                                "Bedrift Navn"               :org_name
                                "yrkeskode"                  :proffesion_code
                                "Aktiv Flagg"                :active
                                "statistikk_aar_mnd"         :statistics_year_month
                                "arbeidssted_fylke"          :fylke
                                "Foretak Navn"               :business_name
                                "Foretak Sektor Gruppe"      :enterprise_sector_group
                                "Bedrift Org Nr"             :orgnr
                                "yrke"                       :proffesion
                                "stilling_kilde"             :source
                                "antall_stillinger"          :positions
                                "offisiell_statistikk_flagg" :official_statistic
                                "stilling_id"                :ad_id
                                "Foretak Org Nr"             :business_orgnr
                                "yrke_grovgruppe"            :proffesion_group
                                "stillingsnummer_nav_no"     :ad_id_nav
                                "isco_versjon"               :isco_version
                                "arbeidssted_kommune"        :kommune
                                "Tilleggskriterium"          :additional_criteria
                                "arbeidssted_fylkesnummer"   :fylkesnr
                                "arbeidssted_land"           :country
                                }))

(defn rename-keys-texts [texts]
  (clojure.set/rename-keys texts {"Stillingsnummer nav no"      :ad_id_nav
                                  "Stilling Id"                 :ad_id
                                  "Tittel vasket"               :title
                                  "Stillingsbeskrivelse vasket" :description
                                  }))




(defn clean-html [t]
  (-> t
      (cstr/replace #"<[^>]*>" " ")
      (cstr/replace #"&nbsp;" " ")))

(defn ads-in [ads]
  (sql/insert-multi! db :ads ads))

(defn descs-in [descs]
  (sql/insert-multi! db :descs descs))

(defn read-ads [path]
  (->> (csv/parse-csv (slurp path) :delimiter \;)
       (csv-data->maps)
       (map rename-keys-ads)
       (filter #(= (:proffesion_group %) "Ingeniør- og ikt-fag (2001-2011)"))
       (map #(update % :proffesion_group (fn [v] "Ingeniør- og ikt-fag")))
       (distinct-by :ad_id)
       (remove #(nil? (:ad_id %)))))

(defn read-descs [path]
  (->> (csv/parse-csv (slurp path) :delimiter \;)
       (csv-data->maps)
       (map rename-keys-texts)
       (distinct-by :ad_id)
       (remove #(nil? (:ad_id %)))
       (map #(update % :description clean-html))))

(defn pop-by-year []
  (->>(sql/query db ["select keyword, c as freq,j.year, round((c::numeric)/(c2::numeric),4)*100 as precent, array_agg(distinct ads.orgnr) from (\nselect keyword, year, count(keyword) as c \nfrom keywords\ninner join ads on keywords.ad_id = ads.ad_id\ngroup by year, keyword\norder by c desc ) j\ninner join (select count(ads.ad_id) c2, year from ads group by year) j2 on j2.year = j.year\ninner join ads on j.year = ads.year and j.keyword in (select keyword from keywords where keywords.ad_id = ads.ad_id) \ngroup by keyword, freq, j.year, precent\norder by year asc"])
      (group-by :keyword)))

(defn pop-by-year-w []
  (->>(sql/query db ["select keyword, c as freq,j.year, round((c::numeric)/(c2::numeric),4)*100 as precent, array_agg(distinct ads.orgnr) from (\nselect keyword, year, count(keyword) as c \nfrom keywords\ninner join ads on keywords.ad_id = ads.ad_id\ngroup by year, keyword\norder by c desc ) j\ninner join (select count(ads.ad_id) c2, year from ads where ad_id in (select ad_id from keywords) group by year) j2 on j2.year = j.year\ninner join ads on j.year = ads.year and j.keyword in (select keyword from keywords where keywords.ad_id = ads.ad_id) \ngroup by keyword, freq, j.year, precent\norder by year asc"])
      (group-by :keyword)))


(defn app []
  (routes
    (GET "/popularity-year/" []
      {:status  200
       :headers {"Content-Type" "application/json" "Access-Control-Allow-Origin" "*"}
       :body    (json/write-str (pop-by-year))})
    (GET "/popularity-year-w/" []
      {:status  200
       :headers {"Content-Type" "application/json" "Access-Control-Allow-Origin" "*"}
       :body    (json/write-str (pop-by-year-w))})))


(defn create-server []
  (server/run-server (app) {:port 8080}))

(defn -main [& args]
  (create-server))
