(ns nav.core
  (:require [org.httpkit.server :as server]
            [org.httpkit.client :as http]
            [hickory.core :as hick]
            [hickory.select :as s]
            [compojure.core :refer :all]
            [clojure.data.json :as json]
            [clojure.java.jdbc :as sql]
            [clojure.string :as cstr]
            [clojure-csv.core :as csv]
            [compojure.route :as route]))

(def db "postgresql://localhost:5432/nav?user=postgres&password=123")


(defn keywordize-keys [m]
  (let [f (fn [[k v]] (if (string? k) [(keyword k) v] [k v]))]
    (clojure.walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

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
                   ["C Sharp" #"(?i)([^A-zæøå](C.?#|C.?(S|s)harp)[^A-zæøå])"]
                   ["F Sharp" #"([^A-zæøå](F#|F.?(S|s)harp)[^A-zæøå])"]
                   ["C++" #"(?i)[^A-zæøå]c.?\+\+"]
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
                   ["AWS" #"(?i)([^A-zæøå]((aws)|(amazon.?web.?services))[^A-zæøå])"]
                   ["Azure" #"(?i)([^A-zæøå](azure)[^A-zæøå])"]
                   ["Guava" #"(?i)([^A-zæøå](guava)[^A-zæøå])"]
                   ["Linux" #"(?i)linux|ubuntu|unix"]
                   ["GIS" #"(?i)([^A-zæøå](gis)[^A-zæøå])"]
                   ["REST" #"([^A-zæøå](REST|Rest)[^A-zæøå])"]
                   ["ML" #"(?i)([^A-zæøå](maskinlæring|ml|machine learning)[^A-zæøå])"]
                   ["NLP" #"(?i)[^A-zæøå](NLP|natural language (processing)?)[^A-zæøå]"]
                   ["AI" #"(?i)([^A-zæøå](ai|artificial intelligence|maskinlæring|ml|((machine|deep) learning)|NLP|natural language (processing)?)[^A-zæøå])"]
                   ["IOT" #"(?i)([^A-zæøå](iot|internet of things)[^A-zæøå])"]
                   ["Big Data" #"(?i)([^A-zæøå](big data|stordata)[^A-zæøå])"]
                   ["Unity" #"(?i)[^A-zæøå]unity"]
                   ["GraphQL" #"(?i)graphql"]
                   ["ETL" #"(?i)([^A-zæøå](etl|extract.?transform.?load)[^A-zæøå])"]
                   ["SaaS" #"(?i)([^A-zæøå](saas|(software.?as.?a.?service))[^A-zæøå])"]
                   ["Cloud" #"(?i)([^A-zæøå](cloud|sky(en|basert)?)[^A-zæøå])"]
                   ["Frontend" #"(?i)front.?end"]
                   ["Backend" #"(?i)back.?end"]
                   ["Fullstack" #"(?i)full.?stack"]
                   ["Devops" #"(?i)[^A-zæøå](dev.?ops)"]
                   ["TensorFlow" #"(?i)[^A-zæøå](tensor.?flow)"]
                   ["BI" #"(?i)([^A-zæøå](BI|Business.?Intelligence)[^A-zæøå])"]
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
                   ["ILOG" #"(?i)([^A-zæøå]ILOG[^A-zæøå])"]
                   ["Fortran" #"(?i)([^A-zæøå]Fortran[^A-zæøå])"]
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
       (distinct-by :ad_id)
       (remove #(nil? (:ad_id %)))))

(defn read-descs [path]
  (->> (csv/parse-csv (slurp path) :delimiter \;)
       (csv-data->maps)
       (map rename-keys-texts)
       (distinct-by :ad_id)
       (remove #(nil? (:ad_id %)))
       (map #(update % :description clean-html))))


((defn save-descs-from-ids []
   (filter #(contains? (json/read-str (slurp "resources/ids")) (:ad_id %)) (read-descs "resources/texts/2018_2.csv")))


(defn save-not-saved-ads [path]
  (ads-in (let [b  (set (map :ad_id (sql/query db ["select ad_id from ads"])))
                c (set (map :proffesion_code (sql/query db ["select proffesion_code from ads"])))
                ]
            (->> (read-ads path)
                 (remove #(contains? b (:ad_id %)))
                 (filter #(contains? c (:proffesion_code %)))
                 (filter #(= "Ingeniør- og ikt-fag" (:proffesion_group %)))
                 )
            )))

(defn q-keywords []
  (->> (sql/query db ["select keyword, \nc as freq,\nj.year, \nround((c::numeric)/(c2::numeric),4)*100 as percent, \nround((c::numeric)/(c3::numeric),4)*100 as percent_all, \narray_agg(distinct ads.business_orgnr) as business_orgnumbers \nfrom (select keyword, year, count(keyword) as c \n\t  from keywords\n\t  inner join ads on keywords.ad_id = ads.ad_id\n\t  group by year, keyword order by c desc ) j\n\t  inner join (select count(ads.ad_id) c2, year \n\t\t\t\t  from ads \n\t\t\t\t  where ad_id in (select ad_id from keywords) \n\t\t\t\t  group by year) j2 on j2.year = j.year\n\t  inner join (select count(ads.ad_id) c3, year \n\t\t\t\t  from ads\n\t\t\t\t  group by year) j3 on j3.year = j.year\n\t  inner join ads on j.year = ads.year and j.keyword in (select keyword from keywords where keywords.ad_id = ads.ad_id) \ngroup by keyword, freq, j.year, percent, percent_all\norder by year asc"])
       (group-by :keyword)))

(defn q-firms []
  (sql/query db ["select a.orgnr as business_orgnr,
                 array_agg(distinct a.org_name) as business_names,
                 array_agg(distinct k.keyword) as keywords,
                 array_agg(distinct a.fylke) as fylker,
                 array_agg(distinct a.kommune) as kommuner,
                 count(a.ad_id) as ads_posted
                 from keywords k inner join ads a on a.ad_id = k.ad_id
                 where a.business_orgnr != ''
                 group by a.orgnr
                 order by orgnr "]))

(defn q-businesses []
  (sql/query db ["select a.business_orgnr as business_orgnr,
                 array_agg(distinct a.business_name) as business_names,
                 array_agg(distinct k.keyword) as keywords,
                 array_agg(distinct a.fylke) as fylker,
                 array_agg(distinct a.kommune) as kommuner,
                 count(a.ad_id) as ads_posted
                 from keywords k inner join ads a on a.ad_id = k.ad_id
                 where a.business_orgnr != ''
                 group by a.business_orgnr
                 order by business_orgnr"]))

(defn q-kommuner []
  (keywordize-keys (apply merge (map #(assoc '{} (:fylke %) (:kommuner %)) (sql/query db ["select fylke, array_agg(distinct kommune) as kommuner from ads\ngroup by fylke"])))))

(defn q-fylker []
  (:fylker (first (sql/query db ["select array_agg( distinct a.fylke) as fylker\n  from ads a where a.ad_id in (select ad_id from keywords)\n    and a.business_orgnr != ''\n\t\t\t\t and a.fylke not in ('*', 'ENT')"]))))


(defn q-keywords-plain []
  (map :keyword (sql/query db ["select distinct keyword from keywords"])))

(defroutes app
           (GET "/keywords/" []
             {:status  200
              :headers {"Content-Type" "application/json" "Access-Control-Allow-Origin" "*"}
              :body    (json/write-str (q-keywords))})
           (GET "/businesses/" []
             {:status  200
              :headers {"Content-Type" "application/json" "Access-Control-Allow-Origin" "*"}
              :body    (json/write-str (q-businesses))})
           (GET "/keywords-plain/" []
             {:status  200
              :headers {"Content-Type" "application/json" "Access-Control-Allow-Origin" "*"}
              :body    (json/write-str (q-keywords-plain))})
           (route/not-found "<h1>Page not found</h1>"))

(defn save-json [f m]
  (spit (str "resources/db/" f) (json/write-str m)))

(defn save-all []
  (do
    (save-json "fylker" (q-fylker))
    (save-json "kommuner" (q-kommuner))
    (save-json "businesses" (q-businesses))
    (save-json "keywordsplain" (q-keywords-plain))
    (save-json "keywords" (q-keywords))
    (save-json "firms" (q-firms))))


(defn create-server []
  (server/run-server (app) {:port 8080}))

