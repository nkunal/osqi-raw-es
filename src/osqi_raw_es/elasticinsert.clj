(ns osqi-raw-es.elasticinsert
  (:require [clojure.tools.logging :as log]
            [clj-time.core :as time]
            [clj-time.format :as tf]
            [clj-time.coerce :as tcoerce]
            [clojure.string :refer [lower-case]]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.bulk :as esb])
  (:gen-class))

(defn get-index-name
  [data-item unix-time]
  (try
    (let [prefix "agentlog"
          isostr (tf/unparse (tf/with-zone (tf/formatter "YYYY-MM-dd")
                               (time/time-zone-for-id "UTC"))
                             (tcoerce/from-long (* (Long. unix-time) 1000)))]
      (str prefix "-" isostr))
    (catch Exception e
      (do
        (log/error "Caught exception for unix-time= " unix-time ", data-item=" data-item)
        (throw e)))))

(defn create-fulldoc
  [data-block]
  (try
    (into []
          (for [data-item data-block]
            (let [name (data-item "name")
                  host-id (data-item "hostIdentifier")
                  unix-time (data-item "unixTime")
                  action (data-item "action")
                  columns (data-item "columns")]
              (assoc columns
                     "hostIdentifier" host-id
                     "unixTime" unix-time
                     "action" action
                     :_id (str host-id "-" unix-time "-" action)
                     :_type name
                     :_index (get-index-name data-item unix-time)))))
    (catch Exception e
      (do
        (log/error "create-fulldoc: caught exception for event " data-block)
        (throw e)))))

(defn create-es-docs
  [json-docs]
  (let [es-docs (into [] (map (fn [item]
                                (create-fulldoc (item "data"))))
                     @json-docs)]
    (flatten (remove nil? es-docs))))

(defn log-errors
  [bulk-resp]
  (when (get bulk-resp :errors)
    (let [items (bulk-resp :items)]
      (doseq [entry items]
        (when (> ((entry :index) :status) 299)
          (log/error "indexing error item =" entry))))))

(defn insert-kafka-batch
  [es-url json-docs]
  (let [conn (esr/connect es-url)
        all-es-docs (create-es-docs json-docs)
        l112 (prn "kunal all-es-docs=" all-es-docs)
        bulk-ops  (esb/bulk-index all-es-docs)
        bulk-resp (esb/bulk conn bulk-ops {:refresh true})]
    (when (get bulk-resp :errors)
      (log-errors bulk-resp))))

(defn handle-kafka-batch
  " This function can be called from torna or s3toelastic"
  ([props json-docs]
   (insert-kafka-batch (get props :es.url) json-docs )))

(defn init
  [props])
