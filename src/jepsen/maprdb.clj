(ns jepsen.maprdb
  (:use [clojure.set :only [union difference]]
        jepsen.util
        jepsen.set-app
        jepsen.load
        clojure.string)
  (:require [clojure.set            :as set])
  (:import (org.apache.hadoop.hbase HBaseConfiguration
                                    HColumnDescriptor
                                    HTableDescriptor
                                    ))
  (:import (org.apache.hadoop.hbase.client HBaseAdmin
                                           HConnection
                                           HConnectionManager
                                           Get
                                           Put
                                           Scan
                                           ))
  (:import (org.apache.hadoop.hbase.util Bytes)))

(def cf (Bytes/toBytes "cf"))

(def cf2 (Bytes/toBytes "cf2"))

(def cf-value (Bytes/toBytes "value"))

(def cf-a (Bytes/toBytes "a"))

(def cf-b (Bytes/toBytes "b"))

(defn create-configuration
  "Creates the configuration."
  [opts]
  (println "creating configuration")
  (let [hbase-config (HBaseConfiguration/create)]
    (.set hbase-config "hbase.zookeeper.quorum" "qa-node209.qa.lab")
    (.set hbase-config "hbase.rootdir" "maprfs:///hbase")
    (.set hbase-config "hbase.cluster.distributed" "true")
    (.set hbase-config "dfs.support.append" "true")
    (.set hbase-config "base.fsutil.maprfs.impl" "org.apache.hadoop.hbase.util.FSMapRUtils")
    (.set hbase-config "hbase.zookeeper.property.clientPort" "5181")
    (.set hbase-config "hbase.table.namespace.mappings" "*:/")
    hbase-config))

(defn hbase-app
  "Creates a new key/value pair for each element."
  [opts]
  (let [hbase-config (create-configuration opts)
        hbase-admin (new HBaseAdmin hbase-config)
        hbase-conn (HConnectionManager/createConnection hbase-config)]
    (reify SetApp
      (setup [app]
        (let [hbase-table (new HTableDescriptor (.getBytes "test"))
              hbase-col (new HColumnDescriptor "cf")]
          (.addFamily hbase-table hbase-col)
          (println "ready to create")
          (when (not (.tableExists hbase-admin (.getBytes "test")))
            (.createTable hbase-admin hbase-table))
          (println "created")
        )
        )

      (add [app element]
        (let [table (.getTable hbase-conn (.getBytes "test"))
              p (new Put (Bytes/toBytes element))]
          (.add p cf cf-value (Bytes/toBytes element))
          (.put table p)
          (.close table)))

      (results [app]
        (let [table (.getTable hbase-conn (.getBytes "test"))]
          (set (map #(Bytes/toInt (.getValue % cf cf-value))
                    (iterator-seq (.iterator (.getScanner table (new Scan))))))))

      (teardown [app]
        (when (.tableExists hbase-admin "test")
          (.disableTable hbase-admin "test")
          (.deleteTable hbase-admin "test")
        )
      ))))

(defn hbase-append-app
  "Adds each element to a list."
  [opts]
  (let [hbase-config (create-configuration opts)
        hbase-admin (new HBaseAdmin hbase-config)
        hbase-conn (HConnectionManager/createConnection hbase-config)]
    (reify SetApp
      (setup [app]
        (let [hbase-table (new HTableDescriptor "/test-append")
              hbase-col (new HColumnDescriptor "cf")]
          (.addFamily hbase-table hbase-col)
          (when (not (.tableExists hbase-admin "/test-append"))
            (.createTable hbase-admin hbase-table))
        ))

      (add [app element]
        (let [table (.getTable hbase-conn "/test-append")
              key (Bytes/toBytes "key")
              g (new Get key)
              old-value (.getValue (.get table g) cf cf-value)
              value (str (or (Bytes/toString old-value) "") element ",")
              p (new Put key)]
          (.add p cf cf-value (Bytes/toBytes value))
          (when (not (.checkAndPut table key cf cf-value old-value p))
            error)))

      (results [app]
        (let [table (.getTable hbase-conn "/test-append")
              key (Bytes/toBytes "key")
              g (new Get key)]
          (map #(new Long %)
               (split (or (Bytes/toString (.getValue (.get table g) cf cf-value)) "") #","))))

      (teardown [app]
        (when (.tableExists hbase-admin "/test-append")
          (.disableTable hbase-admin "/test-append")
          (.deleteTable hbase-admin "/test-append")
        )
      ))))

(defn hbase-counter-app
  "All writes are increments. Recovers [0...n] where n is the current value of
  the counter."
  [opts]
  (let [hbase-config (create-configuration opts)
        hbase-admin (new HBaseAdmin hbase-config)
        hbase-conn (HConnectionManager/createConnection hbase-config)]
    (reify SetApp
      (setup [app]
        (let [hbase-table (new HTableDescriptor "/test-counter")
              hbase-col (new HColumnDescriptor "cf")]
          (.addFamily hbase-table hbase-col)
          (when (not (.tableExists hbase-admin "/test-counter"))
            (.createTable hbase-admin hbase-table))
        ))

      (add [app element]
        (let [table (.getTable hbase-conn "/test-counter")
              key (Bytes/toBytes "key")]
          (.incrementColumnValue table key cf cf-value 1)
          ok))

      (results [app]
        (let [table (.getTable hbase-conn "/test-counter")
              key (Bytes/toBytes "key")
              g (new Get key)]
          (range (Bytes/toLong (.getValue (.get table g) cf cf-value)))))

      (teardown [app]
        (when (.tableExists hbase-admin "/test-counter")
          (.disableTable hbase-admin "/test-counter")
          (.deleteTable hbase-admin "/test-counter")
        )
      ))))

; Hack: use this to record the set of all written elements for isolation-app.
(def writes (atom #{}))

(defn hbase-isolation-app
  "This app tests whether or not it is possible to consistently update multiple
  cells in a row, such that either *both* writes are visible together, or
  *neither* is.

  Each client picks a random int identifier to distinguish itself from the
  other clients. It tries to write this identifier to cell A, and -identifier
  to cell B. The write is considered successful if A=-B. It is unsuccessful if
  A is *not* equal to -B; e.g. our updates were not isolated.

  'concurrency defines the number of writes made to each row. "
  [opts]
  (let [; Number of writes to each row
        concurrency  2
        client-id   (rand-int Integer/MAX_VALUE)
        hbase-config (create-configuration opts)
        hbase-admin (new HBaseAdmin hbase-config)
        hbase-conn (HConnectionManager/createConnection hbase-config)]
    (reify SetApp
      (setup [app]
        (let [hbase-table (new HTableDescriptor "/test-isolation")
              hbase-col (new HColumnDescriptor "cf")]
          (.addFamily hbase-table hbase-col)
          (when (not (.tableExists hbase-admin "/test-isolation"))
            (.createTable hbase-admin hbase-table))
        ))

      (add [app element]
        (let [table (.getTable hbase-conn "/test-isolation")]
          ; Introduce some entropy
          (sleep (rand 200))

          ; Record write in memory
          (swap! writes conj element)

          (dotimes [i concurrency]
            (let [e (- element i)]
              (when (<= 0 e)
                (let [p (new Put (Bytes/toBytes (Integer/valueOf e)))]
                  (.add p cf cf-a (Bytes/toBytes (Integer/valueOf client-id)))
                  (.add p cf cf-b (Bytes/toBytes (Integer/valueOf (- client-id))))
                  (.put table p)))))
          (.close table)))

      (results [app]
        (let [table (.getTable hbase-conn "/test-isolation")]
          (->> (set (map #(hash-map :id (Bytes/toInt (.getRow %)),
                                    :a (Bytes/toInt (.getValue % cf cf-a)),
                                    :b (Bytes/toInt (.getValue % cf cf-b)))
                    (iterator-seq (.iterator (.getScanner table (new Scan))))))
            (remove #(= (:a %) (- (:b %))))
            prn
            dorun)
          (->> (set (map #(hash-map :id (Bytes/toInt (.getRow %)),
                                    :a (Bytes/toInt (.getValue % cf cf-a)),
                                    :b (Bytes/toInt (.getValue % cf cf-b)))
                    (iterator-seq (.iterator (.getScanner table (new Scan))))))
            (remove #(= (:a %) (- (:b %))))
            (map :id)
            (set/difference @writes))
        ))

      (teardown [app]
        (when (.tableExists hbase-admin "/test-isolation")
          (.disableTable hbase-admin "/test-isolation")
          (.deleteTable hbase-admin "/test-isolation")
        )
      ))))

; Hack: use this to record the set of all written elements for isolation-app.
(def writes-multiple-cf (atom #{}))

(defn hbase-isolation-multiple-cf-app
  "This app tests whether or not it is possible to consistently update multiple
  cells in a row, such that either *both* writes are visible together, or
  *neither* is.

  Each client picks a random int identifier to distinguish itself from the
  other clients. It tries to write this identifier to cell A, and -identifier
  to cell B. The write is considered successful if A=-B. It is unsuccessful if
  A is *not* equal to -B; e.g. our updates were not isolated.

  'concurrency defines the number of writes made to each row. "
  [opts]
  (let [; Number of writes to each row
        concurrency  2
        client-id   (rand-int Integer/MAX_VALUE)
        hbase-config (create-configuration opts)
        hbase-admin (new HBaseAdmin hbase-config)
        hbase-conn (HConnectionManager/createConnection hbase-config)]
    (reify SetApp
      (setup [app]
        (let [hbase-table (new HTableDescriptor "/test-isolation-multiple-cf")
              hbase-col (new HColumnDescriptor "cf")
              hbase-col2 (new HColumnDescriptor "cf2")]
          (.addFamily hbase-table hbase-col)
          (.addFamily hbase-table hbase-col2)
          (when (not (.tableExists hbase-admin "/test-isolation-multiple-cf"))
            (.createTable hbase-admin hbase-table))
        ))

      (add [app element]
        (let [table (.getTable hbase-conn "/test-isolation-multiple-cf")]
          ; Introduce some entropy
          (sleep (rand 200))

          ; Record write in memory
          (swap! writes-multiple-cf conj element)

          (dotimes [i concurrency]
            (let [e (- element i)]
              (when (<= 0 e)
                (let [p (new Put (Bytes/toBytes (Integer/valueOf e)))]
                  (.add p cf cf-a (Bytes/toBytes (Integer/valueOf client-id)))
                  (.add p cf2 cf-b (Bytes/toBytes (Integer/valueOf (- client-id))))
                  (.put table p)))))
          (.close table)))

      (results [app]
        (let [table (.getTable hbase-conn "/test-isolation-multiple-cf")]
          (->> (set (map #(hash-map :id (Bytes/toInt (.getRow %)),
                                    :a (Bytes/toInt (.getValue % cf cf-a)),
                                    :b (Bytes/toInt (.getValue % cf2 cf-b)))
                    (iterator-seq (.iterator (.getScanner table (new Scan))))))
            (remove #(= (:a %) (- (:b %))))
            prn
            dorun)
          (->> (set (map #(hash-map :id (Bytes/toInt (.getRow %)),
                                    :a (Bytes/toInt (.getValue % cf cf-a)),
                                    :b (Bytes/toInt (.getValue % cf2 cf-b)))
                    (iterator-seq (.iterator (.getScanner table (new Scan))))))
            (remove #(= (:a %) (- (:b %))))
            (map :id)
            (set/difference @writes-multiple-cf))
        ))

      (teardown [app]
        (when (.tableExists hbase-admin "/test-isolation-multiple-cf")
          (.disableTable hbase-admin "/test-isolation-multiple-cf")
          (.deleteTable hbase-admin "/test-isolation-multiple-cf")
        )
      ))))
