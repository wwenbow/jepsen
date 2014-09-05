(ns jepsen.hbase
  (:use [clojure.set :only [union difference]]
        jepsen.util
        jepsen.set-app
        jepsen.load
        clojure.string)
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

(def cf-value (Bytes/toBytes "value"))

(defn create-configuration
  "Creates the configuration."
  [opts]
  (let [hbase-config (HBaseConfiguration/create)]
    ; zookeeper is running on n5
    (.set hbase-config "hbase.zookeeper.quorum" "n5")
    hbase-config))

(defn hbase-app
  "Creates a new key/value pair for each element."
  [opts]
  (let [hbase-config (create-configuration opts)
        hbase-admin (new HBaseAdmin hbase-config)
        hbase-conn (HConnectionManager/createConnection hbase-config)]
    (reify SetApp
      (setup [app]
        (let [hbase-table (new HTableDescriptor "test")
              hbase-col (new HColumnDescriptor "cf")]
          (.addFamily hbase-table hbase-col)
          (when (not (.tableExists hbase-admin "test"))
            (.createTable hbase-admin hbase-table))
        ))
        
      (add [app element]
        (let [table (.getTable hbase-conn "test")
              p (new Put (Bytes/toBytes element))]
          (.add p cf cf-value (Bytes/toBytes element))
          (.put table p)
          (.close table)))

      (results [app]
        (let [table (.getTable hbase-conn "test")]
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
        (let [hbase-table (new HTableDescriptor "test")
              hbase-col (new HColumnDescriptor "cf")]
          (.addFamily hbase-table hbase-col)
          (when (not (.tableExists hbase-admin "test"))
            (.createTable hbase-admin hbase-table))
        ))
        
      (add [app element]
        (let [table (.getTable hbase-conn "test")
              key (Bytes/toBytes "key")
              g (new Get key)
              old-value (.getValue (.get table g) cf cf-value)
              value (str (or (Bytes/toString old-value) "") element ",")
              p (new Put key)]
          (.add p cf cf-value (Bytes/toBytes value))
          (when (not (.checkAndPut table key cf cf-value old-value p))
            error)))

      (results [app]
        (let [table (.getTable hbase-conn "test")
              key (Bytes/toBytes "key")
              g (new Get key)]
          (map #(new Long %) 
               (split (or (Bytes/toString (.getValue (.get table g) cf cf-value)) "") #","))))

      (teardown [app]
        (when (.tableExists hbase-admin "test")
          (.disableTable hbase-admin "test")
          (.deleteTable hbase-admin "test")
        )
      ))))
