(ns jepsen.control.net
  "Network control functions."
  (:refer-clojure :exclude [partition])
  (:use jepsen.control)
  (:require [clojure.string :as str]))

(def hosts-map {:n1 "qa-node209.qa.lab"
              :n2 "qa-node210.qa.lab"
              :n3 "qa-node211.qa.lab"
              :n4 "qa-node186.qa.lab"
              ;:n5 "localhost"
              })

(def small-partition-set #{(:n1 hosts-map) (:n2 hosts-map)})
;(def large-partition-set #{(:n3 hosts-map) (:n4 hosts-map) (:n5 hosts-map)})
(def large-partition-set #{(:n3 hosts-map) (:n4 hosts-map)})

(defn slow
  "Slows down the network."
  []
  (exec :tc :qdisc :add :dev :eth0 :root :netem :delay :50ms :10ms :distribution :normal))

(defn flaky
  "Drops packets."
  []
  (exec :tc :qdisc :add :dev :eth0 :root :netem :loss "20%" "75%"))

(defn fast
  "Fixes network."
  []
  (exec :tc :qdisc :del :dev :eth0 :root))

(defn reachable?
  "Can the current node ping the given node?"
  [node]
  (try (exec :ping :-w 1 node) true
       (catch RuntimeException _ false)))

(defn ip
  "Look up an ip for a hostname"
  [host]
  ; getent output is of the form:
  ; 74.125.239.39   STREAM host.com
  ; 74.125.239.39   DGRAM
  ; ...
  (first (str/split (->> (exec :getent :ahosts host)
                         (str/split-lines)
                         (first))
                    #"\s+")))

(defn cut-random-link
  "Cuts a random link to any of nodes."
  [nodes]
  (su (exec :iptables :-A :INPUT :-s (ip (rand-nth nodes)) :-j :DROP)))


(defn partition
  "Partitions the network."
  []
  (su
    (let [nodes (map ip large-partition-set)]
      (when (small-partition-set *host*)
        (doseq [n nodes]
          (exec :iptables :-A :INPUT :-s n :-j :DROP))))))

(defn iptables-list
  []
  (su (exec :iptables :--list)))

(defn heal
  "Heals a partition."
  []
  (su
    (exec :iptables :-F)
    (exec :iptables :-X)))
