;; -*- coding: utf-8 -*-
;;
;; © 2009 Digg, Inc. All rights reserved.
;; Author: Ian Eure <ian@digg.com>
;;

(defproject twidoop "0.8.0"
  :description "Read from the firehose, write to Hadoop"
  :dependencies [[org.clojure/clojure "1.1.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.0-SNAPSHOT"]
                 [clojure-http-client "1.0.0-SNAPSHOT"]
                 [org.apache.mahout.hadoop/hadoop-core "0.20.1"]
                 [commons-logging "1.1.1"]
                 [commons-cli "1.2"]]
  :dev-dependencies [[org.clojure/swank-clojure "1.0"]]
  :main twidoop)
