;; -*- coding: utf-8 -*-
;;
;; © 2009 Digg, Inc. All rights reserved.
;; Author: Ian Eure <ian@digg.com>
;;

(ns digg.core.twidoop
  (:require [clojure.http.client :as http])
  (:import (org.apache.hadoop.fs FileSystem LocalFileSystem Path)
           (org.apache.hadoop.conf Configuration))
  (:gen-class))


(def user "user")
(def pass "pass")
(def url (format "http://%s:%s@stream.twitter.com/1/statuses/sample.json"
                 user pass))
(def hadoop-config
     (doto (Configuration.)
       (.addResource
        (Path. "/Volumes/Digg/core/hadoop/hadoop-0.20.1/conf/core-site.xml"))))


(defn -main []
  (let [output (.create (FileSystem/get hadoop-config)
                        (Path. "/user/ieure/spritzer.json") true)]
    (doseq [status (:body-seq (http/request url))]
      (do (.writeBytes output status)
          (print ".")
          (flush)))))
