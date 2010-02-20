;; -*- coding: utf-8 -*-
;;
;; © 2009, 2010 Digg, Inc. All rights reserved.
;; Author: Ian Eure <ian@digg.com>
;;

(ns twidoop
  (:require [clojure.http.client :as http])
  (:use [clojure.contrib.command-line :only (with-command-line)])
  (:import (org.apache.hadoop.fs FileSystem LocalFileSystem Path)
           (org.apache.hadoop.conf Configuration)
           (java.util Date)
           (java.text SimpleDateFormat))
  (:gen-class))

(def #^{:tag FileSystem
        :doc "The HDFS FileSystem instance to write to."}
     *filesystem* nil)

(def #^{:doc "Replica count for the output."}
     *replicas* 1)

(def #^{:doc "The block size of the output."}
     *block-size* (* 1024 1024 16))

(defn stream-url
  "Return the Twitter stream URL to read statuses from.

Type can be `sample' or `firehose'."
  ([user pass]
     (stream-url user pass "sample"))
  ([user pass type]
     (format "http://%s:%s@stream.twitter.com/1/statuses/%s.json"
             user pass type)))

(defn create-file
  "Create a new file on HDFS, and return a new FSDataOutputStream."
  ([name]
     (create-file name true 512 *replicas* (* 1024 1024 *block-size*)))

  ([name overwrite buffer replicas block-size]
     (.create *filesystem*
              (or (and (instance? Path name) name) (Path. name))
              overwrite buffer replicas block-size)))

(defn today []
  "Return today's date in YYYY-MM-DD format."
  (. (SimpleDateFormat. "yyyy-MM-dd") format (Date.)))

(defn get-output
  "Return a hash-map of the date and file we're writing to."
  [path]
  {:date (today)
   :path path
   :file (create-file (format "%s/statuses-%s.json" path (today)))})

(defn stale? [{date :date}]
  "Is the current output stale?"
  (not (= (today) date)))

(defmacro forever
  "Run forms forever, ignoring exceptions."
  [form]
  `(let [attempts# 0]
     (while true
            (try ~form
                 (catch Exception e#
                   (Thread/sleep (* 2000 (inc attempts#))))))))

(defn save-statuses [url out-path]
  "Save Twitter statuses into HDFS."
  (let [output (atom (get-output out-path))]
    (forever
     (doseq [status (:body-seq (http/request url))]
       (.writeBytes (:file @output) (str status "\0"))
       (print ".")
       (flush)

       (when (stale? @output)
         (print (format "\n%s -> " (:date @output)))
         (.close (:file @output))
         (reset! output (get-output (:path @output)))
         (println (:date @output)))))))

(defn -main [& args]
  (with-command-line args
    "twidoop -- Stream the Twitter firehose into Hadoop."
    [[output o "Output here on HDFS" "/firehose"]
     [hdfs "HDFS to connect to" "hdfs://localhost:9000"]
     [block-size b "HDFS block size (in megabytes)" 16]
     [replicas r "HDFS replica count" 1]
     [type t "Type of stream to read from: sample or firehose" "sample"]
     [user u "Twitter username"]
     [pass p "Twitter password"]]
    (binding [*filesystem* (FileSystem/get (doto (Configuration.)
                                             (.set "fs.default.name" hdfs)))
              *block-size* (Integer. block-size)
              *replicas* (Integer. replicas)]
      (save-statuses (stream-url user pass type) output))))
