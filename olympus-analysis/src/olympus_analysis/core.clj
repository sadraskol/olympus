(ns olympus-analysis.core
  (:use clojure.pprint)
  (:gen-class))

(require '[elle.rw-register :as rw])
(load-file "history.clj")

(defn -main
  "analyse history"
  [& _]
  (pprint (rw/check {:consistency-models [:linearizable]} history)))
