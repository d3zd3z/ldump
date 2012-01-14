;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Package declaration.

(defpackage :db-zlib
  (:use :common-lisp :cffi)
  (:export compress-bound compress uncompress))
