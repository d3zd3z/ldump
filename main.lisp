;;; Command-line invocation.

(defpackage #:ldump.main
  (:use #:cl #:iterate #:ldump #:ldump.file-pool #:ldump.nodes
	#:command-line-arguments)
  (:export #:main #:main-list))
(in-package #:ldump.main)

(defun main-list (pool-path)
  (with-pool (pool (cl-fad:pathname-as-directory pool-path))
    (list-backups pool)))

(defun main ()
  (let ((args (get-command-line-arguments)))
    (format t "Args: ~S~%" args)
    (cond ((and (= 2 (length args))
		(string= (car args) "list"))
	   (main-list (second args)))
	  (t (format t "Usage: ldump command~%")))))
