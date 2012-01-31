;;; Command-line invocation.

(defpackage #:ldump.main
  (:use #:cl #:iterate #:ldump #:ldump.pool #:ldump.file-pool #:ldump.nodes
	#:command-line-arguments #:hashlib)
  (:export #:main #:main-list #:main-tree-size))
(in-package #:ldump.main)

(defun main-list (pool-path)
  (with-pool (pool 'file-pool :dir (cl-fad:pathname-as-directory pool-path))
    (list-backups)))

(defun main-tree-size (pool-path hash)
  (with-pool (pool 'file-pool :dir (cl-fad:pathname-as-directory pool-path))
    (tree-size (unhexify hash))))

(defun main ()
  (let ((args (get-command-line-arguments)))
    (format t "Args: ~S~%" args)
    (cond ((and (= 2 (length args))
		(string= (car args) "list"))
	   (main-list (second args)))
	  ((and (= 3 (length args))
		(string= (car args) "du"))
	   (main-tree-size (second args) (third args)))
	  (t (format t "Usage: ldump command~%")))))
