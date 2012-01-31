;;; Storage pools.

(defpackage #:ldump.pool
  (:use #:cl #:ldump #:alexandria)
  (:export #:pool #:*current-pool* #:open-pool
	   #:close-pool #:%close-pool
	   #:pool-get-chunk #:%pool-get-chunk
	   #:pool-get-type #:%pool-get-type
	   #:pool-backup-list #:%pool-backup-list
	   #:write-pool-chunk #:%write-pool-chunk
	   #:pool-flush #:%pool-flush
	   #:with-pool))
(in-package #:ldump.pool)

(defclass pool () ())

(defvar *current-pool*)

(defgeneric open-pool (type &key &allow-other-keys))

(defmacro define-poolfun (name pool &rest others)
  "Define a generic access %NAME that takes a POOL argument as the
first argument, with others after it.  Also defines an accessor
function NAME that calls this generic function with the POOL
argument moved to the end with &key."
  (let ((generic-name (symbolicate "%" name)))
    `(progn
       (defgeneric ,generic-name (,pool ,@others))
       (declaim (inline ,name))
       (defun ,name (,@others &key (,pool *current-pool*))
	 (,generic-name ,pool ,@others)))))

(define-poolfun close-pool pool)
(define-poolfun pool-get-chunk pool hash)
(define-poolfun pool-get-type pool hash)
(define-poolfun pool-backup-list pool)
(define-poolfun write-pool-chunk pool chunk)
(define-poolfun pool-flush pool)

(defmacro with-pool ((var type &rest args) &body body)
  `(let* (*current-pool*
	  (,var (open-pool ,type ,@args)))
     (unwind-protect (progn ,@body)
       (close-pool :pool ,var))))
