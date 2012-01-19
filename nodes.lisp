;;; Backup nodes.

(defpackage #:ldump.nodes
  (:use #:cl #:iterate #:ldump #:ldump.chunk #:ldump.file-pool #:hashlib #:local-time
	#:alexandria)
  (:import-from #:babel #:octets-to-string)
  (:export #:list-backups))
(in-package #:ldump.nodes)

(defgeneric decode-kind (type data)
  (:documentation
   "Given a particular kind, and it's payload, decode it into a lisp
type."))

(defun decode-chunk (chunk)
  (decode-kind (chunk-type chunk) (chunk-data chunk)))

(defun keywordify (str)
  "Make a keyword out of STR, in a case-insensitive way"
  (intern (string-upcase str) "KEYWORD"))

(defun unkeywordify (key)
  (string-downcase (symbol-name key)))

(defun unkeywordify-plist (plist)
  (iter (for (key . rest) on plist by #'cddr)
	(collect (unkeywordify key))
	(collect (car rest))))

(defun get-sxml-key (atts)
  (iter (for (key val) in atts)
	(when (string= key "key")
	  (return-from get-sxml-key (keywordify val))))
  (error "\"key\" not present"))

(defun decode-sxml-plist (sxml)
  (destructuring-bind (pname pattr &rest children) sxml
    (declare (ignorable pattr))
    (iter (for child in children)
	  (destructuring-bind (name atts value) child
	    (when (string= name "entry")
	      (collect (get-sxml-key atts) into entries)
	      (collect value into entries)))
	  (finally (return (list* (keywordify pname) entries))))))

(define-modify-macro unhexifyf () unhexify)

(defun string-to-timestamp (str)
  (multiple-value-bind (seconds milis)
      (floor (parse-integer str) 1000)
    (unix-to-timestamp seconds :nsec (* milis 1000000))))
(define-modify-macro string-to-timestampf () string-to-timestamp)

(defmethod decode-kind ((type (eql :|back|)) data)
  (let ((props (decode-sxml-plist (xmls:parse (octets-to-string data)))))
    (unhexifyf (getf (cdr props) :hash))
    (string-to-timestampf (getf (cdr props) :_date))
    props))

(defun backup-date-func (&key _date &allow-other-keys)
  _date)
(define-apply-macro backup-date backup-date-func)

(defun format-attributes (plist)
  (let* ((plist (remove-from-plist plist :hash :_date :source-hash))
	 (plist (unkeywordify-plist plist))
	 (plist (plist-alist plist))
	 (plist (sort plist #'string< :key #'car))
	 (plist (alist-plist plist)))
    (format nil "~{~A=~A~^ ~}" plist)))

(defun show-backup-func (&rest rest &key source-hash _date &allow-other-keys)
  (format t "~A ~A ~A~%"
	  (hexify source-hash)
	  (format-timestring nil _date
			     :format '(:year #\- (:month 2) #\- (:day 2) #\_
				       (:hour 2) #\: (:min 2)))
	  (format-attributes rest)))
(declaim (inline show-backup))
(defun show-backup (args)
  (apply #'show-backup-func args))

(defun list-backups ()
  (let* ((backs (mapcar (lambda (hash)
			  (list* :source-hash hash
				 (cdr (decode-chunk (pool-get-chunk hash)))))
		       (pool-backup-list)))
	 (backs (sort backs #'timestamp< :key #'backup-date)))
    (mapc #'show-backup backs)
    (values)))
