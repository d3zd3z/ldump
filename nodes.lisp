;;; Backup nodes.

(defpackage #:ldump.nodes
  (:use #:cl #:iterate #:ldump #:ldump.chunk #:ldump.file-pool #:ldump.pack
	#:hashlib #:local-time
	#:alexandria #:split-sequence)
  (:import-from #:babel #:octets-to-string)
  (:export #:list-backups))
(in-package #:ldump.nodes)

(defgeneric decode-kind (type data)
  (:documentation
   "Given a particular kind, and it's payload, decode it into a lisp
type."))

(defun decode-chunk (chunk)
  (decode-kind (chunk-type chunk) (chunk-data chunk)))

(defun get-chunk (hash)
  (decode-chunk (pool-get-chunk hash)))

(defun keywordify (str)
  "Make a keyword out of STR, in a case-insensitive way"
  (intern (string-upcase str) "KEYWORD"))

(defun unkeywordify (key)
  (string-downcase (symbol-name key)))

(defun unkeywordify-plist (plist)
  (iter (for (key . rest) on plist by #'cddr)
	(collect (unkeywordify key))
	(collect (car rest))))

(defun unkeywordify-alist (alist)
  (iter (for (key . value) in alist)
	(collect (cons (unkeywordify key) value))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Decoding XML into a property list.

(defun get-sxml-key (atts)
  (iter (for (key val) in atts)
	(when (string= key "key")
	  (return-from get-sxml-key (keywordify val))))
  (error "\"key\" not present"))

(defun sxml-atts-to-plist (atts)
  (iter (for (key val) in atts)
	(collect (keywordify key))
	(collect val)))

(defun decode-sxml-plist (sxml)
  "Decode a property list in XML format.  The CAR of the result is the
XML name of the overall node.  The CDR is then the properties in plist
format.  Any attributes of the root node will be prepended as
attributes."
  (destructuring-bind (pname pattr &rest children) sxml
    (iter (for child in children)
	  (destructuring-bind (name atts value) child
	    (when (string= name "entry")
	      (collect (get-sxml-key atts) into entries)
	      (collect value into entries)))
	  (finally (return (append (list (keywordify pname))
				   (sxml-atts-to-plist pattr)
				   entries))))))

(defun octets-to-plist (data)
  "Decode a property list storted as octets of XML.  The CAR of the
  result is the type of the outside node in the XML, and the CDR is
the property list of the values in the XML property list."
  (decode-sxml-plist (xmls:parse (octets-to-string data))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Backup entries.

(defstruct (backup (:constructor %make-backup))
  date hash source-hash attributes)

(defun string-to-timestamp (str)
  (multiple-value-bind (seconds milis)
      (floor (parse-integer str) 1000)
    (unix-to-timestamp seconds :nsec (* milis 1000000))))
(define-modify-macro string-to-timestampf () string-to-timestamp)

;;; Attempt to decode a "floating" timestamp represented as an integer
;;; and fraction part.
(defun real-string-to-timestamp (str)
  (let* ((parts (split-sequence #\. str))
	 (seconds (parse-integer (car parts)))
	 (nanos (if-let (fraction (cadr parts))
		  (* (parse-integer fraction)
		     (expt 10 (- 9 (length fraction)))))))
    (unix-to-timestamp seconds :nsec nanos)))

(defun plist-backup (props &optional source-hash)
  (destructuring-bind (type &rest rest &key _date hash &allow-other-keys)
      props
    (unless (eq type :properties)
      (error "Invalid backup node type: ~S" type))
    (unless _date (error "Backup node contains no date"))
    (unless hash (error "Backup node contains no hash"))
    (%make-backup :date (string-to-timestamp _date)
		  :hash (unhexify hash)
		  :source-hash source-hash
		  :attributes (plist-alist
			       (remove-from-plist rest :hash :_date :source-hash)))))

(defmethod decode-kind ((type (eql :|back|)) data)
  (let ((props (decode-sxml-plist (xmls:parse (octets-to-string data)))))
    (plist-backup props)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; Backup nodes.

(defclass node ()
  ((kind :initarg :kind :allocation :class
	 :initform "invalid")))

(defclass dir-node (node)
  ((children :initarg :children :type (byte-vector 20))
   (ctime :initarg :ctime :type timestamp)
   (mtime :initarg :mtime :type timestamp)
   (dev :initarg :dev :type integer)
   (gid :initarg :gid :type integer)
   (uid :initarg :uid :type integer)
   (ino :initarg :ino :type integer)
   (mode :initarg :mode :type integer)
   (nlink :initarg :nlink :type integer)
   (size :initarg :size :type integer)
   (kind :initform "DIR")))

(defparameter *node-decoders*
  (plist-hash-table
   (list
    :children #'unhexify
    :ctime #'real-string-to-timestamp
    :mtime #'real-string-to-timestamp
    :dev #'parse-integer
    :gid #'parse-integer
    :uid #'parse-integer
    :ino #'parse-integer
    :mode #'parse-integer
    :nlink #'parse-integer
    :size #'parse-integer)))

(defun decode-node-args (initargs)
  (iter (for (key . value) on initargs by #'cddr)
	(setf value (car value))
	(for conv = (gethash key *node-decoders*))
	(when conv
	  (setf value (funcall conv value)))
	(collect key)
	(collect value)))

(defmethod shared-initialize :around ((instance node) slots
				      &rest initargs &key &allow-other-keys)
  (apply #'call-next-method instance slots (decode-node-args initargs)))

(defparameter *node-kinds*
  (plist-hash-table
   '("DIR" dir-node)
   :test 'equal))

(defmethod decode-kind ((type (eql :|node|)) data)
  (destructuring-bind (xml-type &rest plist &key kind &allow-other-keys)
      (octets-to-plist data)
    (unless (eq xml-type :node)
      (error "Invalid XML type for node"))
    (if-let ((node-kind (gethash kind *node-kinds*)))
      (apply #'make-instance node-kind plist)
      (error "Unsupported node kind: ~S" kind))))

;; TEST
(defun get-root ()
  (let* ((back (get-chunk (first (pool-backup-list)))))
    (get-chunk (backup-hash back))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; Directories.

(defclass dir-leaf ()
  ((entries :initarg :entries)))

;;; The decoder for directory leaf nodes.
(defmethod decode-kind ((type (eql :|dir |)) data)
  (let ((entries (iter (with pos = 0)
		       (with limit = (length data))
		       (while (< pos limit))
		       (for name-length = (unpack-be-integer data pos 2))
		       (incf pos 2)
		       (for name = (octets-to-string (subseq data pos (+ pos name-length))
						     :encoding :iso-8859-1))
		       (incf pos name-length)
		       (for hash = (subseq data pos (+ pos 20)))
		       (incf pos 20)
		       (collect (cons name hash)))))
    (make-instance 'dir-leaf :entries entries)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun format-attributes (atts)
  "Format the attributes (an alist with CARs as keywords) into a nice
string"
  (let* ((atts (unkeywordify-alist atts))
	 (atts (sort atts #'string< :key #'car))
	 (atts (alist-plist atts)))
    (format nil "~{~A=~A~^ ~}" atts)))

(defun show-backup (back)
  (format t "~A ~A ~A~%"
	  (hexify (backup-source-hash back))
	  (format-timestring nil (backup-date back)
			     :format '(:year #\- (:month 2) #\- (:day 2) #\_
				       (:hour 2) #\: (:min 2)))
	  (format-attributes (backup-attributes back))))

(defun list-backups ()
  (let* ((backs (mapcar (lambda (hash)
			  (let ((back (decode-chunk (pool-get-chunk hash))))
			    (setf (backup-source-hash back) hash)
			    back))
			(pool-backup-list)))
	 (backs (sort backs #'timestamp< :key #'backup-date)))
    (mapc #'show-backup backs)
    (values)))
