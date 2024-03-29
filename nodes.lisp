;;; Backup nodes.

(defpackage #:ldump.nodes
  (:use #:cl #:iterate #:ldump #:ldump.chunk #:ldump.pool #:ldump.pack
	#:hashlib #:local-time
	#:alexandria #:split-sequence)
  (:import-from #:babel #:octets-to-string #:string-to-octets)
  (:export #:list-backups #:tree-size

	   #:write-node

	   #:node-name
	   #:blob
	   #:file-node #:dir-node #:link-node #:chr-node
	   #:blk-node #:fifo-node #:sock-node

	   #:indirect-builder #:indirect-append #:indirect-finish))
(in-package #:ldump.nodes)

(defgeneric decode-kind (type data)
  (:documentation
   "Given a particular kind, and it's payload, decode it into a lisp
type."))

(defun decode-chunk (chunk)
  (decode-kind (chunk-type chunk) (chunk-data chunk)))

(defun get-chunk (hash)
  (let* ((chunk (pool-get-chunk hash))
	 (node (decode-chunk chunk)))
    (setf (slot-value node 'source-hash) hash)
    (setf (slot-value node 'chunk) chunk)
    node))

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

(defun maybe-unhexify (item)
  (etypecase item
    (string (unhexify item))
    ((byte-vector 20) item)))

(defgeneric encode-node (node)
  (:documentation "Convert this node into it's payload.  Should return
the node type and the data."))

(defun write-node (node)
  "Write this node out, and return the hash identifying it.  Will also
fill in the source-hash and chunk slots of the node."
  (multiple-value-bind (type data)
      (encode-node node)
    (let* ((chunk (make-byte-vector-chunk type data))
	   (hash (chunk-hash chunk)))
      ;; TODO: Should the pool be doing the deduping, in case the pool is remote?
      (unless (pool-get-type hash)
	(write-pool-chunk chunk))
      (setf (slot-value node 'source-hash) hash
	    (slot-value node 'chunk) chunk)
      hash)))

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

(defclass node ()
  ((source-hash :initarg :source-hash :type (byte-vector 20)
		:documentation "If bound, indicates this object has been stored and is addressed by the given hash.")
   (chunk :initarg :chunk :type chunk))
  (:documentation "The parent of all nodes that are stored in the pool."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Backup entries.

(defclass backup (node)
  ((date :initarg :date :type timestamp :reader backup-date)
   (hash :initarg :hash :type (byte-vector 20))
   (attributes :initarg :attributes :type list)))

(defun string-to-timestamp (str)
  (multiple-value-bind (seconds milis)
      (floor (parse-integer str) 1000)
    (unix-to-timestamp seconds :nsec (* milis 1000000))))
(define-modify-macro string-to-timestampf () string-to-timestamp)

(defun to-java-date (timestamp)
  (princ-to-string (+ (* (timestamp-to-unix timestamp) 1000)
		      (floor (nsec-of timestamp) 1000000))))

;;; Attempt to decode a "floating" timestamp represented as an integer
;;; and fraction part.
(defun real-string-to-timestamp (str)
  (etypecase str
    (timestamp str)
    (string (let* ((parts (split-sequence #\. str))
		   (seconds (parse-integer (car parts)))
		   (nanos (if-let (fraction (cadr parts))
			    (* (parse-integer fraction)
			       (expt 10 (- 9 (length fraction)))))))
	      (unix-to-timestamp seconds :nsec nanos)))
    (integer (unix-to-timestamp str))))

;;; Make this into an integer.
(defun as-integer (item)
  (etypecase item
    (string (parse-integer item))
    (integer item)))

;;; TODO: These could be decoded the same as regular nodes.
(defun plist-backup (props &optional source-hash)
  (destructuring-bind (type &rest rest &key _date hash &allow-other-keys)
      props
    (unless (eq type :properties)
      (error "Invalid backup node type: ~S" type))
    (unless _date (error "Backup node contains no date"))
    (unless hash (error "Backup node contains no hash"))
    (make-instance 'backup
		   :date (string-to-timestamp _date)
		   :hash (unhexify hash)
		   :source-hash source-hash
		   :attributes (plist-alist
				(remove-from-plist rest :hash :_date :source-hash)))))

(defmethod decode-kind ((type (eql :|back|)) data)
  (let ((props (decode-sxml-plist (xmls:parse (octets-to-string data)))))
    (plist-backup props)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; Backup nodes.

(defclass filesystem-node (node)
  ((kind :initarg :kind :allocation :class
	 :initform "invalid")))

(defclass file-or-dir-node (filesystem-node)
  ((ctime :initarg :ctime :type timestamp)
   (mtime :initarg :mtime :type timestamp)
   (dev :initarg :dev :type integer)
   (gid :initarg :gid :type integer)
   (uid :initarg :uid :type integer)
   (ino :initarg :ino :type integer)
   (mode :initarg :mode :type integer)
   (nlink :initarg :nlink :type integer)
   (size :initarg :size :type integer)))

(defclass dir-node (file-or-dir-node)
  ((children :initarg :children :type (byte-vector 20))
   (kind :initform "DIR")))

(defclass file-node (file-or-dir-node)
  ((data :initarg :data :type (byte-vector 20))
   (kind :initform "REG")))

(defclass link-node (file-or-dir-node)
  ((target :initarg :target :type string)
   (kind :initform "LNK")))

(defclass chr-node (file-or-dir-node)
  ((rdev :initarg rdev :type integer)
   (kind :initform "CHR")))

(defclass blk-node (file-or-dir-node)
  ((rdev :initarg rdev :type integer)
   (kind :initform "BLK")))

(defclass sock-node (file-or-dir-node)
  ((kind :initform "SOCK")))

(defclass fifo-node (file-or-dir-node)
  ((kind :initform "FIFO")))

(defparameter *node-decoders*
  (plist-hash-table
   (list
    :children #'maybe-unhexify
    :data #'maybe-unhexify
    :ctime #'real-string-to-timestamp
    :mtime #'real-string-to-timestamp
    :dev #'as-integer
    :gid #'as-integer
    :uid #'as-integer
    :ino #'as-integer
    :mode #'as-integer
    :nlink #'as-integer
    :size #'as-integer)))

(defgeneric encode-slot (slot))

(defmethod encode-slot ((slot vector))
  (hexify slot))

(defmethod encode-slot ((slot timestamp))
  (let ((seconds (timestamp-to-unix slot))
	(nsec (nsec-of slot)))
    (format nil "~D.~9,'0D" seconds nsec)))

(defmethod encode-slot ((slot integer))
  (princ-to-string slot))

(defun decode-node-args (initargs)
  (iter (for (key . value) on initargs by #'cddr)
	(setf value (car value))
	(for conv = (gethash key *node-decoders*))
	(when conv
	  (setf value (funcall conv value)))
	(collect key)
	(collect value)))

(defmethod shared-initialize :around ((instance filesystem-node) slots
				      &rest initargs &key &allow-other-keys)
  (apply #'call-next-method instance slots (decode-node-args initargs)))

(defparameter *node-kinds*
  (plist-hash-table
   '("DIR" dir-node
     "REG" file-node
     "LNK" link-node
     "CHR" chr-node
     "BLK" blk-node
     "FIFO" fifo-node)
   :test 'equal))

(defmethod decode-kind ((type (eql :|node|)) data)
  (destructuring-bind (xml-type &rest plist &key kind &allow-other-keys)
      (octets-to-plist data)
    (unless (eq xml-type :node)
      (error "Invalid XML type for node"))
    (if-let ((node-kind (gethash kind *node-kinds*)))
      (apply #'make-instance node-kind plist)

      (throw 'debug plist)
      #+(or)
      (error "Unsupported node kind: ~S" kind))))

;; TEST
(defun get-root ()
  (let* ((back (get-chunk (first (pool-backup-list)))))
    (get-chunk (slot-value back 'hash))))

;;; Filesystem nodes are all converted into particular XML data.
(defun get-interesting-slots (node)
  "Get the \"interesting\" slots out of the node.  Removes the slots
that don't contain actual data we want to backup."
  (let ((slots (mopu:slot-names node)))
    (remove-if (lambda (item)
		 (member item '(source-hash chunk kind)))
	       slots)))

(defparameter *xml-header*
  (string-to-octets "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
"))

(defparameter *java-properties-dtd*
  (string-to-octets "<!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\">
"))

(defmethod encode-node ((node filesystem-node))
  (let* ((slot-names (get-interesting-slots node))
	 (slot-names (sort slot-names #'string< :key #'symbol-name))
	 (sxml `("node" (("kind" ,(slot-value node 'kind)))
			,@(iter (for slot-name in slot-names)
				(for name = (unkeywordify slot-name))
				(for value = (encode-slot (slot-value node slot-name)))
				(collect `("entry" (("key" ,name))
						   ,value)))))
	 (xml (xmls:toxml sxml)))
    (concatenate 'byte-vector *xml-header* (string-to-octets xml))))

(defmethod encode-node ((node backup))
  (with-slots (date hash attributes) node
    ;; Note that the date in the backup node is java style (in ms unix time).
    (let* ((alist `(("entry" (("key" "_date")) ,(to-java-date date))
		   ("entry" (("key" "hash")) ,(encode-slot hash))
		   ,@(iter (for (key . value) in attributes)
			   (for name = (unkeywordify key))
			   (collect `("entry" (("key" ,name))
					      ,value)))))
	   (sxml `("properties" () ("comment" () "Backup") ,@alist))
	   (xml (xmls:toxml sxml)))
      (concatenate 'byte-vector
		   *xml-header*
		   *java-properties-dtd*
		   (string-to-octets xml)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; Directories.

(defclass dir-leaf (node)
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

(defgeneric lookup-dir-entry (node name))

(defmethod lookup-dir-entry ((node dir-leaf) name)
  (iter (for (entry-name . hash) in (slot-value node 'entries))
	(when (string= entry-name name)
	  (return-from lookup-dir-entry hash)))
  nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; File data.

(defclass blob (node)
  ((payload :initarg :payload :type byte-vector)))

(defmethod decode-kind ((type (eql :|null|)) data)
  (make-instance 'blob :payload data))

(defmethod decode-kind ((type (eql :|blob|)) data)
  (make-instance 'blob :payload data))

(defmethod encode-node ((node blob))
  (with-slots (payload) node
    (values (if (length= 0 payload) :|null| :|blob|)
	    payload)))

(defclass indirect-data (node)
  ((level :initarg :level :type integer)
   (children :initarg :children :type (vector (byte-vector 20)))))

(defclass indirect-dir (node)
  ((level :initarg :level :type integer)
   (children :initarg :children :type (vector (byte-vector 20)))))

(defun decode-indirect (kind level data)
  (let ((hashes (iter (with pos = 0)
		      (with limit = (length data))
		      (while (< pos limit))
		      (collect (subseq data pos (+ pos 20)))
		      (incf pos 20))))
    (make-instance kind
		   :level level
		   :children (coerce hashes '(vector (byte-vector 20))))))

;;; Three levels of indirection supports a 524PB file or directory.
(defmethod decode-kind ((type (eql :|ind0|)) data)
  (decode-indirect 'indirect-data 0 data))

(defmethod decode-kind ((type (eql :|ind1|)) data)
  (decode-indirect 'indirect-data 0 data))

(defmethod decode-kind ((type (eql :|ind2|)) data)
  (decode-indirect 'indirect-data 0 data))

(defmethod decode-kind ((type (eql :|dir0|)) data)
  (decode-indirect 'indirect-dir 0 data))

(defmethod decode-kind ((type (eql :|dir1|)) data)
  (decode-indirect 'indirect-dir 0 data))

(defmethod decode-kind ((type (eql :|dir2|)) data)
  (decode-indirect 'indirect-dir 0 data))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Indirect builders.
;;; To use these, construct a builder with:
;;;   (make-instance 'indirect-builder :prefix "ind")
;;;
;;; Then, call (indirect-append builder hash) for each hash
;;; representing a leaf node that has been added.
;;;
;;; Finally, call (indirect-finish builder) to flush everything out,
;;; and return the final hash representing the sequence.

(defparameter *indirect-limit* (floor (* 256 1024) 20))

(defclass indirect-builder ()
  ((prefix :initarg :prefix :type (string 3))
   (buffers :initform (make-array 1 :adjustable t :fill-pointer 1
				  :initial-element (make-array 0 :adjustable t :fill-pointer 0
							       :element-type '(byte-vector 20))))))

(defun indirect-push (builder hash)
  (with-slots (buffers) builder
    (let ((buf (make-array 0 :adjustable t :fill-pointer 0
			   :element-type '(byte-vector 20))))
      (vector-push-extend buf buffers)
      (vector-push-extend hash buf))))

(defun indirect-name (builder level)
  (with-slots (prefix) builder
    (intern (format nil "~A~D" prefix level)
	    "KEYWORD")))

(defun indirect-summarize (builder buffer level)
  "Summarize BUFFER at LEVEL, returning the hash of the summary."
  (with-slots (buffers) builder
    (cond ((length= buffer 0)
	   (unless (zerop level)
	     (error "Internal error: Empty hash buffer at non-zero level."))
	   ;; Empty chunk is allowed, but only at the first level.
	   (let* ((chunk (make-string-chunk :|null| ""))
		  (hash (write-pool-chunk chunk)))
	     hash))
	  ((length= buffer 1)
	   ;; If there is a single hash, just use it instead of making
	   ;; an indirect block of one element.
	   (first-elt buffer))
	  (t
	   ;; Otherwise, make a new chunk out of the data.
	   (let* ((data (iter (with dest = (make-byte-vector (* 20 (length buffer))))
			      (for src in-sequence buffer)
			      (for dpos from 0 by 20)
			      (replace dest src :start1 dpos)
			      (finally (return dest))))
		  (chunk (make-byte-vector-chunk (indirect-name builder level)
						 data))
		  (hash (write-pool-chunk chunk)))
	     hash)))))

(defun indirect-append (builder hash &optional (level 0))
  "Add the given indirect at the specified level.  Level 0 indicates a
hash of a data block."
  (with-slots (buffers) builder
    (cond ((zerop (length buffers))
	   (indirect-push builder hash))
	  ((length= *indirect-limit* (last-elt buffers))
	   (let ((summary-hash (indirect-summarize builder (vector-pop buffers) (1+ level))))
	     (indirect-append builder summary-hash (1+ level))
	     (indirect-push builder hash)))
	  (t (vector-push-extend hash (last-elt buffers))))))

(defun indirect-finish (builder)
  "Flush out any remaining indirect blocks, and return the final
resultant hash."
  (with-slots (buffers) builder
    (when (length= 0 buffers)
      (error "Empty indirect-builder."))

    ;; Collapse the buffers.
    (iter (while (> (length buffers) 1))
	  (for level from 0)
	  (let ((tmp (vector-pop buffers)))
	    (indirect-append builder (indirect-summarize builder tmp (1+ level))
			     (1+ level))))
    (indirect-summarize builder (vector-pop buffers) 0)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;; Traversal tries to handle several cases where we need to traverse
;;; the tree.  The requirements are: 1. Allow the user of the
;;; traversal to prune the tree as desired, and 2. Don't read the data
;;; of leaf nodes unless they are requested.

;;; back
;;;   dir-node
;;;     file-node
;;;       (index nodes)
;;;       (data nodes)

;;; To start with, let's write a simple traversal that computes a
;;; size, something like the unix 'du' command.

(defgeneric walk-node-size (node))

;;; The traversal calls visit on each node.  The visit method should
;;; return a true value if the walker should descend its children.
;;; The direction will be :enter during pre-order traversal, and
;;; :leave during post-order traversal.  Note that :leave will be
;;; called, even for nodes that don't have obvious children.

(defgeneric visit (visitor node direction))

(defmethod visit (visitor node direction)
  t)

(defgeneric traverse-node (node visitor))

(defun traverse (hash visitor)
  "Walk through a backup, invoking visit on each node, descending
subnodes as directed by the visitor."
  (let ((node (get-chunk hash)))
    (let ((traverse-children? (visit visitor node :enter)))
      (when traverse-children?
	(traverse-node node visitor)))
    (visit visitor node :leave)))

(defmethod traverse-node ((node backup) visitor)
  (traverse (slot-value node 'hash) visitor))

(defmethod traverse-node ((node dir-node) visitor)
  (traverse (slot-value node 'children) visitor))

(defmethod traverse-node ((node dir-leaf) visitor)
  (dolist (item (slot-value node 'entries))
    ;; TODO: How does the name get passed down?  It seems like it
    ;; would be useful.
    (traverse (cdr item) visitor)))

(defmethod traverse-node ((node file-node) visitor)
  (traverse (slot-value node 'data) visitor))

(defmethod traverse-node ((node indirect-data) visitor)
  (iter (for child in-vector (slot-value node 'children))
	(traverse child visitor)))

(defmethod traverse-node ((node indirect-dir) visitor)
  (iter (for child in-vector (slot-value node 'children))
	(traverse child visitor)))

;; Everything else has nothing to do on traversal.
(defmethod traverse-node (node visitor))

;;; Simple 'du' command (reads all data, kind of dumb).

(defclass node-information ()
  ((type :initarg :type :type keyword)
   (compressed :initform 0 :type integer)
   (uncompressed :initform 0 :type integer)))

(defclass du-state ()
  ((info :initform (make-hash-table) :type hash-table)))

(defun du-state-information (state node-type)
  (with-slots (info) state
    (or (gethash node-type info)
	(let ((new-info (make-instance 'node-information
				       :type node-type)))
	  (setf (gethash node-type info) new-info)))))

(defmethod visit ((visitor du-state) node (direction (eql :enter)))
  (let ((info (du-state-information visitor (type-of node))))
    (with-slots (compressed uncompressed) info
      (incf compressed 1)
      (incf uncompressed (chunk-data-length (slot-value node 'chunk)))))
  t)

;;; Avoid reading the data nodes (we still need sizes eventually).
(defmethod visit ((visitor du-state) (node indirect-data) (direction (eql :enter)))
  (let ((info (du-state-information visitor (type-of node))))
    (with-slots (compressed uncompressed) info
      (incf compressed 1)
      (incf uncompressed (chunk-data-length (slot-value node 'chunk)))
      (let ((children (slot-value node 'children)))
	(incf compressed (length children)))))
  nil)

;;; When visiting file nodes, don't read the data itself, but do walk
;;; down through indirect nodes.
(defmethod visit ((visitor du-state) (node file-node) (direction (eql :enter)))
  (let* ((info (du-state-information visitor (type-of node)))
	 (data-type (pool-get-type (slot-value node 'data))))
    (with-slots (compressed uncompressed) info
      (case data-type
	((:|blob| :|null|)
	 (incf compressed 1)
	 (incf uncompressed 1)
	 nil)
	(t t)))))

(defun pretty-size (number)
  (iter (for suffix in '("B" "kB" "MB" "GB" "TB" "PB" "EB" "ZB" "YB"))
	(while (>= number 1024))
	(setf number (/ number 1024.0))
	(finally
	 (return (format nil "~8,3F ~A" number suffix)))))

(defun tree-size (hash)
  (let ((state (make-instance 'du-state)))
    (traverse hash state)
    (iter (for (type info) in-hashtable (slot-value state 'info))
	  (for uncompressed = (slot-value info 'uncompressed))
	  (for compressed = (slot-value info 'compressed))
	  (sum compressed into compressed-sum)
	  (sum uncompressed into uncompressed-sum)
	  (format t "~20@A ~15d ~15d ~9@A~%" type
		  (slot-value info 'compressed)
		  uncompressed
		  (pretty-size uncompressed))
	  (finally
	   (format t "----------~%~20A ~15d ~15d ~9@A~%"
		   "" compressed-sum
		   uncompressed-sum
		   (pretty-size uncompressed-sum))))))

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
	  (hexify (slot-value back 'source-hash))
	  (format-timestring nil (backup-date back)
			     :format '(:year #\- (:month 2) #\- (:day 2) #\_
				       (:hour 2) #\: (:min 2)))
	  (format-attributes (slot-value back 'attributes))))

(defun list-backups ()
  (let* ((backs (mapcar #'get-chunk (pool-backup-list)))
	 (backs (sort backs #'timestamp< :key #'backup-date)))
    (mapc #'show-backup backs)
    (values)))
