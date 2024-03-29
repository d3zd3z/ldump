(defpackage #:ldump.file-index
  (:use #:cl #:ldump #:ldump.pack #:ldump.chunk #:iterate)
  (:import-from #:babel #:string-to-octets #:octets-to-string)
  (:export #:index-node #:index-node-type #:index-node-offset #:index-node-p #:make-index-node

	   #:make-index #:load-index #:save-index #:clear-index
	   #:index-error #:index-error-reason

	   #:base-index #:ram-index #:file-index
	   #:index-lookup #:index-add))
(in-package #:ldump.file-index)

;;; Each pool file has an index file associated with it.  The index
;;; file maps a hash to the offset of the file where that chunk is
;;; stored.  It also keeps track of the chunks.
;;;
;;; We maintain a RAM-based index until the file is flushed.  Then the
;;; entire index will be written, in-order, so that it can be loaded
;;; in a compact form, and binary searches used.

(defstruct index-node
  (type :xxxx
	:type keyword)
  (offset 0 :type (unsigned-byte 32)))

(defclass base-index () ())

;;; The index protocol.
(defgeneric index-lookup (index hash)
  (:documentation
   "Lookup HASH in the INDEX.  Returns an INDEX-NODE if present.
Otherwise returns NIL."))

(defgeneric index-add (index hash type offset)
  (:documentation
   "Add a new node to the index."))

(defgeneric index-all-nodes (index)
  (:documentation
   "Return a list of pairs of (HASH . INDEX-NODE) for all nodes in
this index.  The results are in an unspecified order."))

(defclass ram-index (base-index)
  ((nodes :type hashtable
	  :initform (make-hash-table :test 'equalp)))) 

(defmethod index-lookup ((index ram-index) hash)
  (let ((nodes (slot-value index 'nodes)))
    (prog1 (gethash hash nodes))))

(defmethod index-add ((index ram-index) hash type offset)
  (let ((nodes (slot-value index 'nodes)))
    (when (gethash hash nodes)
      (error "Duplicate index key"))
    (setf (gethash hash nodes)
	  (make-index-node :type type :offset offset))))

(defmethod index-all-nodes ((index ram-index))
  (let ((nodes (slot-value index 'nodes)))
    (iter (for (hash node) in-hashtable nodes)
	  (collect (cons hash node)))))

(defun ram-index-empty-p (index)
  (zerop (hash-table-count (slot-value index 'nodes))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Comparing hashes.

(defun hash-compare (a b &key (start1 0) end1)
  "Returns an integer less than, greater than or equal to zero
depending on whether the hash A is lexicographically less than,
greater than, or equal to B."
  (let ((pos (mismatch a b :start1 start1 :end1 end1)))
    (unless pos
      (return-from hash-compare 0))
    (let ((a-elt (aref a pos))
	  (b-elt (aref b (- pos start1))))
      (- a-elt b-elt))))

(defun hash< (a b)
  (minusp (hash-compare a b)))

(defun get-sorted-nodes (index)
  (sort (index-all-nodes index)
	#'hash<
	:key #'car))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Index writing.

(defparameter *pool-magic* (string-to-octets "ldumpidx"))

(defun write-header (pool-offset stream)
  (let ((buf (make-sequence '(vector (unsigned-byte 8)) 16)))
    (replace buf *pool-magic*)
    (pack-le-integer buf 8 4 4)
    (pack-le-integer buf 12 pool-offset 4)
    (write-sequence buf stream)))

(defun compute-top-index (hashes)
  "Given a list of hashes, return a list of offsets for the top-level
index.  Each index gives the first hash with a first byte greater
than that index offset."
  (let* ((hashes (coerce hashes 'vector))
	 (size (length hashes))
	 (offset 0))
    (iter (for first from 0 below 256)
	  (iter (while (< offset size))
		(while (>= first (aref (aref hashes offset) 0)))
		(incf offset))
	  (collect offset))))

(defun write-top-index (hashes stream)
  (let ((buffer (make-sequence '(vector (unsigned-byte 8)) (* 4 256))))
    (iter (for top in (compute-top-index hashes))
	  (for offset from 0 by 4)
	  (pack-le-integer buffer offset top 4))
    (write-sequence buffer stream)))

(defun write-hashes (hashes stream)
  (iter (for hash in hashes)
	(write-sequence hash stream)))

(defun write-offsets (offsets stream)
  (let ((buffer (make-sequence '(vector (unsigned-byte 8)) 4)))
    (iter (for offset in offsets)
	  (pack-le-integer buffer 0 offset 4)
	  (write-sequence buffer stream))))

(defun invert-hash-table (table &optional (test 'eql))
  "Build a new hashtable with the keys and values reversed."
  (let ((result (make-hash-table :test test :size (hash-table-size table))))
    (iter (for (key value) in-hashtable table)
	  (setf (gethash value result) key))
    result))

(defun build-kind-map (types)
  "Builds the mapping of types.  Returns a vector of the types, and a
list of the kinds in this indexed order."
  (let ((kind-map (make-hash-table))
	(kinds '())
	(offset -1))
    (dolist (type types)
      (multiple-value-bind (idx ok) (gethash type kind-map)
	(push (if ok idx
		  (setf (gethash type kind-map)
			(incf offset)))
	      kinds)))
    (let* ((inv-kind-map (invert-hash-table kind-map))
	   (keymap (iter (for i from 0 below (hash-table-count kind-map))
			 (collect (gethash i inv-kind-map)))))
      (values keymap (nreverse kinds)))))

(defun write-types (types stream)
  (multiple-value-bind (kind-map kinds)
      (build-kind-map types)
    (let* ((count (length kind-map))
	   (buffer-1 (make-byte-vector (+ 4 (* 4 count))))
	   (buffer-2 (make-byte-vector (length kinds))))
      (pack-le-integer buffer-1 0 count 4)
      (iter (for offset from 1)
	    (for km in kind-map)
	    (let ((name (string-to-octets (symbol-name km))))
	      (replace buffer-1 name :start1 (* 4 offset))))
      (iter (for i from 0)
	    (for kind in kinds)
	    (setf (aref buffer-2 i) kind))
      (write-sequence buffer-1 stream)
      (write-sequence buffer-2 stream))))

(defun write-index (path index pool-offset)
  (with-open-file (stream path :direction :output
			  :element-type '(unsigned-byte 8)
			  :if-does-not-exist :create
			  :if-exists :supersede)
    (let* ((nodes (get-sorted-nodes index))
	   (hashes (mapcar #'car nodes))
	   (offsets (mapcar (lambda (x) (index-node-offset (cdr x))) nodes))
	   (types (mapcar (lambda (x) (index-node-type (cdr x))) nodes)))
      (write-header pool-offset stream)
      (write-top-index hashes stream)
      (write-hashes hashes stream)
      (write-offsets offsets stream)
      (write-types types stream)
      (values))))

;;; TODO: add an fsync to this operation.  It won't be portable, though.
(defun safe-write-index (path index pool-offset)
  (let* ((path (pathname path))
	 (tmp-name (merge-pathnames (make-pathname :type "idx-tmp") path)))
    (write-index tmp-name index pool-offset)
    #+clisp (delete-file path)
    (rename-file tmp-name path
		 #+ccl :if-exists #+ccl :overwrite
		 #|
		 ;; Clisp is supposed to have an overwrite argument, but it fails.
		 #+clisp :if-exists #+clisp :overwrite
		 |#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; File-based index.

(defclass file-index (base-index)
  ((top :type (vector (unsigned-byte 32))
	:initarg :top)
   (count :type integer :initarg :count)
   (hashes :type byte-vector :initarg :hashes)
   (offsets :type (vector (unsigned-byte 32))
	    :initarg :offsets)
   (kind-map :type vector :initarg :kind-map)
   (kinds :type byte-vector :initarg :kinds)))

(define-condition index-error (error)
  ((reason :initform "Unspecified" :initarg :reason :reader index-error-reason))
  (:report (lambda (condition stream)
	     (format stream "Error reading index file: ~A"
		     (index-error-reason condition)))))

(defun read-header (stream pool-offset)
  "Read the index file header, make sure that it is the version we are
expecting, and that the pool-offset matches."
  (let ((header (read-new-sequence stream 16
				   :on-failure-raise 'index-error)))
    (when (mismatch header *pool-magic* :end1 8)
      (error (make-condition 'index-error
			     :reason "Invalid magic in index")))
    (when (/= 4 (unpack-le-integer header 8 4))
      (error (make-condition 'index-error
			     :reason "Invalid index version")))
    (let ((file-pool-offset (unpack-le-integer header 12 4)))
      (when (/= pool-offset file-pool-offset)
	(error (make-condition 'index-error
			       :reason (format nil "Pool offset is incorrect: file has #x~X, expecting #x~X"
					       file-pool-offset pool-offset)))))
    (values)))

(defun read-numbers (stream count)
  "Read COUNT little-endian 32-bit integers from STREAM."
  (let ((raw (read-new-sequence stream (* 4 count)))
	(result (make-array count :element-type '(unsigned-byte 32))))
    (dotimes (i count result)
      (setf (aref result i)
	    (unpack-le-integer raw (* i 4) 4)))))

(defun read-kind-map (stream)
  (let* ((raw-count (read-new-sequence stream 4))
	 (count (unpack-le-integer raw-count 0 4))
	 (raw (read-new-sequence stream (* 4 count)))
	 (result (make-array count)))
    (dotimes (i count result)
      (let* ((name (octets-to-string raw :start (* i 4)
				     :end (* (1+ i) 4)))
	     (type (intern name "KEYWORD")))
	(setf (aref result i) type)))))

(defun read-index (path pool-offset)
  (with-open-file (stream path :element-type '(unsigned-byte 8))
    (read-header stream pool-offset)
    (let* ((top (read-numbers stream 256))
	   (count (aref top 255))
	   (hashes (read-new-sequence stream (* count 20)))
	   (offsets (read-numbers stream count))
	   (kind-map (read-kind-map stream))
	   (kinds (read-new-sequence stream count)))
      (make-instance 'file-index
		     :top top
		     :count count
		     :hashes hashes
		     :offsets offsets
		     :kind-map kind-map
		     :kinds kinds))))

(defmethod index-lookup ((index file-index) hash)
  "Determine if this particular hash is present in the index.  Returns
an integer offset if present or NIL if it is not."
  (let* ((top (slot-value index 'top))
	 (hashes (slot-value index 'hashes))
	 (first-byte (aref hash 0))
	 (low (if (plusp first-byte)
		  (aref top (1- first-byte))
		  0))
	 (high (1- (aref top first-byte))))
    (flet ((lookup (pos)
	     (let ((kind-map (slot-value index 'kind-map))
		   (kinds (slot-value index 'kinds))
		   (offsets (slot-value index 'offsets)))
	       (make-index-node :type (aref kind-map (aref kinds pos))
				:offset (aref offsets pos)))))
    (iter (while (>= high low))
	  (for mid = (+ low (ash (- high low) -1)))
	  (for v = (hash-compare hashes hash
				 :start1 (* 20 mid)
				 :end1 (* 20 (1+ mid))))
	  (cond ((plusp v) (setf high (1- mid)))
		((minusp v) (setf low (1+ mid)))
		(t (return-from index-lookup (lookup mid))))))
    nil))

(defmethod index-add ((index file-index) hash type offset)
  (declare (ignorable hash type offset))
  (error "FILE-TYPE index is not mutable"))

(defmethod index-all-nodes ((index file-index))
  (let ((hashes (slot-value index 'hashes))
	(offsets (slot-value index 'offsets))
	(kind-map (slot-value index 'kind-map))
	(kinds (slot-value index 'kinds)))
    (iter (for i from 0 below (length offsets))
	  (collect (cons (subseq hashes (* i 20) (* (1+ i) 20))
			 (make-index-node :type (aref kind-map (aref kinds i))
					  :offset (aref offsets i)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; A combo index combines a file index of the contents of a file at
;;; the beginning with a RAM index.

(defclass combo-index (base-index)
  ((ram :type ram-index :initarg :ram :initform (make-instance 'ram-index))
   (file :type file-index :initarg :file)
   (path :type pathname :initarg :path)))

(defmethod index-lookup ((index combo-index) hash)
  (or (index-lookup (slot-value index 'ram) hash)
      (and (slot-boundp index 'file)
	   (index-lookup (slot-value index 'file) hash))))

(defmethod index-add ((index combo-index) hash type offset)
  (index-add (slot-value index 'ram) hash type offset))

(defmethod index-all-nodes ((index combo-index))
  (let ((rams (index-all-nodes (slot-value index 'ram)))
	(files (if (slot-boundp index 'file)
		   (index-all-nodes (slot-value index 'file))
		   '())))
    (nconc rams files)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Primary API

(defun make-index (path)
  "Create an index operation to be stored at the given path.  No files
are read."
  (make-instance 'combo-index :path path))

(defun load-index (index pool-offset)
  "Try loading the index from it's file.  The index must have been
written when the size was last POOL-OFFSET, or the load will fail.
Return's true if the data could be loaded.  The second return result
is the condition that caused the failure, or NIL if none."
  (let ((path (slot-value index 'path)))
    (handler-case (read-index path pool-offset)
      ((or index-error file-error) (condition)
	(values nil condition))
      (:no-error (findex)
	(setf (slot-value index 'file) findex
	      (slot-value index 'ram) (make-instance 'ram-index))
	(values index nil)))))

(defun clear-index (index)
  "Clear out any data associated with this index.  The index will
contain no file index, and the RAM index will be empty.  The path will
be the same."
  (setf (slot-value index 'ram) (make-instance 'ram-index))
  (slot-makunbound index 'file))

(defun save-index (index pool-offset)
  "Save this index to it's file.  The index will then be reloaded.
Only writes if the index is dirty."
  (when (ram-index-empty-p (slot-value index 'ram))
    (return-from save-index))
  (write-index (slot-value index 'path) index pool-offset)
  (multiple-value-bind (ok condition)
      (load-index index pool-offset)
    (unless ok
      (error condition))))
