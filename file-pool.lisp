;;; File pools

(defpackage #:ldump.file-pool
  (:use #:cl #:iterate #:cl-fad
	#:alexandria
	#:hashlib
	#:ldump.pool
	#:ldump #:ldump.chunk #:ldump.file-index)
  (:shadowing-import-from #:alexandria #:copy-stream #:copy-file)
  (:export #:file-pool #:create-file-pool))
(in-package #:ldump.file-pool)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Pool creation.

(defun ensure-directory (path)
  (let* ((path (pathname-as-directory path))
	 (path (truename path)))
    (unless (directory-exists-p path)
      (error "Pool pathname is not a directory: ~S" path))
    path))

(defun ensure-empty-directory (path)
  "Given a path, ensure that it is a pathname referring to an empty
directory, and return it.  Otherwise, it raises an error."
  (let* ((path (ensure-directory path)))
    (unless (null (list-directory path))
      (error "Cannot create pool in non-empty directory: ~S" path))
    path))

(deftype power-range (a b)
  "A range of integers between 2^A and 2^B."
  `(integer ,(expt 2 a) ,(expt 2 b)))

(defparameter *default-limit* (* 640 1024 1024))

(defun create-file-pool (path &key (limit *default-limit*) newfile)
  "Create a new pool in PATH, which must be an empty directory.  LIMIT
indicates the larges size an individual pool file can grow (which must
be less than 2GB).  if NEWFILE is true, then each open of the pool
will result in new pool files being created rather than existing files
being appended to."
  (check-type limit (power-range 20 31))
  (let* ((path (ensure-empty-directory path))
	 (metadata (merge-pathnames (make-pathname :directory '(:relative "metadata"))
				    path))
	 (props-name (merge-pathnames (make-pathname :name "props" :type "txt")
				      metadata))
	 (uuid (with-output-to-string (stream)
		 (write (uuid:make-v4-uuid) :stream stream))))
    (ensure-directories-exist props-name)
    (with-open-file (stream props-name :if-does-not-exist :create
			    :direction :output)
      (format stream "# Ldump metadata properties~%")
      (format stream "uuid=~(~A~)~%" uuid)
      (format stream "newfile=~A~%" (if newfile "true" "false"))
      (format stream "limit=~D~%" limit))))

;;; Java properties files aren't all that terribly well documented.
;;; Fortunately, we only write fairly simplistic formats in them.
(defun read-flat-properties (path)
  "Read the file containing java-style properties, returning a
hash-table mapping the keys to the values."
  (let ((result (make-hash-table :test 'equal)))
    (iter (for line in-file path using #'read-line)
	  (when (starts-with #\# line)
	    (next-iteration))
	  (for pos = (position #\= line))
	  (unless pos
	    (error "Invalid line in property file ~S" path))
	  (for key = (subseq line 0 pos))
	  (for value = (subseq line (1+ pos)))
	  (setf (gethash key result) value))
    result))

(defun decode-java-boolean (text)
  (cond ((string-equal text "false") nil)
	((string-equal text "true") t)
	(t (error "Invalid boolean value: ~S" text))))

(defun decode-backup-properties (props pool)
  "Decode the properties and store them into the attributes pool."
  (let ((uuid (gethash "uuid" props))
	(limit (or (when-let ((p (gethash "limit" props)))
		     (parse-integer p))
		   *default-limit*)))
    (unless uuid
      (error "Backup doesn't contain a uuid"))
    (setf (slot-value pool 'uuid) (uuid:make-uuid-from-string uuid))
    (setf (slot-value pool 'limit) limit)
    (setf (slot-value pool 'newfile)
	  (decode-java-boolean (gethash "newfile" props "false")))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; 

(defstruct (pool-file (:constructor make-pool-file
				    (path &aux (cfile (open-chunk-file path))
					  (index (make-index (merge-pathnames
							      (make-pathname :type "idx")
							      path))))))
  (cfile nil :type chunk-file)
  (index nil :type base-index))

(defun reindex-pool-file (pfile)
  "Create a new index for this chunk file."
  (let* ((cfile (pool-file-cfile pfile))
	 (index (pool-file-index pfile))
	 (length (chunk-file-length cfile)))
    (warn "Re-indexing ~S" (chunk-file-path cfile))
    (clear-index index)
    (iter (with pos = 0)
	  (until (= pos length))
	  (for (values type hash next-pos) = (read-chunk-info cfile pos))
	  (index-add index hash type pos)
	  (setf pos next-pos))
    (save-index index length)))

(defun load-pool-file-index (pfile)
  (with-accessors ((cfile pool-file-cfile)
		   (index pool-file-index))
      pfile
    (multiple-value-bind (ok err)
	(load-index index (chunk-file-length cfile))
      (declare (ignorable err))
      (unless ok
	;; Try once to reindex this file.
	(reindex-pool-file pfile)))))

(defun read-pool-file-chunk (pfile hash)
  "Try to read the named chunk from the pool file.  Returns either a
CHUNK if it is present, or NIL if not present."
  (when-let ((offset (index-lookup (pool-file-index pfile) hash)))
    (read-chunk (pool-file-cfile pfile) (index-node-offset offset))))

(defun pool-file-type (pfile hash)
  "Determine if the hash is present in this pool file, and return the
type of the chunk if it is."
  (when-let ((node (index-lookup (pool-file-index pfile) hash)))
    (index-node-type node)))

(defun get-data-files (dir)
  (let* ((names (list-directory dir))
	 (names (delete "data" names :key #'pathname-type :test #'string/=)))
    (sort names #'string> :key #'pathname-name)))

(defun increment-name (name)
  "Assuming that the name ends in digits of a number, increment that
number, returning a fresh string."
  (let ((result (copy-seq name)))
    (iter (for pos downfrom (1- (length result)))
	  (when (minusp pos)
	    (error "Name cannot be numerically incremented: ~S" name))
	  (for ch = (elt result pos))
	  (when (or (char< ch #\0) (char> ch #\9))
	    (error "Name cannot be numerically incremented: ~S" name))
	  (when (char< ch #\9)
	    (setf (elt result pos)
		  (code-char (1+ (char-code ch))))
	    (return result))
	  (setf (elt result pos) #\0))))

(defun read-backup-list (dir)
  "Read the list of completed backups."
  (let* ((subname (make-pathname :directory '(:relative "metadata")
				 :name "backups"
				 :type "txt"))
	 (name (merge-pathnames subname dir)))
    (iter (for line in-file name using #'read-line)
	  (collect (hashlib:unhexify line)))))

(defclass file-pool (pool)
  ((dir :type pathname :initarg :dir)
   (pfiles :initarg :pfiles)

   ;; Properties store in the props.txt file.
   (limit :initarg :limit :initform *default-limit* :type integer)
   (uuid :initarg :uuit :type uuid:uuid)
   (newfile :initarg :newfile :type boolean)))

(defun compute-next-name (last-pfile dir)
  "If last-name is a pathname, return a new name with the number at
the end of the name incremented.  Otherwise, make a name for a new
pool file within DIR as the first file."
  (if (null last-pfile)
      (make-pathname :name "pool-data-0000" :type "data"
		     :defaults dir)
      (let ((name (chunk-file-path (pool-file-cfile last-pfile))))
	(make-pathname :name (increment-name (pathname-name name))
		       :defaults name))))

;;; TODO: handle the 'newfile' property.
(defun room-for-file-p (pool chunk)
  "Returns true if there is room in the current pool to write this
chunk."
  (let ((pfile (first (slot-value pool 'pfiles))))
    (and pfile
	 (let* ((needed (chunk-write-size chunk))
		(size-taken (chunk-file-length (pool-file-cfile pfile)))
		(limit (slot-value pool 'limit)))
	   (<= (+ size-taken needed) limit)))))

(defmethod %pool-flush ((pool file-pool))
  "Flush any pending output."
  (with-slots (pfiles) pool
    (let* ((first-pfile (first pfiles)))
      (when first-pfile
	(let ((cfile (pool-file-cfile first-pfile)))
	  (chunk-file-flush cfile)
	  (save-index (pool-file-index first-pfile) (chunk-file-length cfile)))))))

(defun make-new-pool-file (pool)
  "Create a new pool file in this pool."
  (%pool-flush pool)
  (with-slots (pfiles dir) pool
    (let* ((first-pfile (first pfiles))
	   (new-name (compute-next-name first-pfile dir))
	   (pfile (make-pool-file new-name)))
      (push pfile pfiles))))

(defmethod open-pool ((type (eql 'file-pool)) &key dir &allow-other-keys)
  (let* ((dir (ensure-directory dir))
	 (props-name (merge-pathnames (make-pathname :directory '(:relative "metadata")
						     :name "props" :type "txt")
				      dir))
	 (props (read-flat-properties props-name))
	 (data-names (get-data-files dir))
	 (pfiles (mapcar #'make-pool-file data-names)))
    (mapc #'load-pool-file-index pfiles)
    (let ((pool (make-instance 'file-pool :pfiles pfiles :dir dir)))
      (setf *current-pool* pool)
      (decode-backup-properties props pool)
      pool)))

(defmethod %close-pool ((pool file-pool))
  (%pool-flush pool)
  (with-slots (pfiles)
      pool
    (dolist (pf pfiles)
      (chunk-file-close (pool-file-cfile pf))))
  (slot-makunbound pool 'pfiles)
  (when (eq pool *current-pool*)
    (setf *current-pool* nil)))

(defmethod %pool-get-chunk ((pool file-pool) hash)
  "Try to read the named chunk from the pool file."
  (with-slots (pfiles) pool
    (dolist (pf pfiles)
      (when-let ((chunk (read-pool-file-chunk pf hash)))
	(return-from %pool-get-chunk chunk))))
  nil)

(defmethod %pool-get-type ((pool file-pool) hash)
  "Return the type of the chunk if present in the pool."
  (with-slots (pfiles) pool
    (dolist (pf pfiles)
      (when-let ((type (pool-file-type pf hash)))
	(return-from %pool-get-type type)))))

(defmethod %write-pool-chunk ((pool file-pool) chunk)
  "Write this chunk to the pool.  Returns the hash of the item."
  (unless (room-for-file-p pool chunk)
    (make-new-pool-file pool))
  (let* ((pfile (first (slot-value pool 'pfiles)))
	 (offset (write-chunk (pool-file-cfile pfile) chunk)))
    (index-add (pool-file-index pfile)
	       (chunk-hash chunk)
	       (chunk-type chunk)
	       offset)
    (chunk-hash chunk)))

(defmethod %pool-backup-list ((pool file-pool))
  "Return a list of hashes that represent backup root nodes."
  (with-slots (dir) pool
    (read-backup-list dir)))

;;; Lots of todos here:
;;; - Handle index failures
;;; - Validate pool directory better
;;; - Allow writing
;;; - Pool protocol to allow remote pools.
