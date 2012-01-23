;;; File pools

(defpackage #:ldump.file-pool
  (:use #:cl #:iterate #:cl-fad
	#:alexandria
	#:hashlib
	#:ldump #:ldump.chunk #:ldump.file-index)
  (:shadowing-import-from #:alexandria #:copy-stream #:copy-file)
  (:export #:pool #:open-pool #:close-pool #:with-pool
	   #:*current-pool*
	   #:pool-get-chunk #:pool-get-type #:pool-backup-list))
(in-package #:ldump.file-pool)

;;; TODO: Writing to pools
;;; TODO: Index recovery
;;; TODO: Make pool protocol

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

(defun create-pool (path &key (limit *default-limit*) newfile)
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

(defclass backup-properties ()
  ((limit :initarg :limit :initform *default-limit*
	  :type integer)
   (uuid :initarg :uuid :type uuid:uuid)
   (newfile :initarg :newfile :type boolean)))

(defun decode-java-boolean (text)
  (cond ((string-equal text "false") nil)
	((string-equal text "true") t)
	(t (error "Invalid boolean value: ~S" text))))

(defun decode-backup-properties (props)
  (let ((uuid (gethash "uuid" props))
	(limit (or (when-let ((p (gethash "limit" props)))
		     (parse-integer p))
		   *default-limit*)))
    (unless uuid
      (error "Backup doesn't contain a uuid"))
    (make-instance 'backup-properties
		   :uuid (uuid:make-uuid-from-string uuid)
		   :limit limit
		   :newfile (decode-java-boolean (gethash "newfile" props "false")))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; 

(defstruct (pool-file (:constructor make-pool-file
				    (path &aux (cfile (open-chunk-file path))
					  (index (make-index (merge-pathnames
							      (make-pathname :type "idx")
							      path))))))
  (cfile nil :type chunk-file)
  (index nil :type base-index))

(defun load-pool-file-index (pfile)
  (with-accessors ((cfile pool-file-cfile)
		   (index pool-file-index))
      pfile
    (multiple-value-bind (ok err)
	(load-index index (chunk-file-length cfile))
      (unless ok
	(error err)))))

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
  (let ((names (list-directory dir)))
    (delete "data" names :key #'pathname-type :test #'string/=)))

(defun read-backup-list (dir)
  "Read the list of completed backups."
  (let* ((subname (make-pathname :directory '(:relative "metadata")
				 :name "backups"
				 :type "txt"))
	 (name (merge-pathnames subname dir)))
    (iter (for line in-file name using #'read-line)
	  (collect (hashlib:unhexify line)))))

(defclass pool ()
  ((dir :type pathname :initarg :dir)
   (properties :initarg :properties :type backup-properties)
   (pfiles :initarg :pfiles)))

(defvar *current-pool* "Holds the last opened pool.")

(defun open-pool (dir)
  (let* ((dir (ensure-directory dir))
	 (props-name (merge-pathnames (make-pathname :directory '(:relative "metadata")
						     :name "props" :type "txt")
				      dir))
	 (props (read-flat-properties props-name))
	 (properties (decode-backup-properties props))
	 (data-names (get-data-files dir))
	 (pfiles (mapcar #'make-pool-file data-names)))
    (mapc #'load-pool-file-index pfiles)
    (let ((pool (make-instance 'pool :pfiles pfiles :dir dir
			       :properties properties)))
      (setf *current-pool* pool)
      pool)))

(defun close-pool (&key (pool *current-pool*))
  (with-slots (pfiles)
      pool
    (dolist (pf pfiles)
      (chunk-file-close (pool-file-cfile pf))))
  (slot-makunbound pool 'pfiles)
  (when (eq pool *current-pool*)
    (setf *current-pool* nil)))

(defmacro with-pool ((var dir) &body body)
  `(let* (*current-pool*
	  (,var (open-pool ,dir)))
     (unwind-protect (progn ,@body)
       (close-pool :pool ,var))))

;;; TODO Generalize this code a bit.
(defun pool-get-chunk (hash &key (pool *current-pool*))
  "Try to read the named chunk from the pool file."
  (with-slots (pfiles) pool
    (dolist (pf pfiles)
      (when-let ((chunk (read-pool-file-chunk pf hash)))
	(return-from pool-get-chunk chunk))))
  nil)

(defun pool-get-type (hash &key (pool *current-pool*))
  "Return the type of the chunk if present in the pool."
  (with-slots (pfiles) pool
    (dolist (pf pfiles)
      (when-let ((type (pool-file-type pf hash)))
	(return-from pool-get-type type)))))

(defun pool-backup-list (&key (pool *current-pool*))
  "Return a list of hashes that represent backup root nodes."
  (with-slots (dir) pool
    (read-backup-list dir)))

;;; Lots of todos here:
;;; - Handle index failures
;;; - Validate pool directory better
;;; - Allow writing
;;; - Pool protocol to allow remote pools.
