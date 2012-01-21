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
   (pfiles :initarg :pfiles)))

(defvar *current-pool* "Holds the last opened pool.")

(defun open-pool (dir)
  (let* ((data-names (get-data-files dir))
	 (pfiles (mapcar #'make-pool-file data-names)))
    (mapc #'load-pool-file-index pfiles)
    (let ((pool (make-instance 'pool :pfiles pfiles :dir dir)))
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
