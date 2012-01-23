;;; Most CL implementations provide some kind of bindings to native
;;; syscalls.  These will usually be more efficient than wrapping them
;;; all in cffi.  Plus, some of the calls, such as readdir (struct
;;; dirent) are unusual, and it is usually better to do this directly.

(defpackage #:ldump.posix
  (:use #:cl #:iterate #:ldump.nodes #:ldump.pack))
(in-package #:ldump.posix)

;;; This isn't really documented, but it is necessary to be able to
;;; decode external filenames as if they were :latin-1.  We use
;;; :latin-1 because it allows us to fully encode/decode arbitrary
;;; names, whether they are valid characters or not.

(defmacro with-latin-1 (&body body)
  `(let ((sb-alien::*default-c-string-external-format* :latin-1))
     ,@body))

(defun read-directory (path)
  "Read in the contents of the named directory, returning a list of
pairs, with the car being the name, and the cdr being the inode
number."
  ;; (declare (optimize (speed 0) (space 0)))
  (with-latin-1
    (let ((dir (sb-posix:opendir path)))
      (unwind-protect
	   (iter (for dirent = (sb-posix:readdir dir))
		 (until (sb-alien:null-alien dirent))
		 (for name = (sb-posix:dirent-name dirent))
		 (when (and (string/= name ".")
			    (string/= name ".."))
		   (for ino = (sb-posix:dirent-ino dirent))
		   (collect (cons name ino))))
	(sb-posix:closedir dir)))))

(defun stat-kind (stat)
  (let ((mode (sb-posix:stat-mode stat)))
    (cond ((sb-posix:s-isreg mode) 'file-node)
	  ((sb-posix:s-isdir mode) 'dir-node)
	  ((sb-posix:s-islnk mode) 'link-node)
	  ((sb-posix:s-ischr mode) 'chr-node)
	  ((sb-posix:s-isblk mode) 'blk-node)
	  ((sb-posix:s-isfifo mode) 'fifo-node)
	  ((sb-posix:s-issock mode) 'sock-node)
	  (t (error "Unknown file mode: ~o" mode)))))

(defun decode-stat (name stat)
  (let* ((kind (stat-kind stat))
	 (rdev (case kind
		 ((chr-node blk-node) `(:rdev ,(sb-posix:stat-rdev stat)))
		 (t '()))))
    (apply #'make-instance kind
	   :name name
	   :ctime (sb-posix:stat-ctime stat)
	   :mtime (sb-posix:stat-mtime stat)
	   :dev (sb-posix:stat-dev stat)
	   :gid (sb-posix:stat-gid stat)
	   :uid (sb-posix:stat-uid stat)
	   :ino (sb-posix:stat-ino stat)
	   :mode (logandc2 (sb-posix:stat-mode stat) sb-posix:S-IFMT)
	   :nlink (sb-posix:stat-nlink stat)
	   :size (sb-posix:stat-size stat)
	   rdev)))

(defun symlink-info (path)
  (with-latin-1
    (list :target (sb-posix:readlink path))))

(defun get-directory-entries (path)
  "Read all of the directory entries in PATH, returning nodes for them."
  (with-latin-1
    (flet ((make-name (name)
	     (concatenate 'string path "/" name)))
      (let* ((entries (read-directory path))
	     (entries (sort entries #'< :key #'cdr))
	     (entries (iter (for entry in entries)
			    (for name = (car entry))
			    (collect (decode-stat name (sb-posix:lstat (make-name name)))))))
	(sort entries #'string< :key #'node-name)))))

;;; This seems to be linux specific, and not captured by the sb-posix
;;; library.
(defconstant O-NOATIME #o1000000)

;;; Open a posixy file for reading, trying to not modify the atime (if
;;; possible).
(defun open-fd-for-read (path)
  (with-latin-1
    (handler-case (sb-posix:open path (logior sb-posix:O-RDONLY O-NOATIME))
      (sb-posix:syscall-error (err)
	(if (= (sb-posix:syscall-errno err) sb-posix:EPERM)
	    (sb-posix:open path sb-posix:O-RDONLY)
	    (error err))))))

(defun for-file-contents (path handler &optional (buffer-size (* 256 1024)))
  "Read the contents of the file in BUFFER-SIZE chunks, calling
HANDLER with a freshly allocated buffer for each."
  (let* ((fd (open-fd-for-read path))
	 (stream (sb-sys:make-fd-stream fd :input t :element-type '(unsigned-byte 8)
					:file path
					:name (format nil "Posix stream for ~S" path))))
    (unwind-protect
	 (iter (for buffer = (make-byte-vector buffer-size))
	       (for count = (read-sequence buffer stream))
	       (until (zerop count))
	       (funcall handler (if (= count buffer-size) buffer
				    (subseq buffer 0 count))))
      (close stream))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Root access.
;;;
;;; There are two ways for this code to have root privileges for file
;;; access.  One is to run the lisp image as root.  The other is to
;;; run the image as root, and have the image drop it's privileges
;;; upon startup.  The second scenario supports entering root to
;;; perform operations.  This is useful during development to avoid
;;; having the entire image.

;;; Note that this setting is probably not thread safe.

;;; Normally, the best way of finding this would be to call getresuid,
;;; but this faults in 1.0.55 of sbcl.  Instead, we'll try becoming
;;; root, and if it fails, clear this.
(defparameter *can-become-root* t)

(defmacro as-root (&body body)
  "Try executing BODY as root.  It may or may not actually run as
root, depending on how the lisp image was started."
  (let ((current-id (gensym "CURRENT-ID"))
	(body-func (gensym "BODY")))
    `(flet ((,body-func ()
	      ,@body))
       (if *can-become-root*
	   (let ((,current-id (sb-posix:geteuid)))
	     (sb-posix:seteuid 0)
	     (unwind-protect
		  (,body-func)
	       (sb-posix:seteuid ,current-id)))
	   (,body-func)))))

;;; Try becoming root, and if that fails, clear the state.
(handler-case (as-root)
  (sb-posix:syscall-error ()
    (setf *can-become-root* nil)))

(defun have-root-p ()
  "Are we executing with a effective root user?"
  (zerop (sb-posix:geteuid)))
