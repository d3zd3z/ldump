;;; Posix calls from ccl.

(defpackage #:ldump.posix
  (:use #:cl #:iterate #:ldump.nodes #:ldump.pack))
(in-package #:ldump.posix)

(defun %opendir (path)
  "Open the directory and return the handle."
  (ccl:with-cstrs ((cpath path))
    (let ((result (#_opendir cpath)))
      (when (ccl:%null-ptr-p result)
	(error "Unable to open directory: ~S" path))
      result)))

(defun read-directory (path)
  "Read in the contents of the named directory, returning a list of
pairs, with the CAR being the name, and the CDR being the inode
number."
  (let ((dir (%opendir path)))
    (unwind-protect
	 (iter (for dirent = (#_readdir dir))
	       (until (ccl:%null-ptr-p dirent))
	       (for name = (ccl:%get-cstring (ccl:pref dirent #>dirent.d_name)))
	       (when (and (string/= name ".")
			  (string/= name ".."))
		 (for ino = (ccl:pref dirent #>dirent.d_ino))
		 (collect (cons name ino))))
      (#_closedir dir))))

;;; Note that the 'stat' structure throughout this has dynamic scope.

(defun stat-kind (stat)
  (let ((mode (logand (ccl:pref stat #>stat64.st_mode) #$S_IFMT)))
    (cond
      ((= mode #$S_IFREG) 'file-node)
      ((= mode #$S_IFDIR) 'dir-node)
      ((= mode #$S_IFLNK) 'link-node)
      ((= mode #$S_IFCHR) 'chr-node)
      ((= mode #$S_IFBLK) 'blk-node)
      ((= mode #$S_IFIFO) 'fifo-node)
      ((= mode #$S_IFSOCK) 'sock-node)
      (t (error "Unknown file mode: ~o" mode)))))

;;; TODO: The size field seems to be bogus, and have an extra value in a high bit set on it.
;;; This seems to only be the case on 32-bit ccl.
#-x86-64 (warn "32-bit CCL has some problem with 'stat'.  Use with caution.")

(defun decode-stat (name stat)
  (let* ((kind (stat-kind stat))
	 (rdev (case kind
		 ((chr-node blk-node) `(:rdev ,(ccl:pref stat #>stat64.st_rdev)))
		 (t '()))))
    (apply #'make-instance kind
	   :name name
	   :ctime (ccl:pref (ccl:pref stat #>stat64.st_ctim) #>timespec.tv_sec)
	   :mtime (ccl:pref (ccl:pref stat #>stat64.st_mtim) #>timespec.tv_sec)
	   :dev (ccl:pref stat #>stat64.st_dev)
	   :gid (ccl:pref stat #>stat64.st_gid)
	   :uid (ccl:pref stat #>stat64.st_uid)
	   :ino (ccl:pref stat #>stat64.st_ino)
	   :mode (logandc2 (ccl:pref stat #>stat64.st_mode) #$S_IFMT)
	   :nlink (ccl:pref stat #>stat64.st_nlink)
	   :size (ccl:pref stat #>stat64.st_size)
	   rdev)))

(defun stat-entry (directory name)
  (let ((path (concatenate 'string directory "/" name)))
    (ccl:rlet ((stat #>stat64))
      (ccl:with-cstrs ((cpath path))
	(let ((result (#_ __lxstat #$_STAT_VER_LINUX cpath stat)))
	  (unless (zerop result)
	    (error "Unable to stat file: ~S" path))
	  (decode-stat name stat))))))

(defun get-directory-entries (path)
  "Read all of the directory entries in PATH, returning nodes for
them."
  (let* ((entries (read-directory path))
	 (entries (sort entries #'< :key #'cdr))
	 (entries (iter (for entry in entries)
			(for name = (car entry))
			(collect (stat-entry path name)))))
    (sort entries #'string< :key #'node-name)))

(defun symlink-info (path &optional (bufsize 256))
  (ccl:with-cstrs ((cpath path))
    (do ((bufsize bufsize (* bufsize 2)))
	(nil)
      (ccl:%stack-block ((buffer bufsize))
	(let ((result (#_readlink cpath buffer bufsize)))
	(when (minusp result)
	  (error "Unable to read symlink: ~S" path))
	(when (< result bufsize)
	  (return-from symlink-info
	    (ccl:%str-from-ptr buffer result))))))))

;;; Open a posixy file for reading, trying to not modify the atime if
;;; possible.
(defun open-fd-for-read (path)
  (ccl:with-cstrs ((cpath path))
    (let ((result (#_open cpath (logior #$O_RDONLY #$O_NOATIME))))
      (when (minusp result)
	(setf result (#_open cpath #$O_RDONLY)))
      (when (minusp result)
	(error "Unable to open file for reading: ~S" path))
      result)))

(defun for-file-contents (path handler &optional (buffer-size (* 256 1024)))
  "Read the contents of the file in BUFFER-SIZE chunks, calling
HANDLER with a freshly allocated buffer for each."
  (let* ((fd (open-fd-for-read path))
	 (stream (ccl::make-fd-stream fd :direction :input
				      :element-type '(unsigned-byte 8)
				      :basic t
				      :sharing :lock
				      :interactive nil
				      :encoding :iso-8859-1)))
    (unwind-protect
	 (iter (for buffer = (make-byte-vector buffer-size))
	       (for count = (read-sequence buffer stream))
	       (until (zerop count))
	       (funcall handler (if (= count buffer-size) buffer
				    (subseq buffer 0 count))))
      (close stream))))

;;; TODO SUID stuff.
