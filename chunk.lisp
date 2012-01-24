;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Backup chunks.
;;;
;;; Chunks are the fundamental unit of backup.  Everything is made out
;;; of, or broken up into chunks.
;;;
;;; Each chunk consists of a 4-character kind field (4 8-bit
;;; characters, meaning they should really only be 7-bit characters to
;;; avoid encoding problems), and zero or more bytes of data.
;;;
;;; Chunks inherently support compression of their payload, and be
;;; handled in both compressed and uncompressed form.  Generally, the
;;; uncompressed format will represent the "real" backup data, and the
;;; compressed version will be used for network transfer or the
;;; storage pool.

(defpackage #:ldump.chunk
  (:use #:cl #:iterate #:ldump #:db-zlib #:ldump.pack)
  (:import-from #:babel #:string-to-octets #:octets-to-string)
  (:import-from #:hashlib #:sha1-objects)
  (:import-from #:alexandria #:remove-from-plist)
  (:export #:chunk #:chunk-hash #:chunk-data #:chunk-data-length
	   #:chunk-zdata #:chunk-type #:chunk-type-octets

	   #:chunk-file #:open-chunk-file #:chunk-file-close
	   #:write-chunk #:read-chunk
	   #:chunk-file-length

	   #:chunk-file-path

	   #:make-string-chunk
	   #:make-byte-vector-chunk

	   #:make-test-chunk))
(in-package #:ldump.chunk)

(defclass chunk ()
  ((type :initarg :type :reader chunk-type
	 :type keyword)
   (hash :initarg :hash
	 :type (byte-vector 20))
   (data :initarg :data
	 :type byte-vector)
   (data-length :initarg :data-length
		:type integer)
   (zdata :initarg :zdata
	  :type (or null byte-vector))))

(defmethod shared-initialize :after ((instance chunk) slots
				      &rest initargs &key type &allow-other-keys)
  (declare (ignorable initargs slots))
  (unless (slot-boundp instance 'type)
    (error "Chunk must have a type"))
  (check-type (slot-value instance 'type) keyword)
  (let ((packed-type (string-to-octets (symbol-name type))))
    (check-type packed-type (byte-vector 4)))

  ;; One or data or zdata must be bound.
  (or (slot-boundp instance 'data)
      (and (slot-boundp instance 'zdata)
	   (slot-boundp instance 'data-length))
      ;; We need the uncompressed size in order to uncompress the data
      ;; as well.
      (error "Chunk must give either uncompressed data or compressed and size")))

(defun chunk-type-octets (chunk)
  "Returns the chunk type as an octet vector."
  (string-to-octets (symbol-name (chunk-type chunk))))

(defun octets-to-type (bytes)
  (intern (octets-to-string bytes) "KEYWORD"))

(defgeneric chunk-data (chunk)
  (:documentation
   "Retrieve (or compute) the data payload associated with this chunk"))
(define-memoized-accessor chunk-data (chunk chunk) data
  (with-slots (zdata data-length) chunk
    (let* ((dest (make-byte-vector data-length))
	   (dest-len (uncompress dest zdata)))
      (unless (= data-length dest-len)
	(error "Chunk uncompression error"))
      dest)))

(defgeneric chunk-data-length (chunk))
(define-memoized-accessor chunk-data-length (chunk chunk) data-length
  (length (chunk-data chunk)))

(defgeneric chunk-hash (chunk))
(define-memoized-accessor chunk-hash (chunk chunk) hash
  (sha1-objects (chunk-type chunk)
		(chunk-data chunk)))

(defgeneric chunk-zdata (chunk))
(define-memoized-accessor chunk-zdata (chunk chunk) zdata
  (let* ((src-len (chunk-data-length chunk))
	 (dest (make-byte-vector src-len))
	 (dest-len (compress dest (chunk-data chunk)
			     :overflow-error-p nil)))
    (cond ((null dest-len) nil)
	  ((= src-len dest-len) dest)
	  (t (subseq dest 0 dest-len)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Chunks based on strings of data, encoded nicely in UTF-8.

(defun make-string-chunk (type data)
  (make-instance 'chunk
		 :type type
		 :data (string-to-octets data)))

(defun make-byte-vector-chunk (type data)
  "Make a chunk out of the given DATA.  The DATA should be a vector of
8-bit unsigned bytes, and should not be displaed.  It will not be
copied, so should not be changed as long as the chunk created is
accessible."
  (check-type data byte-vector "A vector of unsigned 8-bit bytes")
  (assert (not (array-displacement data))
	  (data)
	  "Data for byte-chunks should not be displaced")
  (make-instance 'chunk
		 :type type
		 :data data
		 :data-length (length data)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; I/O of chunks to files.  A chunk file supports writing 0 or more
;;; chunks to a named file.  Each chunk is written at a given offset.
;;; We also provide theh ability to recover some problems that happen
;;; with truncated files.

(defstruct (chunk-file
	     (:constructor %make-chunk-file))
  path stream write-position seeked)

(defun open-chunk-file (path)
  "Open a new or already-existing chunk file.  If the file does not
exist, it will not be created until a chunk is written to it."
  (%make-chunk-file :path path))

(defun prepare-write (cfile)
  "Make sure that this chunk file is opened in such a way that we can
write to it."
  (with-accessors ((path chunk-file-path)
		   (stream chunk-file-stream)
		   (write-position chunk-file-write-position)
		   (seeked chunk-file-seeked))
      cfile
    (when (and (streamp stream)
	       (not (output-stream-p stream)))
      (setf seeked nil)
      (when (streamp stream)
	(close stream)
	(setf stream nil)))
    (unless (streamp stream)
      (setf stream (open path :direction :io
			 :element-type '(unsigned-byte 8)
			 :if-exists :append
			 :if-does-not-exist :create)
	    write-position (file-length stream)
	    seeked nil))
    (unless seeked
      (file-position stream write-position)
      (setf seeked t))))

(defun prepare-read (cfile offset)
  "Make sure that the chunk file is opened in such a way that we can
read from it, and position the file pointer to OFFSET."
  (with-accessors ((path chunk-file-path)
		   (stream chunk-file-stream)
		   (seeked chunk-file-seeked))
      cfile
    ;; This is a little easier than write, since we always open the
    ;; file in a mode that can be read from.
    (unless (streamp stream)
      (setf stream (open path :direction :input
			 :element-type '(unsigned-byte 8))))
    (setf seeked nil)
    ;; TODO: Could use a read position, instead of always seeking.
    (file-position stream offset)))

(defun chunk-file-close (cfile)
  (with-accessors ((stream chunk-file-stream)) cfile
    (when (streamp stream)
      (close stream)
      (setf stream nil))))

(defun chunk-file-flush (cfile)
  "Make sure any output has been written."
  (with-accessors ((stream chunk-file-stream)) cfile
    (and (streamp stream)
	 (output-stream-p stream)
	 (finish-output stream))))

(defun chunk-file-length (cfile)
  "Return the length of the given chunk file.  This both corresponds
to the current length of the file, and should also be the next
offset that data will be written."
  (with-accessors ((stream chunk-file-stream)
		   (write-position chunk-file-write-position))
      cfile
    (cond ((and (streamp stream)
		(output-stream-p stream))
	   write-position)
	  ((and (streamp stream)
		(input-stream-p stream))
	   (file-length stream))
	  (t
	   (prepare-read cfile 0)
	   (file-length stream)))))

(defun chunk-file-opened-p (cfile)
  "Is the file underlying this chunk opened?"
  (streamp (chunk-file-stream cfile)))

(defparameter *chunk-magic*
  (string-to-octets (concatenate 'string
				 "adump-pool-v1.1"
				 (string #\Newline))))

(defparameter *chunk-padding*
  (make-byte-vector 16 :initial-element 0))

;;;; The chunk header:
;;;;  offset  length   field
;;;;       0      16   *chunk-magic*
;;;;      16       4   compressed length, amount stored in file.
;;;;      20       4   uncompressed length, or -1 for data not compressed.
;;;;      24       4   type
;;;;      28      20   sha1 hash  of   type + uncompressed-data
;;;;      48    clen   data.
;;;;            0-15   padding
;;;;
;;;; The numbers are always represented in little endian, and the
;;;;  whole chunk is padded to a multiple of 16 bytes.

(defun write-chunk (cfile chunk)
  "Write the chunk to the chunk file.  Returns the offset that the
data was written to."
  (prepare-write cfile)
  (with-accessors ((stream chunk-file-stream)
		   (write-position chunk-file-write-position))
      cfile
    (let ((pos write-position)
	  (header (make-byte-vector 48))
	  (zdata (chunk-zdata chunk))
	  (data-len (chunk-data-length chunk)))
      (unless zdata
	(setf zdata (chunk-data chunk)
	      data-len -1))
      (replace header *chunk-magic*)
      (pack-le-integer header 16 (length zdata) 4)
      (pack-le-integer header 20 data-len 4)
      (replace header (chunk-type-octets chunk) :start1 24)
      (replace header (chunk-hash chunk) :start1 28)
      (write-sequence header stream)
      (incf write-position 48)

      ;; Write the data payload itself.
      (write-sequence zdata stream)
      (incf write-position (length zdata))

      ;; Plus any padding.
      (let ((padding (logand 15 (- (length zdata)))))
	(when (plusp padding)
	  (write-sequence *chunk-padding* stream :end padding)
	  (incf write-position padding)))

      ;; As a sanity check, make sure things are where they should
      ;; be.
      (assert (= write-position (file-position stream))
	      (write-position)
	      "File position in unpected place ~A (expecting ~A)"
	      (file-position stream)
	      write-position)

      pos)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Reading chunks.

(define-condition chunk-read-error (error)
  ((reason :initform "General" :reader chunk-read-error-reason
	   :allocation :class)
   (detail :initform "" :initarg :detail
	   :reader chunk-read-error-detail))
  (:report (lambda (condition stream)
	     (format stream "Error reading chunk from file: ~A (~A)"
		     (chunk-read-error-reason condition)
		     (chunk-read-error-detail condition)))))

(define-condition chunk-short-read-error (chunk-read-error)
  ((reason :initform "Short read on input file")))

(define-condition chunk-corrupt-error (chunk-read-error)
  ((reason :initform "Chunk is corrupt")))

(defun read-chunk (cfile offset &key (verify-hash nil))
  "Read a chunk from the chunk file at the given offset.  May signal
chunk-read-error if unable to read a chunk for some reason.

If VERIFY-HASH is true, then the hash of the read data will be
computed, and an error signalled if there is a mismatch."
  (prepare-read cfile offset)
  (with-accessors ((stream chunk-file-stream)) cfile
    (let* ((header (read-new-sequence stream 48 :on-failure-raise 'chunk-short-read-error))
	   (magic (subseq header 0 16))
	   (zlength (unpack-le-integer header 16 4))
	   (length (unpack-le-integer header 20 4))
	   (type (subseq header 24 28))
	   (keyword-type (octets-to-type type))
	   (hash (subseq header 28 48)))
      (when (mismatch *chunk-magic* magic)
	(error (make-condition 'chunk-corrupt-error
			       :default "Invalid magic")))
      (let* ((payload (read-new-sequence stream zlength
					 :on-failure-raise 'chunk-short-read-error))
	     (chunk (if (= length #xFFFFFFFF)
			(make-instance 'chunk
				       :type keyword-type
				       :hash hash
				       :zdata nil
				       :data payload)
			(make-instance 'chunk
				       :type keyword-type
				       :hash hash
				       :zdata payload
				       :data-length length))))
	(when verify-hash
	  ;; Discard the hash in the new chunk, and force it to be
	  ;; recomputed.
	  (slot-makunbound chunk 'hash)
	  (when (mismatch (chunk-hash chunk) hash)
	    (error (make-condition 'chunk-corrupt-error
				   :detail "Hash verification error"))))
	chunk))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defparameter *word-list*
  (coerce '("the" "be" "to" "of" "and" "a"
	    "in" "that" "have" "I" "it" "for" "not" "on" "with"
	    "he" "as" "you" "do" "at" "this" "but" "his" "by"
	    "from" "they" "we" "say" "her" "she" "or" "an" "will"
	    "my" "one" "all" "would" "there" "their" "what" "so"
	    "up" "out" "if" "about" "who" "get" "which" "go" "me"
	    "when" "make" "can" "like" "time" "no" "just" "him"
	    "know" "take" "person" "into" "year" "your" "good"
	    "some" "could" "them" "see" "other" "than" "then"
	    "now" "look" "only" "come" "its" "over" "think" "also")
	  'vector)
  "A list of words to generate random blocks over")

(defstruct simple-random-state index)
(defvar *simple-random-state* (make-simple-random-state :index 1))
(defun simple-random (limit &optional (state *simple-random-state*))
  "Return the \"next\" number from a simple random number generator."
  (let* ((cur (simple-random-state-index state))
	 (next (logand (+ (* cur 1103515245) 12345)
		       #xFFFFFFFF)))
    (setf (simple-random-state-index state) next)
    (mod cur limit)))

(defun random-word ()
  "Generate a word ranodmly, using the simple ranodm."
  (elt *word-list* (simple-random (length *word-list*))))

(defun make-random-string (size index)
  (let* ((data (make-sequence 'string size
			      :initial-element #\Space))
	 (message (format nil "~S-~S" index size)))
    (replace data message)
    (let ((*simple-random-state*
	   (make-simple-random-state :index index)))
      ;; Consume a random number, so everything doesn't start with the obvious word choice from the list.
      (simple-random 1)
      (do ((pos (1+ (length message))
		(+ 1 pos (length word)))
	   (word (random-word) (random-word)))
	  ((>= pos size))
	(replace data word :start1 pos)))
    data))

(defun make-test-chunk (size index &optional (type :blob))
  (make-instance 'chunk :type type
		 :data (string-to-octets (make-random-string size index))))
