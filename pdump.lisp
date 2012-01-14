;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Dumping utility.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Output a sequences of bytes in a nice pretty hexified format.
;;;; For bytes that represent characters, also print the character
;;;; representation of the byte as well.

(in-package #:ldump)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; In general, this could output bytes of any desired width, but
;;; let's start with the simple case of 8-bit unsigned bytes.

(defun ascii-char-p (ch)
  (char<= #\Space ch #\~))

(defun pdump (data &optional (stream t))
  "Print out the contents of DATA, which should be an array of
unsigned-bytes."
  (let ((hex (make-array 49 :element-type 'character :fill-pointer 0))
	(ascii (make-array 16 :element-type 'character :fill-pointer 0))
	(total-count 0)
	(count 0))
    (labels ((reset ()
	       (setf (fill-pointer hex) 0
		     (fill-pointer ascii) 0
		     count 0))
	     (ship ()
	       (format stream "~6,'0x ~49a |~16a|~%" total-count
		       hex ascii)
	       (reset)
	       (incf total-count 16))
	     (add-byte (value)
	       (when (= count 16)
		 (ship))
	       (when (= count 8)
		 (vector-push #\Space hex))
	       (format hex " ~2,'0x" value)
	       (let ((ch (code-char value)))
		 (vector-push (if (ascii-char-p ch) ch #\.)
			      ascii))
	       (incf count)))
      (map nil #'add-byte data)
      (when (plusp count)
	(ship)))))
