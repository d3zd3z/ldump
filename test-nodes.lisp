(defpackage #:ldump.test.nodes
  (:use #:cl #:iterate #:ldump.nodes #:ldump.pool #:ldump.file-pool
	#:ldump.chunk #:ldump.test)
  (:import-from #:babel #:string-to-octets #:octets-to-string))
(in-package #:ldump.test.nodes)

(defparameter *sample-node*
  (cl-base64:base64-string-to-usb8-array
   "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPG5vZGUga2lu
ZD0iRElSIj48ZW50cnkga2V5PSJjaGlsZHJlbiI+MGY4NDcyMTE5MzYzYWM5N2Q5
NGEwMTM2M2Q4YzEzZmNkODI2ZDg3YzwvZW50cnk+PGVudHJ5IGtleT0iY3RpbWUi
PjEzMjgwNzcwMTEuNTk1NTYxMTI3PC9lbnRyeT48ZW50cnkga2V5PSJkZXYiPjIx
PC9lbnRyeT48ZW50cnkga2V5PSJnaWQiPjEwMDA8L2VudHJ5PjxlbnRyeSBrZXk9
ImlubyI+Nzg4MTgxPC9lbnRyeT48ZW50cnkga2V5PSJtb2RlIj40OTM8L2VudHJ5
PjxlbnRyeSBrZXk9Im10aW1lIj4xMzI4MDc3MDExLjU5NTU2MTEyNzwvZW50cnk+
PGVudHJ5IGtleT0ibmxpbmsiPjE8L2VudHJ5PjxlbnRyeSBrZXk9InNpemUiPjE4
PC9lbnRyeT48ZW50cnkga2V5PSJ1aWQiPjEwMDA8L2VudHJ5Pjwvbm9kZT4=")
  "A backup node (in XML form).")

(defparameter *sample-back-node*
  (cl-base64:base64-string-to-usb8-array
   "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiIHN0YW5kYWxvbmU9
Im5vIj8+CjwhRE9DVFlQRSBwcm9wZXJ0aWVzIFNZU1RFTSAiaHR0cDovL2phdmEu
c3VuLmNvbS9kdGQvcHJvcGVydGllcy5kdGQiPgo8cHJvcGVydGllcz4KPGNvbW1l
bnQ+QmFja3VwPC9jb21tZW50Pgo8ZW50cnkga2V5PSJfZGF0ZSI+MTMyODA3NzAy
NjIxNzwvZW50cnk+CjxlbnRyeSBrZXk9ImtpbmQiPnNuYXBzaG90PC9lbnRyeT4K
PGVudHJ5IGtleT0iYmFzZSI+dGVzdDwvZW50cnk+CjxlbnRyeSBrZXk9Imhhc2gi
PmQ4ZDZiZTIwNzI2YjBiZDlhYjUzOThkYTg2NWRmMTJlODY4NDUxYWQ8L2VudHJ5
Pgo8L3Byb3BlcnRpZXM+Cg==")
  "A 'back' node in XML form.")

(defun to-node-and-back (kind payload)
  (let ((node (ldump.nodes::decode-kind kind payload)))
    (ldump.nodes::encode-node node)))

(in-package #:ldump.test)

(deftestsuite test-nodes () ())
(pushnew 'test-nodes *test-suites*)

(addtest fs-node-roundtrip
  (ensure-null (mismatch ldump.test.nodes::*sample-node*
			 (ldump.test.nodes::to-node-and-back
			  :|node| ldump.test.nodes::*sample-node*))))

;;; The backup nodes are a little harder to test, since the round-trip
;;; doesn't quite work.  A little trick helps, though, and it does
;;; catch many errors.  The main thing missed is if information is
;;; lost.
(addtest backup-node-roundtrip
  (let* ((a (ldump.test.nodes::to-node-and-back :|back| ldump.test.nodes::*sample-back-node*))
	 (b (ldump.test.nodes::to-node-and-back :|back| a)))
    (ensure-null (mismatch a b))))
