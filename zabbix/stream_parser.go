/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mike Trinkala (trink@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package plugins

// Copy from heka 0.8 to get plugins going.

import (
	"bytes"
	"io"

	"github.com/mozilla-services/heka/message"
)

// StreamParser interface to read a spilt a stream into records
type StreamParser interface {
	// Finds the next record in the stream.
	// Returns the number of bytes read from the stream. This will not always
	// correlate to the record size since delimiters can be discarded and data
	// corruption skipped which also means the record could empty even if bytes
	// were read. 'record' will remain valid until the next call to Parse.
	// bytesRead can be non zero even in an error condition i.e. ErrShortBuffer
	Parse(reader io.Reader) (bytesRead int, record []byte, err error)

	// Retrieves the remainder of the parse buffer.  This is the
	// only way to fetch the last record in a stream that specifies a start of
	// line  delimiter or contains a partial last line.  It should only be
	// called when at the EOF and no additional data will be appended to
	// the stream.
	GetRemainingData() []byte

	// Sets the internal buffer to at least 'size' bytes.
	SetMinimumBufferSize(size int)
}

// Internal buffer management for the StreamParser
type streamParserBuffer struct {
	buf      []byte
	readPos  int
	scanPos  int
	needData bool
	err      string
}

func newStreamParserBuffer() (s *streamParserBuffer) {
	s = new(streamParserBuffer)
	s.buf = make([]byte, 1024*8)
	s.needData = true
	return
}

func (s *streamParserBuffer) GetRemainingData() (record []byte) {
	if s.readPos-s.scanPos > 0 {
		record = s.buf[s.scanPos:s.readPos]
	}
	s.scanPos = 0
	s.readPos = 0
	return
}

func (s *streamParserBuffer) SetMinimumBufferSize(size int) {
	if cap(s.buf) < size {
		newSlice := make([]byte, size)
		copy(newSlice, s.buf)
		s.buf = newSlice
	}
	return
}

func (s *streamParserBuffer) read(reader io.Reader) (n int, err error) {
	if cap(s.buf)-s.readPos <= 1024*4 {
		if s.scanPos == 0 { // line will not fit in the current buffer
			newSize := cap(s.buf) * 2
			if newSize > int(message.MAX_RECORD_SIZE) {
				if cap(s.buf) == int(message.MAX_RECORD_SIZE) {
					if s.readPos == cap(s.buf) {
						s.scanPos = 0
						s.readPos = 0
						return cap(s.buf), io.ErrShortBuffer
					} else {
						newSize = 0 // don't allocate any more memory, just read into what is left
					}
				} else {
					newSize = int(message.MAX_RECORD_SIZE)
				}
			}
			if newSize > 0 {
				s.SetMinimumBufferSize(newSize)
			}
		} else { // reclaim the space at the beginning of the buffer
			copy(s.buf, s.buf[s.scanPos:s.readPos])
			s.readPos, s.scanPos = s.readPos-s.scanPos, 0
		}
	}
	n, err = reader.Read(s.buf[s.readPos:])
	return
}

// Byte delimited line parser
type TokenParser struct {
	*streamParserBuffer
	delimiter byte
}

func NewTokenParser() (t *TokenParser) {
	t = new(TokenParser)
	t.streamParserBuffer = newStreamParserBuffer()
	t.delimiter = '\n'
	return
}

func (t *TokenParser) Parse(reader io.Reader) (bytesRead int, record []byte, err error) {
	if t.needData {
		if bytesRead, err = t.read(reader); err != nil {
			return
		}
	}
	t.readPos += bytesRead

	bytesRead, record = t.findRecord(t.buf[t.scanPos:t.readPos])
	t.scanPos += bytesRead
	if len(record) == 0 {
		t.needData = true
	} else {
		if t.readPos == t.scanPos {
			t.readPos = 0
			t.scanPos = 0
			t.needData = true
		} else {
			t.needData = false
		}
	}
	return
}

// Sets the byte delimiter to parse on, defaults to a newline.
func (t *TokenParser) SetDelimiter(delim byte) {
	t.delimiter = delim
}

func (t *TokenParser) findRecord(buf []byte) (bytesRead int, record []byte) {
	n := bytes.IndexByte(buf, t.delimiter)
	if n == -1 {
		return
	}
	bytesRead = n + 1 // include the delimiter for backwards compatibility
	record = buf[:bytesRead]
	return
}
