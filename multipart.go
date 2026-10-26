package httpx

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"os"
	"path/filepath"
)

// buildBody turns the recorded body mode into a replayable reader and, for multipart,
// the Content-Type header value to use.
func buildBody(s state) (io.Reader, string, error) {
	switch s.kind {
	case bodyMultipart:
		return buildMultipart(s)
	case bodyJSON, bodyForm:
		if s.bodyBytes != nil {
			return bytes.NewReader(s.bodyBytes), "", nil
		}
	case bodyRaw:
		return s.body, "", nil
	}
	return nil, "", nil
}

func buildMultipart(s state) (io.Reader, string, error) {
	buf := &bytes.Buffer{}
	mw := multipart.NewWriter(buf)

	// Text fields first (some servers require metadata before the file), in sorted order.
	keys := make([]string, 0, len(s.formFields))
	for k := range s.formFields {
		keys = append(keys, k)
	}
	sortStrings(keys)
	for _, k := range keys {
		for _, v := range s.formFields[k] {
			if err := mw.WriteField(k, v); err != nil {
				return nil, "", fmt.Errorf("httpx: failed to write form field %q: %w", k, err)
			}
		}
	}

	for _, f := range s.files {
		file, err := os.Open(f.path)
		if err != nil {
			return nil, "", fmt.Errorf("httpx: failed to open file %q: %w", f.path, err)
		}
		err = addFilePart(mw, f.field, filepath.Base(f.path), mimeFor(f.path), file)
		closeErr := file.Close()
		if err != nil {
			return nil, "", fmt.Errorf("httpx: failed to add file %q: %w", f.path, err)
		}
		if closeErr != nil {
			return nil, "", fmt.Errorf("httpx: failed to close file %q: %w", f.path, closeErr)
		}
	}

	for _, fr := range s.fileReaders {
		if fr.reader == nil {
			return nil, "", fmt.Errorf("httpx: FileReader %q: reader must not be nil", fr.field)
		}
		if fr.filename == "" {
			return nil, "", fmt.Errorf("httpx: FileReader %q: filename must not be empty", fr.field)
		}
		if err := addFilePart(mw, fr.field, fr.filename, fr.contentType, fr.reader); err != nil {
			return nil, "", fmt.Errorf("httpx: failed to add file %q: %w", fr.filename, err)
		}
	}

	if err := mw.Close(); err != nil {
		return nil, "", fmt.Errorf("httpx: failed to finalize multipart body: %w", err)
	}
	return buf, mw.FormDataContentType(), nil
}

func addFilePart(mw *multipart.Writer, field, filename, contentType string, r io.Reader) error {
	disposition := mime.FormatMediaType("form-data", map[string]string{"name": field, "filename": filename})
	if disposition == "" {
		return errors.New("invalid field name or filename")
	}
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", disposition)
	h.Set(headerContentType, contentType)
	part, err := mw.CreatePart(h)
	if err != nil {
		return err
	}
	_, err = io.Copy(part, r)
	return err
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
