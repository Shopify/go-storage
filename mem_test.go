package storage_test

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/sajari/storage"
)

func TestMemOpen(t *testing.T) {
	m := storage.Mem()
	_, err := m.Open(nil, "")
	if err == nil {
		t.Errorf("expected 'not found' error from m.Open()")
	}
	if !storage.IsNotExist(err) {
		t.Errorf("IsNotExist(%v) = false, expected true", err)
	}
}

func TestMemCreate(t *testing.T) {
	testMemCreate(t, "testing")
}

func TestMemCreateEmpty(t *testing.T) {
	testMemCreate(t, "")
}

func testMemCreate(t *testing.T, content string) {
	path := "test.txt"

	m := storage.Mem()
	wc, err := m.Create(nil, path)
	if err != nil {
		t.Errorf("unexpected error from m.Create(): %v", err)
	}

	if _, err := io.WriteString(wc, content); err != nil {
		t.Errorf("unexpected error from wc.Write(): %v", err)
	}

	if err := wc.Close(); err != nil {
		t.Errorf("unexpected error from wc.Close(): %v", err)
	}

	f, err := m.Open(nil, path)
	if err != nil {
		t.Errorf("unexpected error from m.Open(): %v", err)
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("unexpected error from ioutil.ReadAll(): %v", err)
	}

	got := string(b)
	if got != content {
		t.Errorf("ioutil.ReadAll() = %q, expected %q", got, content)
	}

	if err := f.Close(); err != nil {
		t.Errorf("unexpected error from f.Close(): %v", err)
	}
}

func TestMemDelete(t *testing.T) {
	path := "test.txt"

	m := storage.Mem()
	wc, err := m.Create(nil, path)
	if err != nil {
		t.Errorf("unexpected error from m.Create(): %v", err)
	}

	if err := wc.Close(); err != nil {
		t.Errorf("unexpected error from wc.Close(): %v", err)
	}

	if err := m.Delete(nil, path); err != nil {
		t.Errorf("unexpected error from m.Delete(%q): %v", path, err)
	}

	_, err = m.Open(nil, path)
	if err == nil {
		t.Error("expected 'not found' error from m.Open()")
	}
	if !storage.IsNotExist(err) {
		t.Errorf("IsNotExist(%v) = false, expected true", err)
	}
}
