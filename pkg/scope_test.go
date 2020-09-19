package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScope_Has(t *testing.T) {
	tests := []struct {
		name string
		s1   Scope
		s2   Scope
		has  bool
	}{
		{name: "empty", has: true},
		{name: "empty,read", s2: ScopeRead, has: false},
		{name: "read,empty", s1: ScopeRead, has: true},
		{name: "read,read", s1: ScopeRead, s2: ScopeRead, has: true},
		{name: "read+write,read", s1: ScopeRead | ScopeWrite, s2: ScopeRead, has: true},
		{name: "read,read+write", s1: ScopeRead, s2: ScopeRead | ScopeWrite, has: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.has, tt.s1.Has(tt.s2))
		})
	}
}

func TestScope_String(t *testing.T) {
	tests := []struct {
		want string
		s    Scope
	}{
		{want: "none"},
		{want: "read", s: ScopeRead},
		{want: "read,write", s: ScopeRead | ScopeWrite},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			require.Equal(t, tt.want, tt.s.String())
		})
	}
}
