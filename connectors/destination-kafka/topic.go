package main

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

const maxTopicLength = 249

// topicName fills {namespace} and {stream} of pattern and converts the result
// as Airbyte's destination-kafka does: accents are dropped, whitespace runs
// become one underscore and every other character outside [A-Za-z0-9_]
// becomes an underscore, so "{namespace}.{stream}" gives "public_users".
// Whitespace is Java's \s: space, \t, \n, \v, \f and \r.
func topicName(pattern, namespace, stream string) (string, error) {
	name := strings.NewReplacer("{namespace}", namespace, "{stream}", stream).Replace(pattern)
	name = alphanumericAndUnderscore(name)
	if err := validTopic(name); err != nil {
		return "", fmt.Errorf("topic %q of stream %s: %w", name, streamLabel(namespace, stream), err)
	}
	return name, nil
}

func alphanumericAndUnderscore(s string) string {
	var b strings.Builder
	space := false
	for _, r := range norm.NFKD.String(s) {
		switch {
		case unicode.Is(unicode.M, r):
			continue
		case strings.ContainsRune(" \t\n\v\f\r", r):
			if !space {
				b.WriteByte('_')
			}
			space = true
			continue
		case r < unicode.MaxASCII && (r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'):
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		space = false
	}
	return b.String()
}

// validTopic reports whether Kafka accepts name as a topic name.
func validTopic(name string) error {
	switch {
	case name == "":
		return errors.New("topic name is empty")
	case name == "." || name == "..":
		return fmt.Errorf("topic name %q is not allowed", name)
	case len(name) > maxTopicLength:
		return fmt.Errorf("topic name is %d characters long, more than Kafka's %d", len(name), maxTopicLength)
	}
	for _, r := range name {
		if r != '.' && r != '_' && r != '-' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return fmt.Errorf("topic name %q has %q; Kafka allows only letters, digits, '.', '_' and '-'", name, r)
		}
	}
	return nil
}

func streamLabel(namespace, stream string) string {
	if namespace == "" {
		return stream
	}
	return namespace + "." + stream
}
