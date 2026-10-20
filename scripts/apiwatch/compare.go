package apiwatch

import (
	"strings"
	"unicode"
)

// compareVersions orders the version formats connectors pin: v26.0, v26,
// 2026-10, 202610, 2025-08-27.basil, 2.11, v1beta. Digit runs compare as
// numbers, letter runs as text, and a pre-release word (alpha, beta, rc,
// preview) sorts before the same version without it.
func compareVersions(a, b string) int {
	ta, tb := tokens(a), tokens(b)

	for i := 0; i < len(ta) || i < len(tb); i++ {
		switch {
		case i == len(ta):
			if preRelease(tb[i]) {
				return 1
			}

			return -1
		case i == len(tb):
			if preRelease(ta[i]) {
				return -1
			}

			return 1
		}

		if c := compareToken(ta[i], tb[i]); c != 0 {
			return c
		}
	}

	return 0
}

func compareToken(a, b string) int {
	if isDigits(a) && isDigits(b) {
		a, b = strings.TrimLeft(a, "0"), strings.TrimLeft(b, "0")
		if len(a) != len(b) {
			if len(a) < len(b) {
				return -1
			}

			return 1
		}

		return strings.Compare(a, b)
	}

	if ra, rb := preRank(a), preRank(b); ra != 0 && rb != 0 {
		return ra - rb
	}

	return strings.Compare(a, b)
}

func tokens(s string) []string {
	var (
		out []string
		cur []rune
	)

	flush := func() {
		if len(cur) > 0 {
			out = append(out, string(cur))
			cur = cur[:0]
		}
	}

	for _, r := range strings.ToLower(s) {
		switch {
		case !unicode.IsLetter(r) && !unicode.IsDigit(r):
			flush()
		case len(cur) > 0 && unicode.IsDigit(r) != unicode.IsDigit(cur[0]):
			flush()

			cur = append(cur, r)
		default:
			cur = append(cur, r)
		}
	}

	flush()

	return out
}

func isDigits(s string) bool {
	return s != "" && strings.IndexFunc(s, func(r rune) bool { return !unicode.IsDigit(r) }) < 0
}

func preRank(s string) int {
	return map[string]int{"dev": 1, "alpha": 2, "beta": 3, "preview": 4, "rc": 5}[s]
}

func preRelease(s string) bool { return preRank(s) != 0 }
