package cluster

import (
	"strconv"
	"strings"
)

// IsString is a provided string a string literal?  "hello world"  OR 'hello world'
func (cn *ClusterNode) IsString(str string) bool {
	switch {
	case strings.HasPrefix(str, "\"") && strings.HasSuffix(str, "\""): // has " and "
		return true
	case strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'"): // has ' and '
		return true
	default:
		return false
	}
}

// isArray is a provided value a JSON array?
func (cn *ClusterNode) isArray(str string) bool {
	switch {
	case strings.HasPrefix(str, "[") && strings.HasSuffix(str, "]"):
		return true
	default:
		return false
	}
}

// IsInt is a provided int an int?
func (cn *ClusterNode) IsInt(str string) bool {
	if _, err := strconv.Atoi(str); err == nil { // Atoi because, why not?
		return true
	}
	return false
}

// IsFloat is a provided float a float64?
func (cn *ClusterNode) IsFloat(str string) bool {
	if _, err := strconv.ParseFloat(str, 64); err == nil {
		return true
	}
	return false
}

// IsBool is a provided bool a bool?
func (cn *ClusterNode) IsBool(str string) bool {
	if _, err := strconv.ParseBool(str); err == nil {
		return true
	}
	return false
}
