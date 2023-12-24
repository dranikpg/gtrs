package gtrsconvert

import (
	"reflect"
	"regexp"
	"strings"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// Get reflect type by generic type.
// see https://github.com/golang/go/issues/50741 for a better solution in the future
func typeOf[T any]() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}

// getFieldNameFromType will either use the snake case of the field name, or the gtrs tag
func getFieldNameFromType(fieldType reflect.StructField) string {
	fieldTag := fieldType.Tag
	gtrsTag := strings.TrimSpace(fieldTag.Get("gtrs"))
	nameItem := strings.SplitN(gtrsTag, ",", 2)[0]
	var fieldName string
	if len(nameItem) > 0 {
		fieldName = nameItem
	} else {
		fieldName = toSnakeCase(fieldType.Name)
	}
	return fieldName
}
