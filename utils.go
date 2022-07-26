package main

import "reflect"

// Get reflect type by generic type.
// see https://github.com/golang/go/issues/50741
func typeOf[T any]() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}
