/*
 * tenant.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// FoundationDB Go API

package fdb

// #define FDB_API_VERSION 620
// #include <foundationdb/fdb_c.h>
// #include <stdlib.h>
import "C"

import (
	"errors"
)

var (
	ErrFeatureNotSupported = errors.New("this feature is only supported in FDB 7+")
)

// CreateTenant creates a new tenant in the cluster. The tenant name cannot
// start with the \xff byte.
func (t Transaction) CreateTenant(name KeyConvertible) error {
	return ErrFeatureNotSupported
}

// CreateTenant creates a new tenant in the cluster. The tenant name cannot
// start with the \xff byte.
func (d Database) CreateTenant(name KeyConvertible) error {
	return ErrFeatureNotSupported
}

// DeleteTenant deletes an existing tenant in the cluster.
func (t Transaction) DeleteTenant(name KeyConvertible) error {
	return ErrFeatureNotSupported
}

// DeleteTenant creates a new tenant in the cluster. The tenant name cannot
// start with the \xff byte.
func (d Database) DeleteTenant(name KeyConvertible) error {
	return ErrFeatureNotSupported
}

// ListTenants lists all existings tenants in the cluster.
func (t Transaction) ListTenants() ([]Key, error) {
	return nil, ErrFeatureNotSupported
}

// CreateTenant creates a new tenant in the cluster. The tenant name cannot
// start with the \xff byte.
func (d Database) ListTenants() ([]Key, error) {
	return nil, ErrFeatureNotSupported
}

func (t Transaction) checkTenantExist(tenantPath Key) (bool, error) {
	return false, ErrFeatureNotSupported
}

// Tenant is a handle to a FoundationDB tenant. Tenant is a lightweight
// object that may be efficiently copied, and is safe for concurrent use by
// multiple goroutines.
type Tenant struct {
}

// OpenTenant returns a tenant handle identified by the given name. All transactions
// created by this tenant will operate on the tenant’s key-space.
func (d Database) OpenTenant(name KeyConvertible) (Tenant, error) {
	return Tenant{}, ErrFeatureNotSupported
}

// CreateTransaction returns a new FoundationDB transaction. It is generally
// preferable to use the (Tenant).Transact method, which handles
// automatically creating and committing a transaction with appropriate retry
// behavior.
func (t Tenant) CreateTransaction() (Transaction, error) {
	return Transaction{}, ErrFeatureNotSupported
}

// Transact runs a caller-provided function inside a retry loop, providing it
// with a newly created Transaction. After the function returns, the Transaction
// will be committed automatically. Any error during execution of the function
// (by panic or return) or the commit will cause the function and commit to be
// retried or, if fatal, return the error to the caller.
//
// When working with Future objects in a transactional function, you may either
// explicitly check and return error values using Get, or call MustGet. Transact
// will recover a panicked Error and either retry the transaction or return the
// error.
//
// The transaction is retried if the error is or wraps a retryable Error.
// The error is unwrapped.
//
// Do not return Future objects from the function provided to Transact. The
// Transaction created by Transact may be finalized at any point after Transact
// returns, resulting in the cancellation of any outstanding
// reads. Additionally, any errors returned or panicked by the Future will no
// longer be able to trigger a retry of the caller-provided function.
//
// See the Transactor interface for an example of using Transact with
// Transaction and Database objects.
func (t Tenant) Transact(f func(Transaction) (interface{}, error)) (interface{}, error) {
	return nil, ErrFeatureNotSupported
}

// ReadTransact runs a caller-provided function inside a retry loop, providing
// it with a newly created Transaction (as a ReadTransaction). Any error during
// execution of the function (by panic or return) will cause the function to be
// retried or, if fatal, return the error to the caller.
//
// When working with Future objects in a read-only transactional function, you
// may either explicitly check and return error values using Get, or call
// MustGet. ReadTransact will recover a panicked Error and either retry the
// transaction or return the error.
//
// The transaction is retried if the error is or wraps a retryable Error.
// The error is unwrapped.
//
// Do not return Future objects from the function provided to ReadTransact. The
// Transaction created by ReadTransact may be finalized at any point after
// ReadTransact returns, resulting in the cancellation of any outstanding
// reads. Additionally, any errors returned or panicked by the Future will no
// longer be able to trigger a retry of the caller-provided function.
//
// See the ReadTransactor interface for an example of using ReadTransact with
// Transaction, Snapshot and Database objects.
func (t Tenant) ReadTransact(f func(ReadTransaction) (interface{}, error)) (interface{}, error) {
	return nil, ErrFeatureNotSupported
}
