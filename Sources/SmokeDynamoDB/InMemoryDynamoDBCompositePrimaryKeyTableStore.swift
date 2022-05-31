// swiftlint:disable cyclomatic_complexity
// Copyright 2018-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// A copy of the License is located at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//  InMemoryDynamoDBCompositePrimaryKeyTableStore.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import CollectionConcurrencyKit

internal class InMemoryDynamoDBCompositePrimaryKeyTableStore {

    internal var store: [String: [String: PolymorphicOperationReturnTypeConvertable]] = [:]
    internal let accessQueue = DispatchQueue(
        label: "com.amazon.SmokeDynamoDB.InMemoryDynamoDBCompositePrimaryKeyTable.accessQueue",
        target: DispatchQueue.global())
    
    internal let executeItemFilter: ExecuteItemFilterType?

    init(executeItemFilter: ExecuteItemFilterType? = nil) {
        self.executeItemFilter = executeItemFilter
    }
    
    func getStore() async -> [String: [String: PolymorphicOperationReturnTypeConvertable]] {
        return await withUnsafeContinuation { continuation in
            accessQueue.async {
                continuation.resume(returning: self.store)
            }
        }
    }

    func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await withUnsafeThrowingContinuation { continuation in
            accessQueue.async {
                let partition = self.store[item.compositePrimaryKey.partitionKey]

                // if there is already a partition
                var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
                if let partition = partition {
                    updatedPartition = partition

                    // if the row already exists
                    if partition[item.compositePrimaryKey.sortKey] != nil {
                        let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: item.compositePrimaryKey.partitionKey,
                                                                              sortKey: item.compositePrimaryKey.sortKey,
                                                                              message: "Row already exists.")
                        
                        continuation.resume(throwing: error)
                        return
                    }

                    updatedPartition[item.compositePrimaryKey.sortKey] = item
                } else {
                    updatedPartition = [item.compositePrimaryKey.sortKey: item]
                }

                self.store[item.compositePrimaryKey.partitionKey] = updatedPartition
                continuation.resume(returning: ())
            }
        }
    }

    func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await withUnsafeThrowingContinuation { continuation in
            accessQueue.async {
                let partition = self.store[item.compositePrimaryKey.partitionKey]

                // if there is already a partition
                var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
                if let partition = partition {
                    updatedPartition = partition

                    updatedPartition[item.compositePrimaryKey.sortKey] = item
                } else {
                    updatedPartition = [item.compositePrimaryKey.sortKey: item]
                }

                self.store[item.compositePrimaryKey.partitionKey] = updatedPartition
                continuation.resume(returning: ())
            }
        }
    }

    func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                              existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await withUnsafeThrowingContinuation { continuation in
            accessQueue.async {
                let partition = self.store[newItem.compositePrimaryKey.partitionKey]

                // if there is already a partition
                var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
                if let partition = partition {
                    updatedPartition = partition

                    // if the row already exists
                    if let actuallyExistingItem = partition[newItem.compositePrimaryKey.sortKey] {
                        if existingItem.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion ||
                            existingItem.createDate.iso8601 != actuallyExistingItem.createDate.iso8601 {
                            let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                                                  sortKey: newItem.compositePrimaryKey.sortKey,
                                                                                  message: "Trying to overwrite incorrect version.")
                            continuation.resume(throwing: error)
                            return
                        }
                    } else {
                        let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                                              sortKey: newItem.compositePrimaryKey.sortKey,
                                                                              message: "Existing item does not exist.")
                        continuation.resume(throwing: error)
                        return
                    }

                    updatedPartition[newItem.compositePrimaryKey.sortKey] = newItem
                } else {
                    let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                                          sortKey: newItem.compositePrimaryKey.sortKey,
                                                                          message: "Existing item does not exist.")
                    continuation.resume(throwing: error)
                    return
                }

                self.store[newItem.compositePrimaryKey.partitionKey] = updatedPartition
                continuation.resume(returning: ())
            }
        }
    }
    
    func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        try await entries.asyncForEach { entry in
            switch entry {
            case .update(new: let new, existing: let existing):
                return try await self.updateItem(newItem: new, existingItem: existing)
            case .insert(new: let new):
                return try await self.insertItem(new)
            case .deleteAtKey(key: let key):
                return await deleteItem(forKey: key)
            case .deleteItem(existing: let existing):
                return try await deleteItem(existingItem: existing)
            }
        }
    }

    func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
    -> TypedDatabaseItem<AttributesType, ItemType>? {
        return try await withUnsafeThrowingContinuation { continuation in
            accessQueue.async {
                if let partition = self.store[key.partitionKey] {

                    guard let value = partition[key.sortKey] else {
                        continuation.resume(returning: nil)
                        return
                    }

                    guard let item = value as? TypedDatabaseItem<AttributesType, ItemType> else {
                        let foundType = type(of: value)
                        let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                        let context = DecodingError.Context(codingPath: [], debugDescription: description)
                        let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                        
                        continuation.resume(throwing: error)
                        return
                    }

                    continuation.resume(returning: item)
                    return
                }

                continuation.resume(returning: nil)
            }
        }
    }
    
    func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
    -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] {
        return try await withUnsafeThrowingContinuation { continuation in
            accessQueue.async {
                var map: [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] = [:]
                
                keys.forEach { key in
                    if let partition = self.store[key.partitionKey] {

                        guard let value = partition[key.sortKey] else {
                            return
                        }
                        
                        let itemAsReturnedType: ReturnedType
                            
                        do {
                            itemAsReturnedType = try self.convertToQueryableType(input: value)
                        } catch {
                            continuation.resume(throwing: error)
                            return
                        }
                        
                        map[key] = itemAsReturnedType
                    }
                }
                
                continuation.resume(returning: map)
            }
        }
    }

    func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) async {
        return await withUnsafeContinuation { continuation in
            accessQueue.async {
                self.store[key.partitionKey]?[key.sortKey] = nil
                continuation.resume(returning: ())
            }
        }
    }
    
    func deleteItem<ItemType: DatabaseItem>(existingItem: ItemType) async throws {
        return try await withUnsafeThrowingContinuation { continuation in
            accessQueue.async {
                let partition = self.store[existingItem.compositePrimaryKey.partitionKey]

                // if there is already a partition
                var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
                if let partition = partition {
                    updatedPartition = partition

                    // if the row already exists
                    if let actuallyExistingItem = partition[existingItem.compositePrimaryKey.sortKey] {
                        if existingItem.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion ||
                        existingItem.createDate.iso8601 != actuallyExistingItem.createDate.iso8601 {
                            let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                                                  sortKey: existingItem.compositePrimaryKey.sortKey,
                                                                                  message: "Trying to delete incorrect version.")
                            
                            continuation.resume(throwing: error)
                            return
                        }
                    } else {
                        let error =  SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                                               sortKey: existingItem.compositePrimaryKey.sortKey,
                                                                               message: "Existing item does not exist.")
                        
                        continuation.resume(throwing: error)
                        return
                    }

                    updatedPartition[existingItem.compositePrimaryKey.sortKey] = nil
                } else {
                    let error =  SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                                           sortKey: existingItem.compositePrimaryKey.sortKey,
                                                                           message: "Existing item does not exist.")
                    
                    continuation.resume(throwing: error)
                    return
                }

                self.store[existingItem.compositePrimaryKey.partitionKey] = updatedPartition
                continuation.resume(returning: ())
            }
        }
    }
    
    func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws {
        await keys.asyncForEach { key in
            return await deleteItem(forKey: key)
        }
    }
    
    func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) async throws {
        try await existingItems.asyncForEach { existingItem in
            try await deleteItem(existingItem: existingItem)
        }
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    consistentRead: Bool) async throws
    -> [ReturnedType] {
        return try await withUnsafeThrowingContinuation { continuation in
            accessQueue.async {
                var items: [ReturnedType] = []

                if let partition = self.store[partitionKey] {
                    let sortedPartition = partition.sorted(by: { (left, right) -> Bool in
                        return left.key < right.key
                    })
                    
                    sortKeyIteration: for (sortKey, value) in sortedPartition {

                        if let currentSortKeyCondition = sortKeyCondition {
                            switch currentSortKeyCondition {
                            case .equals(let value):
                                if !(value == sortKey) {
                                    // don't include this in the results
                                    continue sortKeyIteration
                                }
                            case .lessThan(let value):
                                if !(sortKey < value) {
                                    // don't include this in the results
                                    continue sortKeyIteration
                                }
                            case .lessThanOrEqual(let value):
                                if !(sortKey <= value) {
                                    // don't include this in the results
                                    continue sortKeyIteration
                                }
                            case .greaterThan(let value):
                                if !(sortKey > value) {
                                    // don't include this in the results
                                    continue sortKeyIteration
                                }
                            case .greaterThanOrEqual(let value):
                                if !(sortKey >= value) {
                                    // don't include this in the results
                                    continue sortKeyIteration
                                }
                            case .between(let value1, let value2):
                                if !(sortKey > value1 && sortKey < value2) {
                                    // don't include this in the results
                                    continue sortKeyIteration
                                }
                            case .beginsWith(let value):
                                if !(sortKey.hasPrefix(value)) {
                                    // don't include this in the results
                                    continue sortKeyIteration
                                }
                            }
                        }

                        do {
                            items.append(try self.convertToQueryableType(input: value))
                        } catch {
                            continuation.resume(throwing: error)
                            return
                        }
                    }
                }

                continuation.resume(returning: items)
            }
        }
    }
    
    internal func convertToQueryableType<ReturnedType: PolymorphicOperationReturnType>(input: PolymorphicOperationReturnTypeConvertable) throws -> ReturnedType {
        let storedRowTypeName = input.rowTypeIdentifier
        
        var queryableTypeProviders: [String: PolymorphicOperationReturnOption<ReturnedType.AttributesType, ReturnedType>] = [:]
        ReturnedType.types.forEach { (type, provider) in
            queryableTypeProviders[getTypeRowIdentifier(type: type)] = provider
        }

        if let provider = queryableTypeProviders[storedRowTypeName] {
            return try provider.getReturnType(input: input)
        } else {
            // throw an exception, we don't know what this type is
            throw SmokeDynamoDBError.unexpectedType(provided: storedRowTypeName)
        }
    }
    
    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             exclusiveStartKey: String?,
                                                             consistentRead: Bool) async throws
    -> ([ReturnedType], String?) {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               limit: limit,
                               scanIndexForward: true,
                               exclusiveStartKey: exclusiveStartKey,
                               consistentRead: consistentRead)
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             scanIndexForward: Bool,
                                                             exclusiveStartKey: String?,
                                                             consistentRead: Bool) async throws
    -> ([ReturnedType], String?) {
        // get all the results
        let rawItems: [ReturnedType] = try await query(forPartitionKey: partitionKey,
                                                       sortKeyCondition: sortKeyCondition,
                                                       consistentRead: consistentRead)
        
        let items: [ReturnedType]
        if !scanIndexForward {
            items = rawItems.reversed()
        } else {
            items = rawItems
        }

        let startIndex: Int
        // if there is an exclusiveStartKey
        if let exclusiveStartKey = exclusiveStartKey {
            guard let storedStartIndex = Int(exclusiveStartKey) else {
                fatalError("Unexpectedly encoded exclusiveStartKey '\(exclusiveStartKey)'")
            }

            startIndex = storedStartIndex
        } else {
            startIndex = 0
        }

        let endIndex: Int
        let lastEvaluatedKey: String?
        if let limit = limit, startIndex + limit < items.count {
            endIndex = startIndex + limit
            lastEvaluatedKey = String(endIndex)
        } else {
            endIndex = items.count
            lastEvaluatedKey = nil
        }

        return (Array(items[startIndex..<endIndex]), lastEvaluatedKey)
    }
}
