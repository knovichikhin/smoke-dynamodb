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
//  AWSDynamoDBCompositePrimaryKeyTable.swift
//  SmokeDynamoDB
//

import AWSDynamoDB
import AWSClientRuntime
import Logging

public class AWSDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {
    internal let dynamodb: DynamoDbClient
    internal let targetTableName: String
    internal let retryConfiguration: RetryConfiguration
    internal let logger: Logger

    public init(config: AWSClientRuntime.AWSClientConfiguration,
                tableName: String,
                retryConfiguration: RetryConfiguration = .default,
                logger: Logger) {
        self.dynamodb = DynamoDbClient(config: config)
        self.targetTableName = tableName
        self.retryConfiguration = retryConfiguration
        self.logger = logger

        self.logger.info("AWSDynamoDBTable created with table name '\(tableName)'")
    }
    
    public init(region: String,
                tableName: String,
                retryConfiguration: RetryConfiguration = .default,
                logger: Logger) throws {
        self.dynamodb = try DynamoDbClient(region: region)
        self.targetTableName = tableName
        self.retryConfiguration = retryConfiguration
        self.logger = logger

        self.logger.info("AWSDynamoDBTable created with table name '\(tableName)'")
    }
    
    public init(tableName: String,
                retryConfiguration: RetryConfiguration = .default,
                logger: Logger) async throws {
        self.dynamodb = try await DynamoDbClient()
        self.targetTableName = tableName
        self.retryConfiguration = retryConfiguration
        self.logger = logger

        self.logger.info("AWSDynamoDBTable created with table name '\(tableName)'")
    }

    internal func getInputForInsert<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
            -> AWSDynamoDB.PutItemInput {
                let attributes = try getAttributes(forItem: item)

                let expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName, "#sk": AttributesType.sortKeyAttributeName]
                let conditionExpression = "attribute_not_exists (#pk) AND attribute_not_exists (#sk)"

                return AWSDynamoDB.PutItemInput(conditionExpression: conditionExpression,
                                                  expressionAttributeNames: expressionAttributeNames,
                                                  item: attributes,
                                                  tableName: targetTableName)
        }

        internal func getInputForUpdateItem<AttributesType, ItemType>(
                newItem: TypedDatabaseItem<AttributesType, ItemType>,
                existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> AWSDynamoDB.PutItemInput {
            let attributes = try getAttributes(forItem: newItem)

            let expressionAttributeNames = [
                "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
                "#createdate": TypedDatabaseItem<AttributesType, ItemType>.CodingKeys.createDate.stringValue]
            let expressionAttributeValues = [
                ":versionnumber": AWSDynamoDB.DynamoDbClientTypes.AttributeValue.n(String(existingItem.rowStatus.rowVersion)),
                ":creationdate": AWSDynamoDB.DynamoDbClientTypes.AttributeValue.s(existingItem.createDate.iso8601)]

            let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

            return AWSDynamoDB.PutItemInput(conditionExpression: conditionExpression,
                                              expressionAttributeNames: expressionAttributeNames,
                                              expressionAttributeValues: expressionAttributeValues,
                                              item: attributes,
                                              tableName: targetTableName)
        }

        internal func getInputForGetItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> AWSDynamoDB.GetItemInput {
            let attributeValue = try DynamoDBEncoder().encode(key)

            if case let .m(keyAttributes) = attributeValue {
                return AWSDynamoDB.GetItemInput(consistentRead: true,
                                                  key: keyAttributes,
                                                  tableName: targetTableName)
            } else {
                throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
            }
        }
        
        internal func getInputForBatchGetItem<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) throws
        -> AWSDynamoDB.BatchGetItemInput {
            let keys = try keys.map { key -> [String: AWSDynamoDB.DynamoDbClientTypes.AttributeValue] in
                let attributeValue = try DynamoDBEncoder().encode(key)
                
                if case .m(let keyAttributes) = attributeValue {
                   return keyAttributes
                } else {
                    throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
                }
            }

            let keysAndAttributes = AWSDynamoDB.DynamoDbClientTypes.KeysAndAttributes(consistentRead: true,
                                                      keys: keys)
            
            return AWSDynamoDB.BatchGetItemInput(requestItems: [self.targetTableName: keysAndAttributes])
        }

        internal func getInputForDeleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> AWSDynamoDB.DeleteItemInput {
            let attributeValue = try DynamoDBEncoder().encode(key)

            if case .m(let keyAttributes) = attributeValue {
                return AWSDynamoDB.DeleteItemInput(key: keyAttributes,
                                                     tableName: targetTableName)
            } else {
                throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
            }
        }
        
        internal func getInputForDeleteItem<AttributesType, ItemType>(
                existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> AWSDynamoDB.DeleteItemInput {
            let attributeValue = try DynamoDBEncoder().encode(existingItem.compositePrimaryKey)
            
            guard case .m(let keyAttributes) = attributeValue else {
                throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
            }

            let expressionAttributeNames = [
                "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
                "#createdate": TypedDatabaseItem<AttributesType, ItemType>.CodingKeys.createDate.stringValue]
            let expressionAttributeValues = [
                ":versionnumber": AWSDynamoDB.DynamoDbClientTypes.AttributeValue.n(String(existingItem.rowStatus.rowVersion)),
                ":creationdate": AWSDynamoDB.DynamoDbClientTypes.AttributeValue.s(existingItem.createDate.iso8601)]

            let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

            return AWSDynamoDB.DeleteItemInput(conditionExpression: conditionExpression,
                                                 expressionAttributeNames: expressionAttributeNames,
                                                 expressionAttributeValues: expressionAttributeValues,
                                                 key: keyAttributes,
                                                 tableName: targetTableName)
        }
}

extension Array {
    func chunked(by chunkSize: Int) -> [[Element]] {
        return stride(from: 0, to: self.count, by: chunkSize).map {
            Array(self[$0..<Swift.min($0 + chunkSize, self.count)])
        }
    }
}
