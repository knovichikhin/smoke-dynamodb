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
//  AWSDynamoDBCompositePrimaryKeysProjection.swift
//  SmokeDynamoDB
//

import AWSDynamoDB
import AWSClientRuntime
import Logging

public class AWSDynamoDBCompositePrimaryKeysProjection: DynamoDBCompositePrimaryKeysProjection {
    internal let dynamodb: DynamoDbClient
    internal let targetTableName: String
    internal let logger: Logger

    internal class QueryPaginationResults<AttributesType: PrimaryKeyAttributes> {
        var items: [CompositePrimaryKey<AttributesType>] = []
        var exclusiveStartKey: String?
    }
    
    public init(config: AWSClientRuntime.AWSClientConfiguration,
                tableName: String,
                logger: Logger) {
        self.dynamodb = DynamoDbClient(config: config)
        self.targetTableName = tableName
        self.logger = logger

        self.logger.info("AWSDynamoDBCompositePrimaryKeysProjection created with table name '\(tableName)'")
    }
    
    public init(region: String,
                tableName: String,
                logger: Logger) throws {
        self.dynamodb = try DynamoDbClient(region: region)
        self.targetTableName = tableName
        self.logger = logger

        self.logger.info("AWSDynamoDBCompositePrimaryKeysProjection created with table name '\(tableName)'")
    }
    
    public init(tableName: String,
                logger: Logger) async throws {
        self.dynamodb = try await DynamoDbClient()
        self.targetTableName = tableName
        self.logger = logger

        self.logger.info("AWSDynamoDBCompositePrimaryKeysProjection created with table name '\(tableName)'")
    }
    
    internal init(dynamodb: DynamoDbClient,
                  tableName: String,
                  logger: Logger) {
        self.dynamodb = dynamodb
        self.targetTableName = tableName
        self.logger = logger

        self.logger.info("AWSDynamoDBCompositePrimaryKeysProjection created with table name '\(tableName)'")
    }
}
