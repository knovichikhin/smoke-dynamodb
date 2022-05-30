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
//  AWSDynamoDBCompositePrimaryKeysProjectionGenerator.swift
//  SmokeDynamoDB
//

import AWSDynamoDB
import AWSClientRuntime
import Logging

public class AWSDynamoDBCompositePrimaryKeysProjectionGenerator {
    private let dynamodb: DynamoDbClient
    internal let targetTableName: String
    
    public init(config: AWSClientRuntime.AWSClientConfiguration,
                tableName: String) {
        self.dynamodb = DynamoDbClient(config: config)
        self.targetTableName = tableName
    }
    
    public init(region: String,
                tableName: String) throws {
        self.dynamodb = try DynamoDbClient(region: region)
        self.targetTableName = tableName
    }
    
    public init(tableName: String) async throws {
        self.dynamodb = try await DynamoDbClient()
        self.targetTableName = tableName
    }

    public func with(logger: Logging.Logger)
    -> AWSDynamoDBCompositePrimaryKeysProjection {
        return AWSDynamoDBCompositePrimaryKeysProjection(dynamodb: dynamodb, tableName: self.targetTableName, logger: logger)
    }
}
